package litestream_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestCompactor_BatchedIO(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages uint32
	}{{"partial_final_buffer", 1}, {"multiple_buffers", 256}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			base := file.NewReplicaClient(t.TempDir())
			var sources [][]byte
			for txid := ltx.TXID(1); txid <= 2; txid++ {
				data := compactionBufferFixture(t, txid, tc.pages)
				sources = append(sources, data)
				if _, err := base.WriteLTXFile(ctx, 0, txid, txid, bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			// The original, unbuffered encoder defines the exact output bytes.
			var expected bytes.Buffer
			original, err := ltx.NewCompactor(&expected, []io.Reader{bytes.NewReader(sources[0]), bytes.NewReader(sources[1])})
			if err != nil {
				t.Fatal(err)
			}
			original.HeaderFlags = ltx.HeaderFlagNoChecksum
			if err := original.Compact(ctx); err != nil {
				t.Fatal(err)
			}

			client := &compactionIOClient{ReplicaClient: base}
			compactor := litestream.NewCompactor(client, slog.Default())
			var sourceReads atomic.Int64
			var sourceCloses atomic.Int64
			compactor.LocalFileOpener = func(level int, min, max ltx.TXID) (io.ReadCloser, error) {
				r, err := base.OpenLTXFile(ctx, level, min, max, 0, 0)
				if err != nil {
					return nil, err
				}
				return &compactionCountedSource{ReadCloser: r, reads: &sourceReads, closes: &sourceCloses}, nil
			}
			info, err := compactor.Compact(ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(base.LTXFilePath(info.Level, info.MinTXID, info.MaxTXID))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected.Bytes()) {
				t.Fatal("compaction changed encoded bytes")
			}
			if sourceCloses.Load() != 2 {
				t.Fatalf("closed %d source files, want 2", sourceCloses.Load())
			}
			// Require batched I/O without coupling the assertion to an exact
			// buffer size or a machine-dependent wall-clock deadline.
			readBudget := int64((len(sources[0])+len(sources[1]))/8192 + 16)
			writeBudget := int64(len(actual)/8192 + 16)
			if sourceReads.Load() > readBudget {
				t.Errorf("source reads=%d, budget=%d", sourceReads.Load(), readBudget)
			}
			if client.reads.Load() > writeBudget {
				t.Errorf("destination reads=%d, budget=%d", client.reads.Load(), writeBudget)
			}
			t.Logf("source reads=%d, destination reads=%d, output bytes=%d", sourceReads.Load(), client.reads.Load(), len(actual))
		})
	}
}

func TestCompactor_BufferedSourceError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages uint32
	}{{"before_flush", 1}, {"after_flush", 256}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := file.NewReplicaClient(t.TempDir())
			data := compactionBufferFixture(t, 1, tc.pages)
			data[len(data)-1] ^= 1 // Corrupt only the final file checksum.
			if _, err := client.WriteLTXFile(ctx, 0, 1, 1, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			if _, err := litestream.NewCompactor(client, slog.Default()).Compact(ctx, 1); !errors.Is(err, ltx.ErrChecksumMismatch) {
				t.Fatalf("error=%v, want checksum mismatch", err)
			}
			if _, err := os.Stat(client.LTXFilePath(1, 1, 1)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed compaction published output: %v", err)
			}
		})
	}
}

func compactionBufferFixture(t *testing.T, txid ltx.TXID, pages uint32) []byte {
	t.Helper()
	var output bytes.Buffer
	enc, err := ltx.NewEncoder(&output)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 4096, Commit: pages, MinTXID: txid, MaxTXID: txid, Timestamp: 1}); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(uint64(txid), 1))
	page := make([]byte, 4096)
	for pgno := uint32(1); pgno <= pages; pgno++ {
		for i := 0; i < len(page); i += 8 {
			binary.LittleEndian.PutUint64(page[i:], random.Uint64())
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

type compactionCountedSource struct {
	io.ReadCloser
	reads, closes *atomic.Int64
}

func (r *compactionCountedSource) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.ReadCloser.Read(p)
}
func (r *compactionCountedSource) Close() error { r.closes.Add(1); return r.ReadCloser.Close() }

type compactionIOClient struct {
	litestream.ReplicaClient
	reads atomic.Int64
}

func (c *compactionIOClient) WriteLTXFile(ctx context.Context, level int, min, max ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	return c.ReplicaClient.WriteLTXFile(ctx, level, min, max, &compactionCountedReader{reader: r, reads: &c.reads})
}

type compactionCountedReader struct {
	reader io.Reader
	reads  *atomic.Int64
}

func (r *compactionCountedReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.reader.Read(p)
}
