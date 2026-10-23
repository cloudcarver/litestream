package litestream_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/mock"
)

// Measure the complete merge/encode path with four immutable in-memory inputs.
// Network and storage latency are deliberately excluded; each output is checked
// against a merge that reads the raw input bytes without the resumable wrapper.
func BenchmarkCompactorHealthy(b *testing.B) {
	const pages = 256
	var sources [][]byte
	var infos []*ltx.FileInfo
	var inputBytes int64
	random := rand.New(rand.NewPCG(1, 2))
	for txid := ltx.TXID(1); txid <= 4; txid++ {
		var data bytes.Buffer
		enc, err := ltx.NewEncoder(&data)
		if err != nil {
			b.Fatal(err)
		}
		if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum,
			PageSize: 4096, Commit: pages, MinTXID: txid, MaxTXID: txid, Timestamp: 123}); err != nil {
			b.Fatal(err)
		}
		for pg := uint32(1); pg <= pages; pg++ {
			page := make([]byte, 4096)
			for i := 0; i < len(page); i += 8 {
				binary.LittleEndian.PutUint64(page[i:], random.Uint64())
			}
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pg}, page); err != nil {
				b.Fatal(err)
			}
		}
		if err := enc.Close(); err != nil {
			b.Fatal(err)
		}
		sources = append(sources, data.Bytes())
		infos = append(infos, &ltx.FileInfo{Level: 0, MinTXID: txid, MaxTXID: txid, Size: int64(data.Len())})
		inputBytes += int64(data.Len())
	}
	var expected bytes.Buffer
	var raw []io.Reader
	for _, data := range sources {
		raw = append(raw, bytes.NewReader(data))
	}
	merger, err := ltx.NewCompactor(&expected, raw)
	if err != nil {
		b.Fatal(err)
	}
	merger.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := merger.Compact(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := ltx.NewDecoder(bytes.NewReader(expected.Bytes())).Verify(); err != nil {
		b.Fatal(err)
	}
	client := &mock.ReplicaClient{
		LTXFilesFunc: func(_ context.Context, level int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
			if level == 0 {
				return ltx.NewFileInfoSliceIterator(infos), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
		OpenLTXFileFunc: func(_ context.Context, _ int, min, _ ltx.TXID, offset, _ int64) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(sources[int(min)-1][offset:])), nil
		},
		WriteLTXFileFunc: func(_ context.Context, level int, min, max ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
			data, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(data, expected.Bytes()) {
				return nil, fmt.Errorf("compaction output differs from raw-input oracle")
			}
			return &ltx.FileInfo{Level: level, MinTXID: min, MaxTXID: max, Size: int64(len(data))}, nil
		},
	}
	compactor := litestream.NewCompactor(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.SetBytes(inputBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := compactor.Compact(context.Background(), 1); err != nil {
			b.Fatal(err)
		}
	}
}
