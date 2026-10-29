package litestream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

type countedLTXReader struct {
	*bytes.Reader
	n         int
	afterRead func()
}

func (r *countedLTXReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	r.n += n
	return n, err
}

func (r *countedLTXReader) ReadAt(b []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(b, off)
	r.n += n
	if r.afterRead != nil {
		r.afterRead()
	}
	return n, err
}

func lastPageFixture(t *testing.T, pages uint32) (*DB, []byte, []byte, int64) {
	t.Helper()
	const pageSize = 4096
	var out bytes.Buffer
	enc, err := ltx.NewEncoder(&out)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum,
		PageSize: pageSize, Commit: pages, MinTXID: 2, MaxTXID: 2, WALOffset: WALHeaderSize, WALSalt1: 123, WALSalt2: 456}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(42, 101))
	data := make([]byte, pageSize)
	for pgno := uint32(1); pgno <= pages; pgno++ {
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	frameSize := int64(pageSize + WALFrameHeaderSize)
	wal := make([]byte, WALHeaderSize+2*frameSize)
	frame := wal[WALHeaderSize+frameSize:]
	binary.BigEndian.PutUint32(frame, pages)
	binary.BigEndian.PutUint32(frame[8:], 123)
	binary.BigEndian.PutUint32(frame[12:], 456)
	copy(frame[WALFrameHeaderSize:], data)
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	return db, out.Bytes(), wal, frameSize
}

func matchLastPage(t *testing.T, ctx context.Context, db *DB, encoded, wal []byte, frameSize int64, afterRead func()) (bool, int, error) {
	t.Helper()
	if err := os.WriteFile(db.WALPath(), wal, 0600); err != nil {
		t.Fatal(err)
	}
	r := &countedLTXReader{Reader: bytes.NewReader(encoded), afterRead: afterRead}
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	match, err := db.lastPageMatch(ctx, dec, r, int64(len(encoded)), WALHeaderSize+frameSize, frameSize)
	return match, r.n, err
}

func TestDB_LastPageMatchBoundedIO(t *testing.T) {
	db, encoded, wal, frameSize := lastPageFixture(t, 2048)
	match, n, err := matchLastPage(t, t.Context(), db, encoded, wal, frameSize, nil)
	if err != nil || !match {
		t.Fatalf("match=%v error=%v", match, err)
	}
	// 8 MiB of incompressible page bodies, but only the compact index and
	// selected page are needed. Assert bytes, not host-dependent elapsed time.
	if n > 64<<10 {
		t.Fatalf("last-page verification read unrelated LTX page bodies: read=%d file=%d", n, len(encoded))
	}
	t.Logf("verified final page using %d bytes from %d-byte LTX", n, len(encoded))
}

func TestDB_LastPageMatchFences(t *testing.T) {
	for _, name := range []string{"match", "different-data", "different-salt", "missing-page", "truncated-wal", "corrupt-index-size", "corrupt-index-range", "wrong-index-page", "canceled", "cancel-during-index"} {
		t.Run(name, func(t *testing.T) {
			db, encoded, wal, frameSize := lastPageFixture(t, 8)
			frame := wal[WALHeaderSize+frameSize:]
			indexEnd := len(encoded) - ltx.TrailerSize - 8
			indexStart := indexEnd - int(binary.BigEndian.Uint64(encoded[indexEnd:]))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var afterRead func()
			wantErr := false
			switch name {
			case "different-data":
				frame[WALFrameHeaderSize] ^= 1
			case "different-salt":
				frame[8] ^= 1
			case "missing-page":
				binary.BigEndian.PutUint32(frame, 9)
			case "truncated-wal":
				wal = wal[:len(wal)-1]
				wantErr = true
			case "corrupt-index-size":
				binary.BigEndian.PutUint64(encoded[indexEnd:], ^uint64(0))
				wantErr = true
			case "corrupt-index-range":
				encoded[indexStart+1] = 1
				wantErr = true // first offset points into header
			case "wrong-index-page":
				// Retain a valid in-bounds index while changing the selected page header.
				elem, found, err := findLTXPage(t.Context(), bytes.NewReader(encoded), int64(len(encoded)), 8)
				if err != nil || !found {
					t.Fatalf("fixture index: %v %v", found, err)
				}
				binary.BigEndian.PutUint32(encoded[elem.Offset:], 7)
				wantErr = true
			case "canceled":
				cancel()
				wantErr = true
			case "cancel-during-index":
				afterRead = cancel
				wantErr = true
			}
			match, _, err := matchLastPage(t, ctx, db, encoded, wal, frameSize, afterRead)
			if (err != nil) != wantErr || match != (name == "match") {
				t.Fatalf("match=%v error=%v", match, err)
			}
			if (name == "canceled" || name == "cancel-during-index") && !errors.Is(err, context.Canceled) {
				t.Fatalf("expected cancellation, got %v", err)
			}
		})
	}
}

func TestDB_LastPageMatchLegacyFrame(t *testing.T) {
	db, original, wal, frameSize := lastPageFixture(t, 2)
	frame, data := encodeTestPageFrame(t, 4096, true, true)
	encoded := bytes.Clone(original[:ltx.HeaderSize])
	encoded = append(encoded, frame...)
	encoded = append(encoded, make([]byte, ltx.PageHeaderSize)...)
	start := len(encoded)
	for _, n := range []uint64{2, ltx.HeaderSize, uint64(len(frame)), 0} {
		encoded = binary.AppendUvarint(encoded, n)
	}
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(encoded)-start))
	encoded = append(encoded, original[len(original)-ltx.TrailerSize:]...)
	copy(wal[WALHeaderSize+frameSize+WALFrameHeaderSize:], data)
	match, _, err := matchLastPage(t, t.Context(), db, encoded, wal, frameSize, nil)
	if err != nil || !match {
		t.Fatalf("legacy frame: match=%v error=%v", match, err)
	}
}

func TestFindLTXPageMalformedIndex(t *testing.T) {
	for _, index := range [][]byte{nil, {0xff}, {1, 100}, {1, 100, 10}, {1, 100, 10, 1, 110, 10, 0}, {0, 1}} {
		encoded := make([]byte, ltx.HeaderSize+32+ltx.PageHeaderSize)
		encoded = append(encoded, index...)
		encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(index)))
		encoded = append(encoded, make([]byte, ltx.TrailerSize)...)
		if _, _, err := findLTXPage(t.Context(), bytes.NewReader(encoded), int64(len(encoded)), 1); err == nil {
			t.Fatalf("accepted malformed index %x", index)
		}
	}
	if _, _, err := findLTXPage(t.Context(), bytes.NewReader(nil), 1000, 1); !errors.Is(err, io.EOF) {
		t.Fatalf("lost reader error: %v", err)
	}
}
