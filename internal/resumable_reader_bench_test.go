package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/superfly/ltx"
)

// Isolate the healthy-stream CPU/allocation cost, without network latency,
// compression or SQLite. The lazy-open API exists on both sides of the patch.
func BenchmarkResumableReaderHealthy(b *testing.B) {
	for _, chunk := range []int{100, 4096} {
		b.Run(fmt.Sprint(chunk), func(b *testing.B) {
			data := make([]byte, 4<<20)
			buf := make([]byte, chunk)
			opener := &testLTXFileOpener{OpenLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, offset, _ int64) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			}}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				r := NewResumableReader(context.Background(), opener, 0, 1, 1, int64(len(data)), nil, logger)
				total := 0
				for {
					n, err := r.Read(buf)
					total += n
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
				if total != len(data) {
					b.Fatalf("read %d bytes, want %d", total, len(data))
				}
			}
		})
	}
}
