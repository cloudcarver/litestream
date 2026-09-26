package litestream

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/superfly/ltx"
)

func encodeTestPageFrame(tb testing.TB, size int, random, legacy bool) (frame, page []byte) {
	tb.Helper()
	page = bytes.Repeat([]byte{0x42}, size)
	if random {
		rng := rand.New(rand.NewPCG(42, 101))
		for i := range page {
			page[i] = byte(rng.Uint32())
		}
	}
	var encoded bytes.Buffer
	if legacy {
		hdr, err := (&ltx.PageHeader{Pgno: 2}).MarshalBinary()
		if err != nil {
			tb.Fatal(err)
		}
		encoded.Write(hdr)
		zw := lz4.NewWriter(&encoded)
		if _, err := zw.Write(page); err != nil {
			tb.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			tb.Fatal(err)
		}
		return encoded.Bytes(), page
	}
	enc, err := ltx.NewEncoder(&encoded)
	if err != nil {
		tb.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum,
		PageSize: uint32(size), Commit: 2, MinTXID: 2, MaxTXID: 2}); err != nil {
		tb.Fatal(err)
	}
	start := enc.N()
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page); err != nil {
		tb.Fatal(err)
	}
	end := enc.N()
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}
	return encoded.Bytes()[start:end], page
}

func existingDecodeOwned(b []byte) (ltx.PageHeader, []byte, error) {
	hdr, data, err := ltx.DecodePageData(b)
	if err != nil {
		return hdr, nil, err
	}
	if cap(data) > len(data) {
		owned := make([]byte, len(data))
		copy(owned, data)
		data = owned
	}
	return hdr, data, nil
}

func TestDecodePageDataMatchesCurrent(t *testing.T) {
	for _, size := range []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
		for _, legacy := range []bool{false, true} {
			for _, random := range []bool{false, true} {
				t.Run(fmt.Sprintf("size=%d/legacy=%t/random=%t", size, legacy, random), func(t *testing.T) {
					frame, want := encodeTestPageFrame(t, size, random, legacy)
					hdr, data, err := decodePageData(frame)
					oldHdr, oldData, oldErr := existingDecodeOwned(frame)
					if err != nil || oldErr != nil || hdr != oldHdr || !bytes.Equal(data, want) || !bytes.Equal(data, oldData) || cap(data) != size {
						t.Fatalf("page differs: hdr=%+v cap=%d err=%v oldErr=%v", hdr, cap(data), err, oldErr)
					}
					// Retain the result while subsequent calls reuse scratch.
					other, _ := encodeTestPageFrame(t, 65536, !random, false)
					for range 20 {
						if _, _, err := decodePageData(other); err != nil {
							t.Fatal(err)
						}
					}
					if !bytes.Equal(data, want) {
						t.Fatal("scratch reuse changed an earlier result")
					}
				})
			}
		}
	}
}

func assertPageDecodeMatches(tb testing.TB, frame []byte) {
	tb.Helper()
	hdr, data, err := decodePageData(frame)
	wantHdr, want, wantErr := existingDecodeOwned(frame)
	if hdr != wantHdr || fmt.Sprint(err) != fmt.Sprint(wantErr) || !bytes.Equal(data, want) || (data == nil) != (want == nil) {
		tb.Fatalf("decode differs: header=%+v want=%+v bytes=%d want=%d err=%v want=%v", hdr, wantHdr, len(data), len(want), err, wantErr)
	}
	if err != nil && data != nil {
		tb.Fatal("decode error exposes partial data")
	}
	if cap(data) != len(data) {
		tb.Fatalf("decoded result retains %d bytes for %d bytes of data", cap(data), len(data))
	}
}

func TestDecodePageDataMalformed(t *testing.T) {
	assertPageDecodeMatches(t, nil)
	assertPageDecodeMatches(t, make([]byte, ltx.PageHeaderSize))
	for _, legacy := range []bool{false, true} {
		frame, _ := encodeTestPageFrame(t, 4096, true, legacy)
		// Exhaustive truncation, including the page header, compressed size,
		// legacy frame checksum, and final literal/match sequence.
		for n := range len(frame) {
			assertPageDecodeMatches(t, frame[:n])
		}
		assertPageDecodeMatches(t, append(bytes.Clone(frame), 0xaa, 0xbb))
		if !legacy {
			for _, size := range []uint32{0, 1, 8, 65536, 65537, 0x7fffffff, 0xffffffff} {
				bad := bytes.Clone(frame)
				binary.BigEndian.PutUint32(bad[ltx.PageHeaderSize:], size)
				assertPageDecodeMatches(t, bad)
			}
			// UnmarshalBinary does not validate unknown flags; do not silently
			// change that contract as part of a buffer allocation optimization.
			bad := bytes.Clone(frame)
			binary.BigEndian.PutUint16(bad[4:], 0xffff)
			assertPageDecodeMatches(t, bad)
		}
	}
	for _, size := range []int{65537, 131072} {
		page := bytes.Repeat([]byte{0x9d}, size)
		compressed := make([]byte, lz4.CompressBlockBound(size))
		n, err := lz4.CompressBlock(page, compressed, nil)
		if err != nil || n == 0 {
			t.Fatalf("encode oversized block: size=%d n=%d err=%v", size, n, err)
		}
		hdr, _ := (&ltx.PageHeader{Pgno: 2, Flags: ltx.PageHeaderFlagSize}).MarshalBinary()
		frame := binary.BigEndian.AppendUint32(hdr, uint32(n))
		frame = append(frame, compressed[:n]...)
		assertPageDecodeMatches(t, frame)
		if _, data, err := decodePageData(frame); err == nil || data != nil {
			t.Fatal("oversized output did not fail without data")
		}
	}
}

func TestDecodePageDataConcurrentOwnership(t *testing.T) {
	type sample struct{ frame, page []byte }
	var samples []sample
	for _, size := range []int{512, 4096, 65536} {
		for _, random := range []bool{false, true} {
			frame, page := encodeTestPageFrame(t, size, random, false)
			samples = append(samples, sample{frame, page})
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := range 32 {
		wg.Go(func() {
			<-start
			test := samples[worker%len(samples)]
			_, retained, err := decodePageData(test.frame)
			if err != nil {
				t.Error(err)
				return
			}
			for i := range 100 {
				other := samples[(worker+i)%len(samples)]
				_, got, err := decodePageData(other.frame)
				if err != nil || !bytes.Equal(got, other.page) || !bytes.Equal(retained, test.page) {
					t.Errorf("concurrent decode changed data: worker=%d iteration=%d err=%v", worker, i, err)
					return
				}
				// Mutating owned results must not poison a later decode.
				clear(got)
			}
			if !bytes.Equal(retained, test.page) {
				t.Error("concurrent scratch reuse changed retained data")
			}
		})
	}
	close(start)
	wg.Wait()
	if len(pageDecodeScratch) > 8 || cap(pageDecodeScratch) != 8 {
		t.Fatal("scratch retention exceeded its process-wide bound")
	}
}

func FuzzDecodePageData(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, ltx.PageHeaderSize))
	for _, size := range []int{512, 4096, 65536} {
		for _, random := range []bool{false, true} {
			frame, _ := encodeTestPageFrame(f, size, random, false)
			f.Add(frame)
		}
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		// Fuzz the changed block path. Arbitrary legacy streams are unbounded
		// io.ReadAll upstream; their finite truncation cases are tested above.
		if len(frame) > 66000 {
			return
		}
		if len(frame) >= ltx.PageHeaderSize {
			var hdr ltx.PageHeader
			_ = hdr.UnmarshalBinary(frame)
			if !hdr.IsZero() && hdr.Flags&ltx.PageHeaderFlagSize == 0 {
				return
			}
		}
		// Pre-poison every idle buffer to detect decoding malformed data that
		// mistakenly exposes bytes left by another database's earlier page.
		for range len(pageDecodeScratch) {
			scratch := <-pageDecodeScratch
			for i := range scratch {
				scratch[i] = 0xa5
			}
			pageDecodeScratch <- scratch
		}
		assertPageDecodeMatches(t, frame)
	})
}

func BenchmarkPageDecodeScratch(b *testing.B) {
	for _, size := range []int{512, 4096, 65536} {
		for _, random := range []bool{false, true} {
			frame, _ := encodeTestPageFrame(b, size, random, false)
			for _, candidate := range []bool{false, true} {
				b.Run(fmt.Sprintf("size=%d/random=%t/pooled=%t", size, random, candidate), func(b *testing.B) {
					decode := existingDecodeOwned
					if candidate {
						decode = decodePageData
					}
					b.ReportAllocs()
					b.SetBytes(int64(size))
					for b.Loop() {
						_, data, err := decode(frame)
						if err != nil || len(data) != size {
							b.Fatal(err, len(data))
						}
					}
				})
			}
		}
	}
}

func BenchmarkPageDecodeScratchParallel(b *testing.B) {
	for _, size := range []int{4096, 65536} {
		frame, _ := encodeTestPageFrame(b, size, false, false)
		for _, candidate := range []bool{false, true} {
			b.Run(fmt.Sprintf("size=%d/pooled=%t", size, candidate), func(b *testing.B) {
				decode := existingDecodeOwned
				if candidate {
					decode = decodePageData
				}
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, data, err := decode(frame)
						if err != nil || len(data) != size {
							b.Error(err, len(data))
							return
						}
					}
				})
			})
		}
	}
}

type pageFrameClient struct {
	ReplicaClient
	frame []byte
}

func (c *pageFrameClient) OpenLTXFile(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(c.frame)), nil
}

func BenchmarkFetchPageAllocation(b *testing.B) {
	frame, _ := encodeTestPageFrame(b, 4096, false, false)
	client := &pageFrameClient{frame: frame}
	b.ReportAllocs()
	for b.Loop() {
		_, data, err := FetchPage(context.Background(), client, 0, 2, 2, 0, int64(len(frame)))
		if err != nil || len(data) != 4096 {
			b.Fatal(err, len(data))
		}
	}
}
