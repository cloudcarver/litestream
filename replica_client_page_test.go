package litestream_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/mock"
)

func TestFetchPageRetainsOnlyPageBytes(t *testing.T) {
	for _, pageSize := range []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
		for _, legacy := range []bool{false, true} {
			t.Run(fmt.Sprintf("size=%d/legacy=%t", pageSize, legacy), func(t *testing.T) {
				want := bytes.Repeat([]byte{0x42}, pageSize)
				var encoded bytes.Buffer
				var frame []byte
				if legacy {
					hdr, err := (&ltx.PageHeader{Pgno: 2}).MarshalBinary()
					if err != nil {
						t.Fatal(err)
					}
					encoded.Write(hdr)
					zw := lz4.NewWriter(&encoded)
					if _, err := zw.Write(want); err != nil {
						t.Fatal(err)
					}
					if err := zw.Close(); err != nil {
						t.Fatal(err)
					}
					frame = encoded.Bytes()
				} else {
					enc, err := ltx.NewEncoder(&encoded)
					if err != nil {
						t.Fatal(err)
					}
					if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: uint32(pageSize), Commit: 2, MinTXID: 2, MaxTXID: 2}); err != nil {
						t.Fatal(err)
					}
					start := enc.N()
					if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, want); err != nil {
						t.Fatal(err)
					}
					end := enc.N()
					if err := enc.Close(); err != nil {
						t.Fatal(err)
					}
					frame = encoded.Bytes()[start:end]
				}
				client := &mock.ReplicaClient{OpenLTXFileFunc: func(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(frame)), nil
				}}
				hdr, got, err := litestream.FetchPage(context.Background(), client, 0, 2, 2, 0, int64(len(frame)))
				if err != nil || hdr.Pgno != 2 || !bytes.Equal(got, want) {
					t.Fatalf("page differs: header=%+v len=%d err=%v", hdr, len(got), err)
				}
				if cap(got) != pageSize {
					t.Fatalf("page retains %d bytes, cache accounts for %d", cap(got), pageSize)
				}
			})
		}
	}
}

func TestFetchPageDecodeErrorHasNoData(t *testing.T) {
	client := &mock.ReplicaClient{OpenLTXFileFunc: func(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("bad"))), nil
	}}
	_, got, err := litestream.FetchPage(context.Background(), client, 0, 1, 1, 0, 3)
	if err == nil || got != nil {
		t.Fatalf("decode failure returned data: len=%d err=%v", len(got), err)
	}
}
