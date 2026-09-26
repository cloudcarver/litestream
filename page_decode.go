package litestream

import (
	"encoding/binary"
	"fmt"

	"github.com/pierrec/lz4/v4"
	"github.com/superfly/ltx"
)

// Bound idle scratch to 512 KiB per process, not per VFS/database. Borrowing
// never waits: excess concurrent decoders allocate and discard their buffers.
var pageDecodeScratch = make(chan *[65536]byte, 8)

// decodePageData keeps ltx v0.5.2's single-frame decoding semantics but reuses
// its 64 KiB block scratch. Returned data always owns exactly its page bytes;
// it never aliases a buffer that can be reused by another database. Keep this
// shim local to FetchPage until LTX supports caller-owned decode buffers.
func decodePageData(b []byte) (hdr ltx.PageHeader, data []byte, err error) {
	if err := hdr.UnmarshalBinary(b); err != nil {
		return hdr, nil, fmt.Errorf("unmarshal: %w", err)
	}
	if hdr.IsZero() {
		return hdr, nil, nil
	}
	if hdr.Flags&ltx.PageHeaderFlagSize == 0 {
		hdr, data, err = ltx.DecodePageData(b)
		if err != nil {
			return hdr, nil, err
		}
		// Legacy frame buffers can also exceed the page's accounted size.
		if cap(data) > len(data) {
			owned := make([]byte, len(data))
			copy(owned, data)
			data = owned
		}
		return hdr, data, nil
	}
	const offset = ltx.PageHeaderSize + 4
	if len(b) < offset {
		return hdr, nil, fmt.Errorf("buffer too small for size prefix")
	}
	size := binary.BigEndian.Uint32(b[ltx.PageHeaderSize:])
	// Compare before converting to int, including on 32-bit platforms.
	if uint64(size) > uint64(len(b)-offset) {
		return hdr, nil, fmt.Errorf("buffer too small for data: need %d, have %d", uint64(offset)+uint64(size), len(b))
	}
	var scratch *[65536]byte
	select {
	case scratch = <-pageDecodeScratch:
	default:
		scratch = new([65536]byte)
	}
	n, err := lz4.UncompressBlock(b[offset:offset+int(size)], scratch[:])
	if err == nil && n == len(scratch) {
		// Transfer ownership for maximum-sized pages instead of copying.
		// This buffer must NOT go back into the scratch cache.
		return hdr, scratch[:], nil
	}
	if err == nil {
		data = make([]byte, n)
		copy(data, scratch[:n])
	}
	select {
	case pageDecodeScratch <- scratch:
	default:
	}
	if err != nil {
		return hdr, nil, fmt.Errorf("decompress block: %w", err)
	}
	return hdr, data, nil
}
