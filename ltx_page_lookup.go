package litestream

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/superfly/ltx"
)

// findLTXPage reads the compact index, not the preceding compressed pages.
// Stream the index so verification keeps constant memory even for large DBs.
// This is a WAL continuity check; full-file checksum validation is performed
// by the separate LTX verification/restore paths.
func findLTXPage(ctx context.Context, f io.ReaderAt, fileSize int64, pgno uint32) (elem ltx.PageIndexElem, found bool, err error) {
	if err := ctx.Err(); err != nil {
		return elem, false, err
	}
	const footerSize = ltx.TrailerSize + 8
	if fileSize < ltx.HeaderSize+ltx.PageHeaderSize+footerSize+1 {
		return elem, false, fmt.Errorf("ltx file too short for page index: %d", fileSize)
	}
	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], fileSize-footerSize); err != nil {
		return elem, false, fmt.Errorf("read page index footer: %w", err)
	}
	indexSize := binary.BigEndian.Uint64(footer[:8])
	if indexSize == 0 || indexSize > uint64(fileSize-footerSize-ltx.HeaderSize-ltx.PageHeaderSize) {
		return elem, false, fmt.Errorf("invalid ltx page index size: %d", indexSize)
	}
	indexOffset := fileSize - footerSize - int64(indexSize)
	r := bufio.NewReader(io.NewSectionReader(f, indexOffset, int64(indexSize)))
	var previous uint64
	for {
		if err := ctx.Err(); err != nil {
			return elem, false, err
		}
		page, err := binary.ReadUvarint(r)
		if err != nil {
			return elem, false, fmt.Errorf("read page index number: %w", err)
		}
		if page == 0 {
			if _, err := r.ReadByte(); err != io.EOF {
				return elem, false, fmt.Errorf("page index has trailing data: %v", err)
			}
			return elem, found, nil
		}
		if page <= previous || page > math.MaxUint32 {
			return elem, false, fmt.Errorf("invalid ltx page index order: %d after %d", page, previous)
		}
		previous = page
		offset, err := binary.ReadUvarint(r)
		if err != nil {
			return elem, false, fmt.Errorf("read page index offset: %w", err)
		}
		size, err := binary.ReadUvarint(r)
		if err != nil {
			return elem, false, fmt.Errorf("read page index length: %w", err)
		}
		// Exclude the header, index and page-block end marker. Compare before
		// conversion/subtraction so corrupt uint64 offsets cannot overflow.
		end := uint64(indexOffset - ltx.PageHeaderSize)
		if offset < ltx.HeaderSize || offset > end || size < ltx.PageHeaderSize || size > end-offset {
			return elem, false, fmt.Errorf("invalid ltx page index range: offset=%d size=%d", offset, size)
		}
		if uint32(page) == pgno {
			elem.Offset, elem.Size, found = int64(offset), int64(size), true
		}
	}
}
