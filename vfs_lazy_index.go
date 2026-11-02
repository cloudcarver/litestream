//go:build vfs

package litestream

import (
	"context"
	"fmt"

	"github.com/superfly/ltx"
)

// Bound remote probes on a cache miss even with an unusually long restore
// history. Keep the largest indexes lazy; other adjacent runs are coalesced
// eagerly, so a large base is not displaced by many recent small deltas.
const maxLazyIndexRuns = 8

type vfsIndexRun struct {
	lookup        PageIndexLookup
	pages         map[uint32]ltx.PageIndexElem
	commit, floor uint32
}
type vfsIndexBase struct{ runs []vfsIndexRun }

func (b *vfsIndexBase) Lookup(ctx context.Context, pgno uint32) (ltx.PageIndexElem, bool, error) {
	for i := len(b.runs) - 1; i >= 0; i-- {
		r := &b.runs[i]
		if pgno > r.commit {
			return ltx.PageIndexElem{}, false, nil
		}
		var e ltx.PageIndexElem
		var found bool
		var err error
		if r.lookup != nil {
			e, found, err = r.lookup.Lookup(ctx, pgno)
		} else {
			e, found = r.pages[pgno]
		}
		if err != nil {
			return e, false, err
		}
		if found {
			return e, true, nil
		}
		// Truncate followed by expansion must not resurrect an older page.
		if pgno > r.floor {
			return ltx.PageIndexElem{}, false, nil
		}
	}
	return ltx.PageIndexElem{}, false, nil
}

func (f *VFSFile) buildIndexState(ctx context.Context, infos []*ltx.FileInfo) (map[uint32]ltx.PageIndexElem, PageIndexLookup, uint32, error) {
	lookups := make([]PageIndexLookup, len(infos))
	selected := make([]int, 0, maxLazyIndexRuns+1)
	cost := func(i int) int64 {
		if s, ok := lookups[i].(interface{ PageIndexSize() int64 }); ok && s.PageIndexSize() > 0 {
			return s.PageIndexSize()
		}
		return infos[i].Size
	}
	// Hydration already reads the entire database. Keep its eager contract.
	if f.hydrationPath == "" {
		for i := len(infos) - 1; i >= 0; i-- {
			var err error
			lookups[i], err = OpenPageIndexLookup(ctx, f.client, infos[i])
			if err != nil {
				return nil, nil, 0, err
			}
			if lookups[i] != nil {
				selected = append(selected, i)
				if len(selected) > maxLazyIndexRuns {
					smallest := 0
					for j, idx := range selected {
						prev := selected[smallest]
						if cost(idx) < cost(prev) || (cost(idx) == cost(prev) && idx < prev) {
							smallest = j
						}
					}
					lookups[selected[smallest]] = nil
					selected = append(selected[:smallest], selected[smallest+1:]...)
				}
			}
		}
	}
	if len(selected) == 0 {
		index, commit, err := f.buildIndexMap(ctx, infos)
		return index, nil, commit, err
	}
	base := &vfsIndexBase{}
	var commit uint32
	for i, info := range infos {
		header, err := FetchLTXHeader(ctx, f.client, info)
		if err != nil {
			return nil, nil, 0, err
		}
		commit = header.Commit
		if lookups[i] != nil {
			base.runs = append(base.runs, vfsIndexRun{lookup: lookups[i], commit: commit, floor: commit})
			continue
		}
		pages, err := FetchPageIndex(ctx, f.client, info)
		if err != nil {
			return nil, nil, 0, err
		}
		if len(base.runs) == 0 || base.runs[len(base.runs)-1].lookup != nil {
			base.runs = append(base.runs, vfsIndexRun{pages: make(map[uint32]ltx.PageIndexElem), floor: commit})
		}
		r := &base.runs[len(base.runs)-1]
		for pgno, e := range pages {
			r.pages[pgno] = e
		}
		if commit < r.commit {
			for pgno := range r.pages {
				if pgno > commit {
					delete(r.pages, pgno)
				}
			}
		}
		r.commit, r.floor = commit, min(r.floor, commit)
	}
	return make(map[uint32]ltx.PageIndexElem), base, commit, nil
}

func lookupIndexBase(ctx context.Context, base PageIndexLookup, limit, page uint32) (ltx.PageIndexElem, bool, error) {
	if base == nil || page > limit {
		return ltx.PageIndexElem{}, false, nil
	}
	return base.Lookup(ctx, page)
}

// Caller holds f.mu. The ordinary ReadAt path releases it before lazy I/O.
func (f *VFSFile) lookupIndexLocked(ctx context.Context, pgno uint32) (ltx.PageIndexElem, bool, error) {
	if e, ok := f.index[pgno]; ok {
		return e, true, nil
	}
	return lookupIndexBase(ctx, f.indexBase, f.indexBaseLimit, pgno)
}

// Explicit full snapshots require all addresses. The caller captures the
// immutable base and copies the overlay, then releases f.mu before lazy I/O.
func fillSnapshotIndex(ctx context.Context, pages map[uint32]ltx.PageIndexElem, base PageIndexLookup, limit, pageSize uint32) error {
	if base != nil {
		for page := uint64(1); page <= uint64(limit); page++ {
			if uint32(page) == ltx.LockPgno(pageSize) {
				continue
			}
			if _, overridden := pages[uint32(page)]; overridden {
				continue
			}
			e, found, err := base.Lookup(ctx, uint32(page))
			if err != nil {
				return fmt.Errorf("snapshot page index: %w", err)
			}
			if found {
				pages[uint32(page)] = e
			}
		}
	}
	return nil
}
