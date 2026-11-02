//go:build vfs

package litestream

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

type lazyIndexFixture struct {
	*writeTestReplicaClient
	indexes map[string]map[uint32]ltx.PageIndexElem
	calls   atomic.Int64
	fail    atomic.Bool
}
type fixtureIndexLookup struct {
	fixture *lazyIndexFixture
	pages   map[uint32]ltx.PageIndexElem
}

var errLazyIndexUnavailable = errors.New("lazy index unavailable")

func (l *fixtureIndexLookup) Lookup(ctx context.Context, pg uint32) (ltx.PageIndexElem, bool, error) {
	l.fixture.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return ltx.PageIndexElem{}, false, err
	}
	if l.fixture.fail.Load() {
		return ltx.PageIndexElem{}, false, errLazyIndexUnavailable
	}
	e, ok := l.pages[pg]
	return e, ok, nil
}
func (c *lazyIndexFixture) OpenPageIndexLookup(ctx context.Context, info *ltx.FileInfo) (PageIndexLookup, error) {
	pages, ok := c.indexes[ltxKey(info.Level, info.MinTXID, info.MaxTXID)]
	if !ok {
		return nil, nil
	}
	return &fixtureIndexLookup{fixture: c, pages: pages}, nil
}
func lazyFixture(t *testing.T, c *writeTestReplicaClient) *lazyIndexFixture {
	t.Helper()
	f := &lazyIndexFixture{writeTestReplicaClient: c, indexes: make(map[string]map[uint32]ltx.PageIndexElem)}
	for _, infos := range c.ltxFiles {
		for _, info := range infos {
			index, err := FetchPageIndex(t.Context(), c, info)
			if err != nil {
				t.Fatal(err)
			}
			f.indexes[ltxKey(info.Level, info.MinTXID, info.MaxTXID)] = index
		}
	}
	return f
}
func openLazyFixture(t *testing.T, c *lazyIndexFixture) *VFSFile {
	t.Helper()
	f := setupWriteableVFSFile(t, c.writeTestReplicaClient)
	f.client = c
	f.PollInterval = time.Hour
	f.CacheSize = 4096 * 4
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	if f.indexBase == nil || len(f.index) != 0 {
		t.Fatal("bootstrap materialized the index")
	}
	return f
}
func lazyThreePages(t *testing.T) *writeTestReplicaClient {
	c := newWriteTestReplicaClient()
	createTestLTXFile(t, c, 1, 4096, 3, map[uint32][]byte{1: make([]byte, 4096), 2: bytes.Repeat([]byte{2}, 4096), 3: bytes.Repeat([]byte{3}, 4096)})
	return c
}

func TestVFSFile_LazyIndexReadsWritesAndErrors(t *testing.T) {
	c := lazyFixture(t, lazyThreePages(t))
	f := openLazyFixture(t, c)
	if c.calls.Load() != 0 {
		t.Fatal("startup loaded page locations")
	}
	assertSyncIndexPage(t, f, 2, bytes.Repeat([]byte{2}, 4096))
	f.cache.Purge()
	c.fail.Store(true)
	if _, err := f.ReadAt(make([]byte, 4096), 4096); !errors.Is(err, errLazyIndexUnavailable) {
		t.Fatal("index error became zero page", err)
	}
	if _, err := f.WriteAt([]byte{77}, 4096+100); !errors.Is(err, errLazyIndexUnavailable) {
		t.Fatal("index error lost on partial write", err)
	}
	c.fail.Store(false)
	if _, err := f.WriteAt([]byte{77}, 4096+100); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	want := bytes.Repeat([]byte{2}, 4096)
	want[100] = 77
	assertSyncIndexPage(t, f, 2, want)
	if err := f.RefreshIndex(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, want)
	c.fail.Store(true)
	if _, err := f.ReadAt(make([]byte, 4096), 2*4096); !errors.Is(err, errLazyIndexUnavailable) {
		t.Fatal("refresh discarded untouched base page", err)
	}
}

func TestVFSFile_LazyIndexTruncateExpandDoesNotResurrect(t *testing.T) {
	c := lazyFixture(t, lazyThreePages(t))
	f := openLazyFixture(t, c)
	assertSyncIndexPage(t, f, 3, bytes.Repeat([]byte{3}, 4096))
	if err := f.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 * 4096); err != nil {
		t.Fatal(err)
	}
	assertSyncIndexPage(t, f, 3, make([]byte, 4096))
	if _, err := f.WriteAt([]byte{77}, 2*4096+100); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	want := make([]byte, 4096)
	want[100] = 77
	assertSyncIndexPage(t, f, 3, want)
}

func TestVFSFile_LazyIndexPendingTruncationKeepsReaderSnapshot(t *testing.T) {
	raw := lazyThreePages(t)
	c := lazyFixture(t, raw)
	f := openLazyFixture(t, c)
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	createTestLTXFile(t, raw, 2, 4096, 1, nil)
	createTestLTXFile(t, raw, 3, 4096, 3, map[uint32][]byte{1: make([]byte, 4096)})
	if err := f.pollReplicaClient(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertSyncIndexPage(t, f, 3, bytes.Repeat([]byte{3}, 4096))
	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatal(err)
	}
	assertSyncIndexPage(t, f, 3, make([]byte, 4096))
	// Cold lookup must also stop at the truncation instead of reaching txid 1.
	cold := openLazyFixture(t, lazyFixture(t, raw))
	assertSyncIndexPage(t, cold, 3, make([]byte, 4096))
}

func TestVFSFile_LazyIndexBoundsRemoteHistoryProbes(t *testing.T) {
	raw := lazyThreePages(t)
	for tx := ltx.TXID(2); tx <= 20; tx++ {
		createTestLTXFile(t, raw, tx, 4096, 3, map[uint32][]byte{1: make([]byte, 4096)})
	}
	c := lazyFixture(t, raw)
	f := openLazyFixture(t, c)
	assertSyncIndexPage(t, f, 3, bytes.Repeat([]byte{3}, 4096))
	if c.calls.Load() > maxLazyIndexRuns {
		t.Fatal("unbounded remote fanout", c.calls.Load())
	}
}

func TestVFSFile_LazyIndexSnapshotIncludesUntouchedBase(t *testing.T) {
	c := lazyFixture(t, lazyThreePages(t))
	f := openLazyFixture(t, c)
	want := bytes.Repeat([]byte{7}, 4096)
	if _, err := f.WriteAt(want, 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	// Enable the explicit snapshot entrypoint without starting background work.
	f.compactor = &Compactor{}
	info, err := f.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	index, err := FetchPageIndex(t.Context(), c, info)
	if err != nil || len(index) != 3 {
		t.Fatal("snapshot omitted base pages", len(index), err)
	}
	for page, expected := range map[uint32][]byte{1: make([]byte, 4096), 2: want, 3: bytes.Repeat([]byte{3}, 4096)} {
		e := index[page]
		_, data, err := FetchPage(t.Context(), c, e.Level, e.MinTXID, e.MaxTXID, e.Offset, e.Size)
		if err != nil || !bytes.Equal(data, expected) {
			t.Fatalf("snapshot page %d: %v", page, err)
		}
	}
}

type lockPageIndexLookup struct {
	lock, last uint32
	calls      uint32
}

func (l *lockPageIndexLookup) Lookup(_ context.Context, page uint32) (ltx.PageIndexElem, bool, error) {
	if page == l.lock {
		return ltx.PageIndexElem{}, false, errors.New("SQLite lock page must not be read")
	}
	l.calls++
	return ltx.PageIndexElem{Offset: int64(page) * 4096}, page == 1 || page == l.last, nil
}

func TestVFSFile_LazyIndexSnapshotSkipsLockPageAbove1GB(t *testing.T) {
	lock := ltx.LockPgno(4096)
	base := &lockPageIndexLookup{lock: lock, last: lock + 1}
	index := make(map[uint32]ltx.PageIndexElem)
	err := fillSnapshotIndex(t.Context(), index, base, base.last, 4096)
	if err != nil || len(index) != 2 || base.calls != base.last-1 || index[base.last].Offset != int64(base.last)*4096 {
		t.Fatal("snapshot crossed the SQLite lock page incorrectly", len(index), base.calls, err)
	}
}

type blockedSnapshotLookup struct{ entered chan struct{} }

func (l *blockedSnapshotLookup) Lookup(ctx context.Context, _ uint32) (ltx.PageIndexElem, bool, error) {
	close(l.entered)
	<-ctx.Done()
	return ltx.PageIndexElem{}, false, ctx.Err()
}

func TestVFSFile_LazySnapshotDoesNotBlockFileWhileLoadingIndex(t *testing.T) {
	f := openLazyFixture(t, lazyFixture(t, lazyThreePages(t)))
	base := &blockedSnapshotLookup{entered: make(chan struct{})}
	f.mu.Lock()
	f.indexBase, f.compactor = base, &Compactor{}
	f.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := f.Snapshot(ctx); done <- err }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	<-base.entered
	size := make(chan int64, 1)
	go func() {
		n, err := f.FileSize()
		if err != nil {
			t.Error(err)
		}
		size <- n
	}()
	select {
	case n := <-size:
		if n != 3*4096 {
			t.Fatal("file size changed during snapshot", n)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot index I/O held the file lock")
	}
}
