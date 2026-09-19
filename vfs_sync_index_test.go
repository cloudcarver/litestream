//go:build vfs

package litestream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3" // Link SQLite's C API for sqlite3vfs in package tests.
	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

func openSyncIndexTestFile(t *testing.T, client *writeTestReplicaClient) *VFSFile {
	t.Helper()
	f := setupWriteableVFSFile(t, client)
	f.PollInterval = time.Hour // All polls in these tests are explicit.
	f.CacheSize = 4096         // Deliberately smaller than the write set.
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func assertSyncIndexPage(t *testing.T, f *VFSFile, pgno uint32, want []byte) {
	t.Helper()
	got := make([]byte, 4096)
	if n, err := f.ReadAt(got, int64(pgno-1)*4096); err != nil || n != len(got) {
		t.Fatalf("read page %d: n=%d err=%v", pgno, n, err)
	}
	// Page 1's journal mode and change counter are virtualized by the VFS.
	if pgno == 1 {
		got = got[100:]
		want = want[100:]
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("page %d differs from latest write", pgno)
	}
}

func TestVFSFile_SyncInstallsPageIndex(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 2, map[uint32][]byte{
		1: bytes.Repeat([]byte{1}, 4096), 2: bytes.Repeat([]byte{2}, 4096),
	})
	f := openSyncIndexTestFile(t, client)
	counted := &countingSyncIndexClient{ReplicaClient: client}
	f.client = counted
	want := make(map[uint32][]byte)
	for pgno := uint32(2); pgno <= 10; pgno++ {
		want[pgno] = bytes.Repeat([]byte{byte(pgno + 10)}, 4096)
		if _, err := f.WriteAt(want[pgno], int64(pgno-1)*4096); err != nil {
			t.Fatal(err)
		}
	}
	readsBeforeSync := counted.reads.Load()
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	if got := counted.reads.Load() - readsBeforeSync; got != 0 {
		t.Fatalf("sync downloaded %d objects; index maintenance must use local encoder offsets", got)
	}
	if size, err := f.FileSize(); err != nil || size != 10*4096 {
		t.Fatalf("size after sync=%d err=%v, want %d", size, err, 10*4096)
	}
	if len(f.dirty) != 0 {
		t.Fatal("sync did not clear dirty pages")
	}
	f.cache.Purge()
	for pgno, data := range want {
		assertSyncIndexPage(t, f, pgno, data)
		if elem := f.index[pgno]; elem.Level != 0 || elem.MinTXID != 2 || elem.MaxTXID != 2 || elem.Size <= 0 {
			t.Fatalf("page %d index=%+v", pgno, elem)
		}
	}
	// A partial rewrite must fetch the NEW remote page after eviction.
	f.cache.Purge()
	if _, err := f.WriteAt([]byte{99}, 4096+100); err != nil {
		t.Fatal(err)
	}
	want[2][100] = 99
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, want[2])
}

type countingSyncIndexClient struct {
	ReplicaClient
	reads atomic.Int64
}

func (c *countingSyncIndexClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.reads.Add(1)
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func TestVFSFile_TruncateOnlySyncAndColdReopen(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 3, map[uint32][]byte{1: make([]byte, 4096), 2: make([]byte, 4096), 3: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	if err := f.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	if f.Pos().TXID != 2 {
		t.Fatal("size-only change was not uploaded")
	}
	reader := openSyncIndexTestFile(t, client)
	if size, err := reader.FileSize(); err != nil || size != 4096 {
		t.Fatalf("cold size=%d err=%v", size, err)
	}
	if len(reader.index) != 1 {
		t.Fatalf("truncated pages survived cold rebuild: %d", len(reader.index))
	}
}

func TestVFSFile_OutOfOrderPageGrowth(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	for _, pgno := range []uint32{3, 2} {
		if _, err := f.WriteAt(bytes.Repeat([]byte{byte(pgno)}, 4096), int64(pgno-1)*4096); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	for _, pgno := range []uint32{2, 3} {
		assertSyncIndexPage(t, f, pgno, bytes.Repeat([]byte{byte(pgno)}, 4096))
	}
}

type failingPageReadClient struct {
	ReplicaClient
	err error
}

func (c *failingPageReadClient) OpenLTXFile(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
	return nil, c.err
}

func TestVFSFile_PartialWriteDoesNotZeroPageAfterReadError(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 2, map[uint32][]byte{1: make([]byte, 4096), 2: bytes.Repeat([]byte{2}, 4096)})
	f := openSyncIndexTestFile(t, client)
	wantErr := errors.New("injected page read failure")
	f.client = &failingPageReadClient{ReplicaClient: client, err: wantErr}
	if _, err := f.WriteAt([]byte{9}, 4096+100); !errors.Is(err, wantErr) {
		t.Fatalf("write error=%v", err)
	}
	if len(f.dirty) != 0 {
		t.Fatal("failed read produced a zero-filled dirty page")
	}
	f.client = client
	assertSyncIndexPage(t, f, 2, bytes.Repeat([]byte{2}, 4096))
}

// publishSyncIndexL1 creates a real compacted file without deleting any L0.
func publishSyncIndexL1(t *testing.T, client *writeTestReplicaClient, minTXID, maxTXID ltx.TXID) {
	t.Helper()
	var readers []io.Reader
	for txid := minTXID; txid <= maxTXID; txid++ {
		r, err := client.OpenLTXFile(context.Background(), 0, txid, txid, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		readers = append(readers, r)
	}
	var out bytes.Buffer
	c, err := ltx.NewCompactor(&out, readers)
	if err != nil {
		t.Fatal(err)
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteLTXFile(context.Background(), 1, minTXID, maxTXID, &out); err != nil {
		t.Fatal(err)
	}
}

func TestVFSFile_OlderL1DoesNotReplaceSyncedPages(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 2, map[uint32][]byte{1: make([]byte, 4096), 2: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	for txid := 2; txid <= 3; txid++ {
		if _, err := f.WriteAt(bytes.Repeat([]byte{byte(txid)}, 4096), 4096); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(bytes.Repeat([]byte{byte(txid)}, 4096), int64(txid)*4096); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(0); err != nil {
			t.Fatal(err)
		}
	}
	// Delayed compaction covers an older/smaller state and straddles the L1 watermark.
	publishSyncIndexL1(t, client, 1, 2)
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, bytes.Repeat([]byte{3}, 4096))
	assertSyncIndexPage(t, f, 4, bytes.Repeat([]byte{3}, 4096))
	if size, err := f.FileSize(); err != nil || size != 4*4096 || f.Pos().TXID != 3 {
		t.Fatalf("older L1 regressed writer: size=%d pos=%s err=%v", size, f.Pos(), err)
	}
	if got := f.MaxTXID1(); got != 2 {
		t.Fatalf("straddling L1 was not consumed: watermark=%d", got)
	}
}

type pausedSyncIndexClient struct {
	ReplicaClient
	pause  atomic.Bool
	ready  chan struct{}
	resume chan struct{}
}

func (c *pausedSyncIndexClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, metadata bool) (ltx.FileIterator, error) {
	itr, err := c.ReplicaClient.LTXFiles(ctx, level, seek, metadata)
	if level == 1 && c.pause.CompareAndSwap(true, false) {
		close(c.ready)
		select {
		case <-c.resume:
		case <-ctx.Done():
			if itr != nil {
				_ = itr.Close()
			}
			return nil, ctx.Err()
		}
	}
	return itr, err
}

func TestVFSFile_PollStartedBeforeSyncCannotRegressPosition(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	paused := &pausedSyncIndexClient{ReplicaClient: client, ready: make(chan struct{}), resume: make(chan struct{})}
	f.client = paused
	paused.pause.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.pollReplicaClient(ctx) }()
	select {
	case <-paused.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	data := bytes.Repeat([]byte{7}, 4096)
	if _, err := f.WriteAt(data, 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	close(paused.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.Pos().TXID != 2 {
		t.Fatalf("late poll regressed position to %s", f.Pos())
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, data)
}

func TestVFSFile_TruncateKeepsIndexAndSizeConsistent(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 3, map[uint32][]byte{
		1: make([]byte, 4096), 2: bytes.Repeat([]byte{2}, 4096), 3: bytes.Repeat([]byte{3}, 4096),
	})
	f := openSyncIndexTestFile(t, client)
	assertSyncIndexPage(t, f, 3, bytes.Repeat([]byte{3}, 4096))
	if err := f.Truncate(2 * 4096); err != nil {
		t.Fatal(err)
	}
	if size, err := f.FileSize(); err != nil || size != 2*4096 {
		t.Fatalf("size=%d err=%v", size, err)
	}
	// Re-extension must not resurrect the truncated page from the cache/index.
	if _, err := f.WriteAt([]byte{9}, 2*4096+100); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 4096)
	want[100] = 9
	assertSyncIndexPage(t, f, 3, want)
}

type failingSyncIndexClient struct {
	ReplicaClient
	err error
}

func (c *failingSyncIndexClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, c.err // Fail before consuming the stream.
}

func TestVFSFile_FailedSyncRetainsDirtyPagesAndIndex(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 2, map[uint32][]byte{1: make([]byte, 4096), 2: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	old := f.index[2]
	want := bytes.Repeat([]byte{8}, 4096)
	if _, err := f.WriteAt(want, 4096); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("injected upload failure")
	f.client = &failingSyncIndexClient{ReplicaClient: client, err: wantErr}
	if err := f.Sync(0); !errors.Is(err, wantErr) {
		t.Fatalf("sync error=%v", err)
	}
	if f.index[2] != old || f.Pos().TXID != 1 || len(f.dirty) != 1 {
		t.Fatal("failed sync published local state")
	}
	assertSyncIndexPage(t, f, 2, want)
	f.client = client
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, want)
}

func TestVFSFile_PendingSizeIsNotVisibleInsideReadTransaction(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 3, map[uint32][]byte{1: make([]byte, 4096), 2: make([]byte, 4096), 3: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	if err := f.SetWriteEnabled(false); err != nil {
		t.Fatal(err)
	}
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	createTestLTXFile(t, client, 2, 4096, 2, map[uint32][]byte{1: make([]byte, 4096)})
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	if size, err := f.FileSize(); err != nil || size != 3*4096 {
		t.Fatalf("active reader size=%d err=%v", size, err)
	}
	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatal(err)
	}
	if size, err := f.FileSize(); err != nil || size != 2*4096 {
		t.Fatalf("next reader size=%d err=%v", size, err)
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, make([]byte, 4096)) // Truncation delta need not rewrite unchanged pages.
}

func TestVFSFile_HydratedReaderFallsBackAfterPendingUpdate(t *testing.T) {
	client := newWriteTestReplicaClient()
	old, updated := bytes.Repeat([]byte{2}, 4096), bytes.Repeat([]byte{9}, 4096)
	createTestLTXFile(t, client, 1, 4096, 2, map[uint32][]byte{1: make([]byte, 4096), 2: old})
	f := openSyncIndexTestFile(t, client)
	if err := f.SetWriteEnabled(false); err != nil {
		t.Fatal(err)
	}
	h := NewHydrator(filepath.Join(t.TempDir(), "hydrated.db"), false, 4096, client, f.logger)
	if err := h.Init(); err != nil {
		t.Fatal(err)
	}
	if err := h.WritePage(2, old); err != nil {
		t.Fatal(err)
	}
	h.SetComplete()
	f.hydrator = h
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	createTestLTXFile(t, client, 2, 4096, 2, map[uint32][]byte{2: updated})
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertSyncIndexPage(t, f, 2, old)
	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatal(err)
	}
	assertSyncIndexPage(t, f, 2, updated)
}
