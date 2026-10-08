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

func TestVFSFileRefreshPreservesHotPagesAndReleasesOldFiles(t *testing.T) {
	ctx := context.Background()
	client := newWriteTestReplicaClient()
	unchanged := bytes.Repeat([]byte{4}, 4096)
	createTestLTXFile(t, client, 1, 4096, 3, map[uint32][]byte{1: make([]byte, 4096), 2: bytes.Repeat([]byte{2}, 4096), 3: unchanged})
	page := bytes.Repeat([]byte{3}, 4096)
	createTestLTXFile(t, client, 2, 4096, 3, map[uint32][]byte{2: page})
	f := openSyncIndexTestFile(t, client)
	f.cache.Resize(4)
	counted := &countingSyncIndexClient{ReplicaClient: client}
	f.client = counted
	assertSyncIndexPage(t, f, 2, page)
	assertSyncIndexPage(t, f, 3, unchanged)
	cache, buffer := f.cache, f.bufferFile
	if _, err := NewCompactor(client, f.logger).Compact(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if f.cache != cache || f.bufferFile != buffer || f.index[2].Level != 1 {
		t.Fatal("refresh replaced runtime state or kept L0 reference")
	}
	before := counted.reads.Load()
	assertSyncIndexPage(t, f, 2, page)
	assertSyncIndexPage(t, f, 3, unchanged)
	if counted.reads.Load() != before {
		t.Fatal("equivalent compaction evicted hot page")
	}
	client.mu.Lock()
	for _, info := range client.ltxFiles[0] {
		delete(client.ltxData, ltxKey(0, info.MinTXID, info.MaxTXID))
	}
	delete(client.ltxFiles, 0)
	client.mu.Unlock()
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, page)
	if _, err := f.WriteAt([]byte{99}, 4096+100); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	if f.Pos().TXID != 3 {
		t.Fatal("refresh broke writer TXID", f.Pos())
	}
	page[100] = 99
	f.cache.Purge()
	assertSyncIndexPage(t, f, 2, page)
}

func TestVFSFileRefreshFailureAndActiveSnapshotKeepOldIndex(t *testing.T) {
	client := newWriteTestReplicaClient()
	page := bytes.Repeat([]byte{2}, 4096)
	createTestLTXFile(t, client, 1, 4096, 2, map[uint32][]byte{1: make([]byte, 4096), 2: page})
	f := openSyncIndexTestFile(t, client)
	assertSyncIndexPage(t, f, 2, page)
	old := f.index[2]
	want := errors.New("injected refresh GET failure")
	f.client = &failingPageReadClient{ReplicaClient: client, err: want}
	if err := f.RefreshIndex(context.Background()); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if f.index[2] != old || !f.cache.Contains(2) {
		t.Fatal("failed refresh discarded working index/cache")
	}
	f.client = client
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	if err := f.RefreshIndex(context.Background()); !errors.Is(err, sqlite3vfs.BusyError) {
		t.Fatal(err)
	}
	assertSyncIndexPage(t, f, 2, page)
	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatal(err)
	}
	if err := f.RefreshIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type refreshLongPoll struct {
	ReplicaClient
	once    atomic.Bool
	entered chan struct{}
}

func (c *refreshLongPoll) LTXFiles(ctx context.Context, level int, seek ltx.TXID, metadata bool) (ltx.FileIterator, error) {
	if level == 0 && c.once.CompareAndSwap(false, true) {
		close(c.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.ReplicaClient.LTXFiles(ctx, level, seek, metadata)
}
func TestVFSFileRefreshCancelsAndDrainsLongPoll(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	c := &refreshLongPoll{ReplicaClient: client, entered: make(chan struct{})}
	f.client = c
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.pollReplicaClient(ctx) }()
	<-c.entered
	if err := f.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("old discovery was not drained", err)
	}
}
