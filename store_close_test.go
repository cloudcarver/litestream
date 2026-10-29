package litestream_test

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

type heldStoreSnapshotClient struct {
	*file.ReplicaClient
	db                   *litestream.DB
	started              chan struct{}
	once                 sync.Once
	canceledAfterDBClose atomic.Bool
}

func (c *heldStoreSnapshotClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (*ltx.FileInfo, error) {
	if level != litestream.SnapshotLevel {
		return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, rd)
	}
	// The actual snapshot reader holds database references until this owned
	// storage operation is canceled and returns to DB.Snapshot's cleanup.
	c.once.Do(func() { close(c.started) })
	<-ctx.Done()
	c.canceledAfterDBClose.Store(!c.db.IsOpen())
	return nil, ctx.Err()
}

func TestStoreCloseCancelsSnapshotBeforeClosingDatabase(t *testing.T) {
	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 0
	client := &heldStoreSnapshotClient{ReplicaClient: file.NewReplicaClient(t.TempDir()), db: db, started: make(chan struct{})}
	db.Replica = litestream.NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	store.L0RetentionCheckInterval = 0
	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			if err := store.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.started:
	case <-time.After(5 * time.Second):
		t.Fatal("owned snapshot did not start")
	}
	// Shutdown must still publish unsynced writes after it stops background
	// snapshot/compaction work. Canceling the Store must not cancel this flush.
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatal(err)
	}
	err := store.Close(context.Background())
	closed = true
	if err != nil {
		t.Fatal(err)
	}
	if client.canceledAfterDBClose.Load() {
		t.Error("database closed before its owned snapshot was canceled")
	}
	restoredPath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(t.Context(), litestream.RestoreOptions{OutputPath: restoredPath}); err != nil {
		t.Fatal(err)
	}
	restored := testingutil.MustOpenSQLDB(t, restoredPath)
	defer testingutil.MustCloseSQLDB(t, restored)
	var n int
	if err := restored.QueryRow(`SELECT count(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatal("shutdown lost a pending committed row", n)
	}
}
