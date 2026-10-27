package litestream

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Gate the existing trace point immediately before the database file read.
// Channels fix the shutdown interleaving without sleeps or production hooks.
type snapshotReadGate struct {
	reached chan struct{}
	resume  chan struct{}
	once    sync.Once
	release sync.Once
}

func (g *snapshotReadGate) unblock() { g.release.Do(func() { close(g.resume) }) }

type snapshotGateHandler struct {
	slog.Handler
	gate *snapshotReadGate
}

func (h *snapshotGateHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "encode page from database" {
		h.gate.once.Do(func() {
			close(h.gate.reached)
			<-h.gate.resume
		})
	}
	return h.Handler.Handle(ctx, r)
}

func (h *snapshotGateHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &snapshotGateHandler{Handler: h.Handler.WithAttrs(attrs), gate: h.gate}
}

func (h *snapshotGateHandler) WithGroup(name string) slog.Handler {
	return &snapshotGateHandler{Handler: h.Handler.WithGroup(name), gate: h.gate}
}

func gateSnapshotRead(db *DB) *snapshotReadGate {
	gate := &snapshotReadGate{reached: make(chan struct{}), resume: make(chan struct{})}
	db.Logger = slog.New(&snapshotGateHandler{
		Handler: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(-8)}),
		gate:    gate,
	})
	return gate
}

func newSnapshotLifecycleDB(t *testing.T) *DB {
	t.Helper()
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	db.Replica = NewReplica(db)
	db.Replica.MonitorEnabled = false
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db.IsOpen() {
			if err := db.Close(context.Background()); err != nil {
				t.Errorf("close DB: %v", err)
			}
		}
	})
	sqldb, err := sql.Open("sqlite", db.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sqldb.Close(); err != nil {
			t.Errorf("close SQLite: %v", err)
		}
	})
	for _, statement := range []string{
		"PRAGMA journal_mode = WAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)",
		"INSERT INTO t VALUES (1, zeroblob(65536))",
	} {
		if _, err := sqldb.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Leave pages in the database file, not exclusively in the WAL.
	if err := db.Checkpoint(t.Context(), CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}
	return db
}

func waitSnapshotEvent(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot lifecycle event did not complete")
	}
}

func assertSnapshotReleased(t *testing.T, db *DB) {
	t.Helper()
	if !db.chkMu.TryLock() {
		t.Fatal("snapshot still holds the checkpoint lock")
	}
	db.chkMu.Unlock()
}

func TestDB_CloseWaitsForSnapshotEncoder(t *testing.T) {
	db := newSnapshotLifecycleDB(t)
	gate := gateSnapshotRead(db)
	defer gate.unblock()
	_, r, err := db.SnapshotReader(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close snapshot: %v", err)
		}
	})
	readDone := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, r); readDone <- err }()
	waitSnapshotEvent(t, gate.reached)

	closed := make(chan struct{})
	var closeErr error
	go func() { closeErr = db.Close(context.Background()); close(closed) }()
	t.Cleanup(func() {
		waitSnapshotEvent(t, closed)
		if closeErr != nil {
			t.Errorf("DB.Close: %v", closeErr)
		}
	})
	select {
	case err := <-readDone:
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("snapshot read error = %v, want os.ErrClosed", err)
		}
	case <-closed:
		t.Fatal("DB.Close returned while the snapshot encoder was still reading")
	case <-time.After(5 * time.Second):
		t.Fatal("DB.Close did not cancel the snapshot reader")
	}

	db.mu.RLock()
	f := db.f
	db.mu.RUnlock()
	if f == nil {
		t.Fatal("DB.Close cleared the file while the snapshot encoder was still reading")
	}
	if _, err := f.Stat(); err != nil {
		t.Fatalf("snapshot's file was closed early: %v", err)
	}
	gate.unblock()
	waitSnapshotEvent(t, closed)
	assertSnapshotReleased(t, db)
}

func TestDB_SnapshotReaderCancellation(t *testing.T) {
	for _, mode := range []string{"caller", "caller_cause", "reader", "database", "database_canceled_close"} {
		t.Run(mode, func(t *testing.T) {
			db := newSnapshotLifecycleDB(t)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			_, r, err := db.SnapshotReader(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := r.Close(); err != nil {
					t.Errorf("close snapshot: %v", err)
				}
			})
			wantErr := context.Canceled
			actionDone := make(chan struct{})
			var actionErr error
			switch mode {
			case "caller":
				cancel(nil)
				close(actionDone)
			case "caller_cause":
				wantErr = errors.New("snapshot request interrupted")
				cancel(wantErr)
				close(actionDone)
			case "reader":
				wantErr = io.ErrClosedPipe
				go func() { actionErr = r.Close(); close(actionDone) }()
			case "database", "database_canceled_close":
				wantErr = os.ErrClosed
				closeCtx := context.Background()
				if mode == "database_canceled_close" {
					canceled, stop := context.WithCancel(closeCtx)
					stop()
					closeCtx = canceled
				}
				go func() { actionErr = db.Close(closeCtx); close(actionDone) }()
			}
			// There is deliberately no pipe consumer until cancellation/close
			// completes. Shutdown must unblock an abandoned snapshot itself.
			waitSnapshotEvent(t, actionDone)
			if mode == "database_canceled_close" {
				if !errors.Is(actionErr, context.Canceled) {
					t.Fatalf("canceled DB.Close = %v, want context.Canceled", actionErr)
				}
			} else if actionErr != nil {
				t.Fatal(actionErr)
			}
			if _, err := io.Copy(io.Discard, r); !errors.Is(err, wantErr) {
				t.Fatalf("snapshot read error = %v, want %v", err, wantErr)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			assertSnapshotReleased(t, db)
		})
	}
}

func TestSnapshotReadCloser_JoinsEncoder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		pr, pw := io.Pipe()
		encoderDone, releaseEncoder, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		r := &snapshotReadCloser{PipeReader: pr, cancel: cancel, done: encoderDone}
		go func() {
			<-ctx.Done()
			<-releaseEncoder
			if err := pw.Close(); err != nil {
				t.Errorf("close pipe: %v", err)
			}
			close(encoderDone)
		}()
		go func() {
			if err := r.Close(); err != nil {
				t.Errorf("close reader: %v", err)
			}
			close(closed)
		}()
		synctest.Wait()
		if !errors.Is(context.Cause(ctx), io.ErrClosedPipe) {
			t.Errorf("Close did not cancel encoder: %v", context.Cause(ctx))
		}
		select {
		case <-closed:
			t.Error("Close returned before encoder exited")
		default:
		}
		close(releaseEncoder)
		synctest.Wait()
		<-closed
	})
}

func TestDB_SnapshotReaderAfterCloseAndReopen(t *testing.T) {
	db := newSnapshotLifecycleDB(t)
	if err := db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, r, err := db.SnapshotReader(t.Context()); !errors.Is(err, os.ErrClosed) {
		if r != nil {
			if closeErr := r.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
		t.Fatalf("SnapshotReader after Close = %v, want os.ErrClosed", err)
	}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(t.Context()); err != nil {
		t.Fatalf("snapshot after reopening DB: %v", err)
	}
	assertSnapshotReleased(t, db)
}

func TestStore_CloseStopsSnapshotsBeforeClosingDB(t *testing.T) {
	db := newSnapshotLifecycleDB(t)
	s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}})
	s.L0Retention = 0
	gate := gateSnapshotRead(db)
	defer gate.unblock()
	if err := s.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitSnapshotEvent(t, gate.reached)
	closed := make(chan struct{})
	var closeErr error
	go func() { closeErr = s.Close(context.Background()); close(closed) }()
	t.Cleanup(func() {
		waitSnapshotEvent(t, closed)
		if closeErr != nil {
			t.Errorf("Store.Close: %v", closeErr)
		}
	})
	waitSnapshotEvent(t, s.ctx.Done())
	if !db.IsOpen() {
		t.Fatal("Store closed its DB before its snapshot monitor exited")
	}
	gate.unblock()
	waitSnapshotEvent(t, closed)
	if db.IsOpen() {
		t.Fatal("Store.Close did not close its DB")
	}
	assertSnapshotReleased(t, db)
}
