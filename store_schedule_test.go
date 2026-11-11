package litestream

import (
	"context"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

// Observe the real monitor after its metadata lookup. Slow replica I/O can
// cross a wall-clock scheduling boundary even when there is nothing to compact.
func TestStoreCompactionMonitorSchedulesAfterWork(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay time.Duration
	}{
		{"before_boundary", 10 * time.Millisecond},
		{"cross_boundary", time.Second},
		{"missed_boundaries", 95 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Start half a second before a minute, independently of the
				// synthetic clock's initial wall time.
				start := time.Now().Truncate(time.Minute).Add(2*time.Minute - 500*time.Millisecond)
				time.Sleep(time.Until(start))
				firstRead := true
				client := &followTestReplicaClient{LTXFilesFunc: func(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
					if firstRead {
						firstRead = false
						time.Sleep(tc.delay)
					}
					return ltx.NewFileInfoSliceIterator(nil), nil
				}}
				db, observed := compactionScheduleDB(client)
				ctx, cancel := context.WithCancel(t.Context())
				store := NewStore([]*DB{db}, nil)
				defer store.cancel()
				db.SetLogger(slog.New(observed))
				done := make(chan struct{})
				go func() {
					defer close(done)
					store.monitorCompactionLevel(ctx, &CompactionLevel{Level: 3, Interval: time.Minute})
				}()
				first, second := <-observed.calls, <-observed.calls
				cancel()
				<-done
				if first.Before(start.Add(tc.delay)) {
					t.Fatal("metadata delay was not exercised", first.Sub(start))
				}
				want := first.Truncate(time.Minute).Add(time.Minute)
				if !second.Equal(want) {
					t.Fatalf("next monitor pass at %s, want next wall-clock boundary %s after work completed at %s", second, want, first)
				}
			})
		})
	}
}

func TestStoreCompactionMonitorKeepsUnreadyRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, observed := compactionScheduleDB(&followTestReplicaClient{})
		db.pageSize = 0
		ctx, cancel := context.WithCancel(t.Context())
		store := NewStore([]*DB{db}, nil)
		defer store.cancel()
		db.SetLogger(slog.New(observed))
		done := make(chan struct{})
		go func() {
			defer close(done)
			store.monitorCompactionLevel(ctx, &CompactionLevel{Level: 3, Interval: time.Minute})
		}()
		first := <-observed.calls
		db.mu.Lock()
		db.pageSize = 4096
		db.mu.Unlock()
		second := <-observed.calls
		cancel()
		<-done
		if got := second.Sub(first); got != time.Second {
			t.Fatalf("unready database retry took %s, want 1s", got)
		}
	})
}

func compactionScheduleDB(client ReplicaClient) (*DB, *compactionScheduleObserver) {
	db := NewDB("schedule.db")
	// This fixture only exercises metadata admission; it has no SQLite file,
	// background database monitor, or storage payload to close.
	db.opened, db.pageSize = true, 4096
	db.Replica = NewReplicaWithClient(db, client)
	return db, &compactionScheduleObserver{calls: make(chan time.Time)}
}

type compactionScheduleObserver struct{ calls chan time.Time }

func (*compactionScheduleObserver) Enabled(context.Context, slog.Level) bool { return true }
func (h *compactionScheduleObserver) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "no compaction" || r.Message == "db not ready, skipping" {
		h.calls <- r.Time
	}
	return nil
}
func (h *compactionScheduleObserver) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *compactionScheduleObserver) WithGroup(string) slog.Handler      { return h }
