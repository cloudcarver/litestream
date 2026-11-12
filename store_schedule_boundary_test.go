package litestream

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

type compactionPassHandler struct {
	slog.Handler
	work     time.Duration
	attempts chan time.Time
	first    bool
}

func (h *compactionPassHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "no compaction" {
		h.attempts <- time.Now()
		if h.first {
			h.first = false
			// Virtual time models work done by the monitor's first pass.
			time.Sleep(h.work)
		}
	}
	return h.Handler.Handle(ctx, r)
}

func TestStoreCompactionMonitorSchedulesAfterPass(t *testing.T) {
	for _, tc := range []struct {
		name string
		work time.Duration
		next time.Duration
	}{
		{"before_boundary", 100 * time.Millisecond, 0},
		{"cross_boundary", 300 * time.Millisecond, time.Minute},
		{"cross_multiple_boundaries", 2*time.Minute + 300*time.Millisecond, 3 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				boundary := time.Now().Truncate(time.Minute).Add(time.Minute)
				time.Sleep(time.Until(boundary.Add(-200 * time.Millisecond)))
				attempts := make(chan time.Time)
				logger := slog.New(&compactionPassHandler{
					Handler:  slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}),
					work:     tc.work,
					attempts: attempts,
					first:    true,
				})
				// A ready metadata-only database with no new files exercises the
				// actual monitor and CompactDB without starting unrelated workers.
				db := &DB{opened: true, pageSize: 4096, Logger: logger}
				db.maxLTXFileInfos.m = map[int]*ltx.FileInfo{0: {}, 1: {}}
				level := &CompactionLevel{Level: 1, Interval: time.Minute}
				store := &Store{dbs: []*DB{db}, levels: CompactionLevels{{Level: 0}, level}, Logger: logger}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				go func() {
					defer close(done)
					store.monitorCompactionLevel(ctx, level)
				}()
				<-attempts
				got := <-attempts
				cancel()
				<-done
				if want := boundary.Add(tc.next); !got.Equal(want) {
					t.Fatalf("next pass at %s, want %s after %s of work", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano), tc.work)
				}
			})
		})
	}
}
