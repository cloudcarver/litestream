//go:build vfs

package litestream

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestPollBackoffBounds(t *testing.T) {
	base, ceiling := time.Second, 30*time.Second
	current := base
	for _, want := range []time.Duration{2, 4, 8, 16, 30, 30} {
		current = nextPollInterval(current, base, ceiling, false)
		if current != want*time.Second {
			t.Fatalf("interval=%v want=%v", current, want*time.Second)
		}
	}
	if nextPollInterval(current, base, ceiling, true) != base {
		t.Fatal("activity must reset backoff")
	}
	if nextPollInterval(base, base, base, false) != base {
		t.Fatal("fixed polling changed")
	}
	if nextPollInterval(time.Duration(1<<62), base, time.Duration(1<<63-1), false) != time.Duration(1<<63-1) {
		t.Fatal("duration overflow")
	}
}

func TestPollBackoffWakesAndStops(t *testing.T) {
	c := newCountingReplicaClient()
	f := NewVFSFile(c, "idle", slog.Default())
	f.PollInterval, f.MaxPollInterval = time.Millisecond, time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.monitorReplicaClient(ctx) }()
	defer func() { cancel(); <-done; f.cancel() }()
	deadline := time.Now().Add(time.Second)
	for c.calls.Load() < 12 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	before := c.calls.Load()
	if before < 12 {
		t.Fatal("monitor did not poll")
	}
	f.pollWake <- struct{}{}
	deadline = time.Now().Add(50 * time.Millisecond)
	for c.calls.Load() <= before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.calls.Load() <= before {
		t.Fatal("local activity did not wake backed-off monitor")
	}
}
