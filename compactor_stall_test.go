package litestream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestCompactor_InitialStreamStall(t *testing.T) {
	for _, tt := range []struct {
		name      string
		stallAt   int64
		cancel    bool
		failWrite bool
	}{
		{name: "header"},
		{name: "page", stallAt: ltx.HeaderSize + 16},
		{name: "caller_cancel", cancel: true},
		{name: "upload_failure", failWrite: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Use the real 30-second policy on a virtual clock. The parent
				// deadline turns an unprotected stream into a test failure.
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				base := file.NewReplicaClient(t.TempDir())
				createTestLTXFile(t, base, 0, 1, 1)
				client := &stalledCompactionClient{
					ReplicaClient: base,
					stallAt:       tt.stallAt,
					started:       make(chan struct{}),
				}
				if tt.failWrite {
					client.writeErr = errors.New("upload failed")
				}
				if tt.cancel {
					go func() {
						<-client.started
						cancel()
					}()
				}

				start := time.Now()
				info, err := litestream.NewCompactor(client, slog.Default()).Compact(ctx, 1)
				switch {
				case tt.cancel:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("Compact error=%v, want context.Canceled", err)
					}
				case tt.failWrite:
					if !errors.Is(err, client.writeErr) {
						t.Fatalf("Compact error=%v, want upload error", err)
					}
					if elapsed := time.Since(start); elapsed >= 30*time.Second {
						t.Fatalf("upload failure waited %v for stalled input", elapsed)
					}
				default:
					if err != nil {
						t.Fatal(err)
					}
					if elapsed := time.Since(start); elapsed < 30*time.Second || elapsed >= 31*time.Second {
						t.Fatalf("stall recovery took %v, want 30s plus retry backoff", elapsed)
					}
					if info.Level != 1 || info.MinTXID != 1 || info.MaxTXID != 1 {
						t.Fatalf("unexpected compacted file: %+v", info)
					}
					if len(client.offsets) != 2 || client.offsets[0] != 0 || client.offsets[1] != tt.stallAt {
						t.Fatalf("open offsets=%v, want [0 %d]", client.offsets, tt.stallAt)
					}
					rc, err := base.OpenLTXFile(ctx, 1, 1, 1, 0, 0)
					if err != nil {
						t.Fatal(err)
					}
					if err := ltx.NewDecoder(rc).Verify(); err != nil {
						t.Error(err)
					}
					if err := rc.Close(); err != nil {
						t.Error(err)
					}
				}
				// Wait for the detached merger to exit before inspecting
				// ownership. No replacement may be opened after Close.
				synctest.Wait()
				if !client.stalled.closed.Load() {
					t.Fatal("stalled source was not closed")
				}
				if (tt.cancel || tt.failWrite) && len(client.offsets) != 1 {
					t.Fatalf("source reopened after cancellation: %v", client.offsets)
				}
			})
		})
	}
}

type stalledCompactionClient struct {
	litestream.ReplicaClient
	stallAt  int64
	started  chan struct{}
	writeErr error
	offsets  []int64
	stalled  *stalledCompactionBody
}

func (c *stalledCompactionClient) OpenLTXFile(ctx context.Context, level int, min, max ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.offsets = append(c.offsets, offset)
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, min, max, offset, size)
	if err != nil || len(c.offsets) != 1 {
		return rc, err
	}
	c.stalled = &stalledCompactionBody{
		ReadCloser: rc, ctx: ctx, remaining: c.stallAt, started: c.started,
	}
	return c.stalled, nil
}

func (c *stalledCompactionClient) WriteLTXFile(ctx context.Context, level int, min, max ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if c.writeErr != nil {
		select {
		case <-c.started:
			return nil, c.writeErr
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for source read: %w", ctx.Err())
		}
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, min, max, r)
}

type stalledCompactionBody struct {
	io.ReadCloser
	ctx       context.Context
	remaining int64
	started   chan struct{}
	closed    atomic.Bool
}

func (r *stalledCompactionBody) Read(p []byte) (int, error) {
	if r.remaining > 0 {
		n, err := r.ReadCloser.Read(p[:min(int64(len(p)), r.remaining)])
		r.remaining -= int64(n)
		return n, err
	}
	close(r.started)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (r *stalledCompactionBody) Close() error {
	// Model a transport that needs request cancellation before Close can
	// finish; closing the response body alone cannot interrupt its read.
	<-r.ctx.Done()
	r.closed.Store(true)
	return r.ReadCloser.Close()
}
