package internal

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func TestResumableReader_WatchdogReusesTimer(t *testing.T) {
	data := make([]byte, 2000)
	client := &testLTXFileOpener{OpenLTXFileFunc: func(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}}
	r, err := OpenResumableReader(context.Background(), client, 0, 1, 1, int64(len(data)), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	}()
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if n, err := r.Read(buf); n != 1 || err != nil {
			t.Fatalf("read: %d %v", n, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("healthy reads allocate %g times per call", allocs)
	}
}

func TestResumableReader_SuccessRacingTimeoutResumes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var opens int
		client := &testLTXFileOpener{OpenLTXFileFunc: func(ctx context.Context, _ int, _, _ ltx.TXID, offset, _ int64) (io.ReadCloser, error) {
			opens++
			if opens == 1 {
				returned := false
				return io.NopCloser(readerFunc(func(p []byte) (int, error) {
					// The body returns bytes concurrently with the watchdog's
					// cancellation. They must be retained exactly once.
					<-ctx.Done()
					if returned {
						return 0, ctx.Err()
					}
					returned = true
					p[0] = 'a'
					return 1, nil
				})), nil
			}
			if offset != 1 {
				t.Fatalf("resume offset: %d", offset)
			}
			return io.NopCloser(bytes.NewBufferString("bc")), nil
		}}
		r, err := OpenResumableReader(context.Background(), client, 0, 1, 1, 3, slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()
		data, err := io.ReadAll(r)
		if err != nil || string(data) != "abc" || opens != 2 {
			t.Fatalf("data=%q opens=%d error=%v", data, opens, err)
		}
	})
}

func TestResumableReader_IdleTimeDoesNotCancelStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var request context.Context
		client := &testLTXFileOpener{OpenLTXFileFunc: func(ctx context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
			request = ctx
			return io.NopCloser(&slowProgressReader{ctx: ctx}), nil
		}}
		r, err := OpenResumableReader(context.Background(), client, 0, 1, 1, 2, slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()
		for range 2 {
			time.Sleep(time.Minute)
			if request.Err() != nil {
				t.Fatal("watchdog cancelled an idle stream", request.Err())
			}
			if n, err := r.Read(make([]byte, 1)); n != 1 || err != nil {
				t.Fatalf("read after idle: %d %v", n, err)
			}
		}
	})
}
