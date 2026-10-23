package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

func TestResumableReader_CloseInterruptsPendingWork(t *testing.T) {
	for _, opening := range []bool{true, false} {
		name := "backoff"
		if opening {
			name = "open"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				started := make(chan struct{})
				client := &testLTXFileOpener{
					OpenLTXFileFunc: func(requestCtx context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
						close(started)
						if opening {
							<-requestCtx.Done()
							return nil, requestCtx.Err()
						}
						return nil, io.ErrUnexpectedEOF
					},
				}
				r := NewResumableReader(ctx, client, 0, 1, 1, 1, nil, slog.Default())
				result := make(chan error, 1)
				go func() {
					_, err := r.Read(make([]byte, 1))
					result <- err
				}()
				<-started
				synctest.Wait()
				if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				select {
				case err := <-result:
					if !errors.Is(err, os.ErrClosed) {
						t.Fatalf("read after Close: %v", err)
					}
				default:
					t.Fatal("Close did not interrupt pending work")
				}
				if ctx.Err() != nil {
					t.Fatal("Close cancelled the caller")
				}
			})
		})
	}
}

func TestResumableReader_SlowProgressDoesNotTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		var opens int
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(ctx context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
				opens++
				return io.NopCloser(&slowProgressReader{ctx: ctx}), nil
			},
		}
		r, err := OpenResumableReader(ctx, client, 0, 1, 1, 4, slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()
		start := time.Now()
		for range 4 {
			buf := make([]byte, 1)
			if n, err := r.Read(buf); n != 1 || err != nil || buf[0] != 'x' {
				t.Fatalf("slow read: n=%d data=%q err=%v", n, buf, err)
			}
		}
		if elapsed := time.Since(start); elapsed != 80*time.Second || opens != 1 {
			t.Fatalf("elapsed=%s opens=%d; want one healthy stream for 80s", elapsed, opens)
		}
	})
}

type slowProgressReader struct{ ctx context.Context }

func (r *slowProgressReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-time.After(20 * time.Second):
		p[0] = 'x'
		return 1, nil
	}
}

func TestResumableReader_ParentDeadlineAtStall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var opens int
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(ctx context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
				opens++
				return io.NopCloser(newSilentAfterN(ctx, nil, 0)), nil
			},
		}
		r, err := OpenResumableReader(ctx, client, 0, 1, 1, 1, slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()
		if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline must remain terminal: %v", err)
		}
		if opens != 1 {
			t.Fatalf("reopened after deadline: %d opens", opens)
		}
	})
}

func TestResumableReader_RenewsBudgetAfterStallRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const chunk = 4
		data := []byte("0123456789abcdefghijklmn")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var contexts []context.Context
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				if offset != int64(len(contexts)*chunk) {
					t.Errorf("open offset=%d, want %d", offset, len(contexts)*chunk)
				}
				contexts = append(contexts, ctx)
				if len(data)-int(offset) <= chunk {
					return io.NopCloser(bytes.NewReader(data[offset:])), nil
				}
				return io.NopCloser(newSilentAfterN(ctx, data[offset:], chunk)), nil
			},
		}
		r, err := OpenResumableReader(ctx, client, 0, 1, 1, int64(len(data)), slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}()

		// Complete successful reads renew the budget across more than three
		// stalls. Trickle-only failures still have the upstream retry cap.
		got := make([]byte, len(data))
		for offset := 0; offset < len(got); offset += chunk {
			if _, err := io.ReadFull(r, got[offset:offset+chunk]); err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("resumed data=%q, want %q", got, data)
		}
		if _, err := r.Read(make([]byte, chunk)); err != io.EOF {
			t.Fatalf("final read=%v, want EOF", err)
		}
		if len(contexts) != len(data)/chunk {
			t.Fatalf("opens=%d, want %d", len(contexts), len(data)/chunk)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		for i, ctx := range contexts {
			if ctx.Err() != context.Canceled {
				t.Errorf("stream %d context remains live", i)
			}
		}
	})
}

func TestOpenResumableReader_FailedOpenCancelsAndClosesBody(t *testing.T) {
	openErr, closeErr := errors.New("open failed"), errors.New("close failed")
	var streamCtx context.Context
	body := &failedOpenBody{closeErr: closeErr}
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			streamCtx = ctx
			body.ctx = ctx
			return body, openErr
		},
	}
	r, err := OpenResumableReader(context.Background(), client, 0, 1, 1, 10, slog.Default())
	if r != nil || !errors.Is(err, openErr) || !errors.Is(err, closeErr) {
		t.Fatalf("reader=%v, error=%v; want both open and close errors", r, err)
	}
	if !body.closed || streamCtx.Err() != context.Canceled {
		t.Fatal("failed request was not cancelled and closed")
	}
}

func TestResumableReader_FailedReopenClosesReturnedBody(t *testing.T) {
	closeErr := errors.New("close failed")
	body := &failedOpenBody{closeErr: closeErr}
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(ctx context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
			body.ctx = ctx
			return body, context.DeadlineExceeded
		},
	}
	r := NewResumableReader(context.Background(), client, 0, 1, 1, 1, nil, slog.Default())
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, closeErr) {
		t.Fatalf("failed reopen lost error: %v", err)
	}
	if !body.closed || body.ctx.Err() != context.Canceled {
		t.Fatal("failed reopen leaked its body or request")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestResumableReader_LogsCleanupErrorWhenReopenRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		closeErr := errors.New("failed response body cleanup")
		var opens int
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(ctx context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
				opens++
				if opens == 1 {
					return &failedOpenBody{ctx: ctx, closeErr: closeErr}, io.ErrUnexpectedEOF
				}
				return io.NopCloser(bytes.NewReader([]byte("ok"))), nil
			},
		}
		r := NewResumableReader(context.Background(), client, 0, 1, 1, 2, nil, logger)
		data, err := io.ReadAll(r)
		if closeErr := r.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil || string(data) != "ok" || opens != 2 {
			t.Fatalf("recovery failed: data=%q opens=%d err=%v", data, opens, err)
		}
		if !bytes.Contains(logs.Bytes(), []byte("level=WARN")) || !bytes.Contains(logs.Bytes(), []byte(closeErr.Error())) {
			t.Fatalf("successful retry silently lost cleanup failure: %s", logs.Bytes())
		}
	})
}

func TestResumableReader_ParentCancellationAtRetryLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var opens int
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
				opens++
				return io.NopCloser(&cancelledReadAtLimit{cancel: cancel, last: opens == resumableReaderMaxRetries+1}), nil
			},
		}
		r := NewResumableReader(ctx, client, 0, 1, 1, 1, nil, slog.Default())
		if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
			t.Fatalf("retry exhaustion masked cancellation: %v", err)
		}
		if opens != resumableReaderMaxRetries+1 {
			t.Fatal("did not reach retry boundary", opens)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

type cancelledReadAtLimit struct {
	cancel context.CancelFunc
	last   bool
}

func (r *cancelledReadAtLimit) Read([]byte) (int, error) {
	if r.last {
		r.cancel()
	}
	return 0, io.ErrUnexpectedEOF
}

type failedOpenBody struct {
	ctx      context.Context
	closeErr error
	closed   bool
}

func (r *failedOpenBody) Read([]byte) (int, error) { return 0, io.EOF }

func (r *failedOpenBody) Close() error {
	r.closed = true
	if r.ctx.Err() != context.Canceled {
		return errors.New("body closed before request cancellation")
	}
	return r.closeErr
}
