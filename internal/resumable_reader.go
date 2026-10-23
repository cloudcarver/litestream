package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/superfly/ltx"
)

type LTXFileOpener interface {
	OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}

// resumableReader wraps an io.ReadCloser from a remote storage backend with
// automatic reconnection on read errors.
//
// During restore, the LTX compactor opens all LTX file streams upfront, then
// processes pages in page-number order. Incremental LTX files that only contain
// high-numbered pages may have their S3/storage streams sit idle for minutes
// while the compactor works through lower-numbered pages from the snapshot.
// Storage providers (S3, Tigris, etc.) may close these idle connections,
// causing "unexpected EOF" errors.
//
// This reader detects two failure modes:
//  1. Non-EOF errors (connection reset, timeout) - the stream broke mid-transfer.
//  2. Premature EOF - the server closed the connection cleanly, but we haven't
//     read all bytes yet (detected by comparing offset against known file size).
//
// On failure, it closes the dead stream and reopens from the current byte
// offset using the storage backend's range request support (the offset parameter
// of OpenLTXFile). Callers like io.ReadFull see a seamless byte stream because
// partial reads are returned without error, prompting the caller to request
// remaining bytes on the next Read call.
type ResumableReader struct {
	ctx     context.Context
	client  LTXFileOpener
	level   int
	minTXID ltx.TXID
	maxTXID ltx.TXID
	size    int64 // expected total file size from FileInfo; 0 means unknown
	offset  int64
	retryN  int
	err     error
	logger  *slog.Logger
	// Only Read uses these. Reuse one watchdog per stream; a fired callback
	// must finish cancellation before the timer can be reset or discarded.
	readTimer *time.Timer
	timerDone chan struct{}

	// mu guards rc, cancel and closed. Close may be called from a different goroutine
	// than Read: Compactor.Compact closes its source readers when it returns
	// while its compaction goroutine may still be reading them. Once closed is
	// set, Read must not reopen the stream or it would leak the new connection.
	mu     sync.Mutex
	rc     io.ReadCloser
	cancel context.CancelFunc
	closed bool
	done   chan struct{}
}

// OpenResumableReader opens the initial stream with a reader-owned request
// context, so stall recovery and Close can cancel it just like a resumed stream.
// The open is eager: callers can reject unavailable inputs before uploading.
func OpenResumableReader(ctx context.Context, client LTXFileOpener, level int, minTXID, maxTXID ltx.TXID, size int64, logger *slog.Logger) (*ResumableReader, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	rc, err := client.OpenLTXFile(streamCtx, level, minTXID, maxTXID, 0, 0)
	if err != nil {
		cancel()
		if rc != nil {
			err = errors.Join(err, rc.Close())
		}
		return nil, err
	}
	r := NewResumableReader(ctx, client, level, minTXID, maxTXID, size, rc, logger)
	r.cancel = cancel
	return r, nil
}

// NewResumableReader creates a ResumableReader. Primarily exposed for testing.
// A non-nil rc has no reader-owned context and cannot be interrupted by the
// stall watchdog. Use OpenResumableReader for eager, cancellable initial opens.
func NewResumableReader(ctx context.Context, client LTXFileOpener, level int, minTXID, maxTXID ltx.TXID, size int64, rc io.ReadCloser, logger *slog.Logger) *ResumableReader {
	return &ResumableReader{
		ctx:     ctx,
		client:  client,
		level:   level,
		minTXID: minTXID,
		maxTXID: maxTXID,
		size:    size,
		rc:      rc,
		logger:  logger,
		done:    make(chan struct{}),
	}
}

const resumableReaderMaxRetries = 3

// resumableReaderBackoff is the base delay between retry attempts, doubling
// per attempt. Zero-delay retries land every attempt inside the same provider
// throttle window (e.g. Tigris 408 load shedding), guaranteeing exhaustion.
const resumableReaderBackoff = 250 * time.Millisecond

// resumableReaderStallTimeout bounds a single Read of an open stream. Nothing
// else does: the S3 client sets no ResponseHeaderTimeout and no read deadline on
// the response body, and TCP keepalive does not fire against a peer that is
// alive but simply sending nothing. It is a var rather than a const only so
// tests can shorten it.
var resumableReaderStallTimeout = 30 * time.Second

// ErrReadStalled reports a read that produced nothing for
// resumableReaderStallTimeout and had its stream cancelled to force a reconnect.
var ErrReadStalled = errors.New("ltx stream read stalled")

func (r *ResumableReader) Read(p []byte) (int, error) {
	for {
		rc, cancel, err := r.stream()
		if err != nil {
			return 0, err
		}
		if r.err != nil {
			return 0, r.err
		}

		// Reopen the stream from the current offset if the previous
		// connection was closed (the stream is nil after a retry).
		if rc == nil {
			streamCtx, streamCancel := context.WithCancel(r.ctx)
			// Publish cancellation before opening: Close must also interrupt a
			// request waiting for response headers, not just an installed body.
			if !r.setStream(nil, streamCancel) {
				streamCancel()
				return 0, os.ErrClosed
			}
			newRC, err := r.client.OpenLTXFile(streamCtx, r.level, r.minTXID, r.maxTXID, r.offset, 0)
			if err != nil {
				streamCancel()
				if newRC != nil {
					closeErr := newRC.Close()
					if closeErr != nil {
						// A later retry may succeed and consume err. Preserve
						// evidence of a failed cleanup even in that case.
						r.logger.Warn("close failed ltx open response",
							"level", r.level, "min", r.minTXID, "max", r.maxTXID,
							"offset", r.offset, "error", closeErr)
					}
					err = errors.Join(err, closeErr)
				}
				r.dropStream()
				if _, _, closedErr := r.stream(); closedErr != nil {
					return 0, errors.Join(closedErr, err)
				}
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return 0, fmt.Errorf("reopen ltx file at offset %d: %w", r.offset, err)
				}
				if ctxErr := r.ctx.Err(); ctxErr != nil {
					r.err = fmt.Errorf("reopen ltx file at offset %d: %v: %w", r.offset, err, ctxErr)
					return 0, r.err
				}
				if retryErr := r.retry(fmt.Errorf("reopen ltx file at offset %d: %w", r.offset, err)); retryErr != nil {
					return 0, retryErr
				}
				r.logger.Debug("reopen ltx file failed, retrying",
					"level", r.level, "min", r.minTXID, "max", r.maxTXID,
					"offset", r.offset, "error", err, "attempt", r.retryN)
				continue
			}
			if !r.setStream(newRC, streamCancel) {
				// Closed while the stream was being reopened.
				streamCancel()
				return 0, errors.Join(os.ErrClosed, newRC.Close())
			}
			rc, cancel = newRC, streamCancel
		}

		n, err := r.readStream(rc, cancel, p)
		r.offset += int64(n)

		if err == nil {
			if n > 0 && n == len(p) {
				r.retryN = 0
			}
			return n, nil
		}

		if err == io.EOF {
			// Distinguish legitimate EOF (fully read) from premature EOF
			// (server closed idle connection). When the file size is known
			// and we haven't read it all, treat as a connection drop.
			if r.size > 0 && r.offset < r.size {
				r.logger.Debug("premature EOF on ltx file, reconnecting",
					"level", r.level, "min", r.minTXID, "max", r.maxTXID,
					"offset", r.offset, "size", r.size, "attempt", r.retryN+1)
				r.dropStream()
				if retryErr := r.retry(io.ErrUnexpectedEOF); retryErr != nil {
					return n, retryErr
				}
				if n > 0 {
					// Return the bytes we did get. The caller (e.g. io.ReadFull)
					// will call Read again, which will trigger the reopen above.
					return n, nil
				}
				continue
			}
			return n, io.EOF
		}

		// Non-EOF error (connection reset, timeout, etc.). Close the dead
		// stream so the next iteration reopens from the current offset.
		r.logger.Debug("read error on ltx file, reconnecting",
			"level", r.level, "min", r.minTXID, "max", r.maxTXID,
			"error", err, "offset", r.offset, "attempt", r.retryN+1)
		r.dropStream()
		if retryErr := r.retry(err); retryErr != nil {
			return n, retryErr
		}
		if n > 0 {
			return n, nil
		}
	}
}

// readStream reads from the current stream under a stall timeout. A read that
// produces nothing before the timeout expires has its stream's request context
// cancelled, which unblocks the read with an error and sends the caller down the
// reconnect path that already handles a dropped connection.
//
// Cancellation rather than Close: net/http's (*body).Read holds the body mutex
// for the duration of the network read and (*body).Close wants that same mutex,
// so a watchdog calling Close would queue behind the very read it is trying to
// interrupt. Only the request context reaches a parked read.
func (r *ResumableReader) readStream(rc io.ReadCloser, cancel context.CancelFunc, p []byte) (int, error) {
	timeout := resumableReaderStallTimeout
	if cancel == nil || timeout <= 0 {
		return rc.Read(p)
	}

	if r.readTimer == nil {
		done := make(chan struct{})
		r.timerDone = done
		// Capture the request's cancel, never the mutable r.cancel. Close can
		// run concurrently; a retired stream must not cancel its successor.
		r.readTimer = time.AfterFunc(timeout, func() {
			cancel()
			done <- struct{}{}
		})
	} else {
		r.readTimer.Reset(timeout)
	}

	n, err := rc.Read(p)
	if !r.readTimer.Stop() {
		// AfterFunc.Stop does not join a running callback. In particular, a
		// successful Read racing the deadline must wait before reusing the
		// timer; otherwise the old callback could cancel the next read.
		<-r.timerDone
		r.logger.Debug("ltx file read stalled, cancelling stream",
			"level", r.level, "min", r.minTXID, "max", r.maxTXID,
			"offset", r.offset, "timeout", timeout)
		// %v, not %w: the underlying error is context.Canceled, and a stall is
		// retryable where a cancelled parent context is terminal. Letting
		// context.Canceled into the chain invites an errors.Is check above to
		// abandon a restore that only needed to reconnect.
		// Even a successful Read may race cancellation. Discard that stream
		// now, preserving its bytes, instead of exposing context.Canceled on
		// the next Read and mistaking it for a cancelled parent.
		err = fmt.Errorf("%w after %s: %v", ErrReadStalled, timeout, err)
	}
	return n, err
}

// Close closes the current underlying stream, if any, and marks the reader as
// closed. Closing is permanent: subsequent reads fail with os.ErrClosed rather
// than reopening the remote stream, which would leak the new connection.
func (r *ResumableReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.done)
	rc, cancel := r.rc, r.cancel
	r.rc, r.cancel = nil, nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rc != nil {
		return rc.Close()
	}
	return nil
}

// stream returns the current underlying stream, or nil if the next read should
// reopen it. Returns os.ErrClosed if the reader has been closed.
func (r *ResumableReader) stream() (io.ReadCloser, context.CancelFunc, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, os.ErrClosed
	}
	return r.rc, r.cancel, nil
}

// setStream installs a newly reopened stream. It reports false if the reader
// was closed while the stream was being opened, in which case the caller must
// close the new stream itself.
func (r *ResumableReader) setStream(rc io.ReadCloser, cancel context.CancelFunc) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.rc, r.cancel = rc, cancel
	return true
}

// dropStream closes and clears the current stream after a read failure so the
// next read reopens from the current offset. The stream is already being
// discarded, so a close error should not stop recovery. Log it only to aid
// debugging.
func (r *ResumableReader) dropStream() {
	// readStream has stopped/joined the callback before returning.
	r.readTimer, r.timerDone = nil, nil
	r.mu.Lock()
	rc, cancel := r.rc, r.cancel
	r.rc, r.cancel = nil, nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rc == nil {
		return
	}
	if err := rc.Close(); err != nil {
		r.logger.Warn("close ltx file",
			"level", r.level, "min", r.minTXID, "max", r.maxTXID,
			"offset", r.offset, "error", err)
	}
}

func (r *ResumableReader) retry(err error) error {
	select {
	case <-r.done:
		return os.ErrClosed
	default:
	}
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		r.err = ctxErr
		return r.err
	}
	r.retryN++
	if r.retryN > resumableReaderMaxRetries {
		r.err = fmt.Errorf("max retries exceeded reading ltx file (level=%d, min=%s, max=%s, offset=%d): %w",
			r.level, r.minTXID, r.maxTXID, r.offset, err)
		return r.err
	}

	// Wait before the caller reopens. Retrying with no delay lands every
	// attempt inside the same provider throttle window, so the attempts are
	// spent without the provider ever getting a chance to recover.
	select {
	case <-r.done:
		return os.ErrClosed
	case <-r.ctx.Done():
		r.err = r.ctx.Err()
		return r.err
	case <-time.After(resumableReaderBackoff << (r.retryN - 1)):
	}
	return nil
}
