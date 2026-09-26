//go:build vfs

package litestream

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"
)

type failIndexOpenClient struct {
	ReplicaClient
	err error
}

func (c *failIndexOpenClient) OpenLTXFile(ctx context.Context, level int, min, max ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if size == 0 { // Header probe succeeds; page-index fetch fails after buffer creation.
		return nil, c.err
	}
	return c.ReplicaClient.OpenLTXFile(ctx, level, min, max, offset, size)
}

func TestVFSFile_FailedOpenClosesWriteBuffer(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
	want := errors.New("injected page-index read failure")
	for i := 0; i < 20; i++ {
		f := setupWriteableVFSFile(t, client)
		f.client = &failIndexOpenClient{ReplicaClient: client, err: want}
		if err := f.Open(); !errors.Is(err, want) {
			t.Fatalf("open error=%v", err)
		}
		if f.bufferFile != nil {
			t.Fatal("failed open retained buffer descriptor")
		}
		if _, err := os.Stat(f.bufferPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed-open buffer remains: %v", err)
		}
		if !errors.Is(f.ctx.Err(), context.Canceled) {
			t.Fatal("failed-open context remains active")
		}
	}
}

func TestVFSFile_NewDatabaseRequiresWritableBuffer(t *testing.T) {
	f := setupWriteableVFSFile(t, newWriteTestReplicaClient())
	parentFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("block directory creation"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.bufferPath = filepath.Join(parentFile, "write-buffer")
	if err := f.Open(); err == nil {
		_ = f.Close()
		t.Fatal("new database opened without a usable write buffer")
	}
	if f.bufferFile != nil || !errors.Is(f.ctx.Err(), context.Canceled) {
		t.Fatal("failed new-database open left resources active")
	}
}
