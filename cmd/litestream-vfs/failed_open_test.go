//go:build vfs

package main_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestVFS_FailedOpenKeepsExistingConnection(t *testing.T) {
	// sqlite3vfs allocates process-global file IDs from zero. A fresh process
	// makes the first live file deterministic even when the whole suite runs.
	const child = "LITESTREAM_FAILED_OPEN_TEST_CHILD"
	if os.Getenv(child) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestVFS_FailedOpenKeepsExistingConnection$", "-test.v")
		cmd.Env = append(os.Environ(), child+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("failed-open isolation: %v\n%s", err, output)
		}
		return
	}

	base := file.NewReplicaClient(t.TempDir())
	db, primary := openReplicatedPrimary(t, base, time.Hour, time.Hour)
	if _, err := primary.Exec("CREATE TABLE existing(value INTEGER); INSERT INTO existing VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	forceReplicaSync(t, db)
	client := &failedOpenClient{ReplicaClient: base}
	vfs := newVFS(t, client)
	vfs.PollInterval = time.Hour
	replica := openVFSReplicaDB(t, registerTestVFS(t, vfs))
	defer func() {
		if err := replica.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx := context.Background()
	conn, err := replica.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	var value int
	if err := conn.QueryRowContext(ctx, "SELECT value FROM existing").Scan(&value); err != nil || value != 42 {
		t.Fatal("initial connection", value, err)
	}
	client.fail.Store(true)
	other, err := replica.Conn(ctx) // The first connection is held, forcing xOpen.
	if err == nil {
		if closeErr := other.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		t.Fatal("injected second open unexpectedly succeeded")
	}
	client.fail.Store(false)
	if err := conn.QueryRowContext(ctx, "SELECT value FROM existing").Scan(&value); err != nil || value != 42 {
		t.Fatalf("failed second open damaged existing connection: value=%d error=%v", value, err)
	}
}

type failedOpenClient struct {
	litestream.ReplicaClient
	fail atomic.Bool
}

func (c *failedOpenClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if c.fail.Load() {
		return nil, errors.New("injected open failure")
	}
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}
