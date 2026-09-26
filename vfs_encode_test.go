//go:build vfs

package litestream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

type encodingPathClient struct {
	ReplicaClient
	seekable bool
	called   bool
}

func (c *encodingPathClient) WriteLTXFile(ctx context.Context, level int, min, max ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	c.called = true
	_, c.seekable = r.(io.ReadSeeker)
	return c.ReplicaClient.WriteLTXFile(ctx, level, min, max, r)
}

func TestVFSFile_LTXEncodingPaths(t *testing.T) {
	for _, pages := range []int{1, vfsSmallLTXPageBytes / 4096, vfsSmallLTXPageBytes/4096 + 1, 256} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			client := newWriteTestReplicaClient()
			createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
			f := openSyncIndexTestFile(t, client)
			tracked := &encodingPathClient{ReplicaClient: client}
			f.client = tracked
			rng := rand.New(rand.NewSource(42))
			want := make([][]byte, pages)
			for i := range want {
				want[i] = make([]byte, 4096)
				_, _ = rng.Read(want[i]) // Incompressible pages exercise buffer boundaries.
				if _, err := f.WriteAt(want[i], int64(i+1)*4096); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.Sync(0); err != nil {
				t.Fatal(err)
			}
			if !tracked.called || tracked.seekable != (pages*4096 <= vfsSmallLTXPageBytes) {
				t.Fatalf("wrong upload path: called=%t seekable=%t", tracked.called, tracked.seekable)
			}
			if f.ltxBuffer.Cap() > vfsLTXBufferLimit {
				t.Fatalf("oversized scratch retained: %d", f.ltxBuffer.Cap())
			}
			f.cache.Purge()
			for i, data := range want {
				assertSyncIndexPage(t, f, uint32(i+2), data)
			}
			// Reuse scratch for a new upload, without damaging earlier objects.
			if _, err := f.WriteAt(bytes.Repeat([]byte{7}, 4096), 4096); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(0); err != nil {
				t.Fatal(err)
			}
			// TXID 2's index is different from TXID 3's; decode its own index.
			info := client.ltxFiles[0][1]
			index, err := FetchPageIndex(context.Background(), client, info)
			if err != nil {
				t.Fatal(err)
			}
			elem := index[2]
			_, page, err := FetchPage(context.Background(), client, 0, 2, 2, elem.Offset, elem.Size)
			if err != nil || !bytes.Equal(page, want[0]) {
				t.Fatalf("old object changed after buffer reuse: %v", err)
			}
		})
	}
}

func TestVFSFile_EncodeErrorPreservesDirtyState(t *testing.T) {
	for _, pages := range []int{1, 32} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			client := newWriteTestReplicaClient()
			createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
			f := openSyncIndexTestFile(t, client)
			writePages := func() {
				for pgno := 2; pgno < pages+2; pgno++ {
					if _, err := f.WriteAt(bytes.Repeat([]byte{byte(pgno)}, 4096), int64(pgno-1)*4096); err != nil {
						t.Fatal(err)
					}
				}
			}
			writePages()
			if err := f.bufferFile.Truncate(0); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(0); err == nil {
				t.Fatal("truncated write buffer reported successful sync")
			}
			if f.Pos().TXID != 1 || len(f.dirty) != pages || len(client.ltxFiles[0]) != 1 {
				t.Fatal("encoding failure published incomplete LTX")
			}
			// Restore the injected local I/O fault directly: WriteAt correctly
			// refuses to read/merge a dirty page from a truncated buffer.
			for pgno, offset := range f.dirty {
				if _, err := f.bufferFile.WriteAt(bytes.Repeat([]byte{byte(pgno)}, 4096), offset); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.Sync(0); err != nil {
				t.Fatal(err)
			}
			f.cache.Purge()
			assertSyncIndexPage(t, f, uint32(pages+1), bytes.Repeat([]byte{byte(pages + 1)}, 4096))
		})
	}
}

func TestVFSFile_StreamUploadFailureJoinsEncoder(t *testing.T) {
	client := newWriteTestReplicaClient()
	createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
	f := openSyncIndexTestFile(t, client)
	rng := rand.New(rand.NewSource(73))
	var last []byte
	for pgno := uint32(2); pgno < 130; pgno++ {
		last = make([]byte, 4096)
		_, _ = rng.Read(last)
		if _, err := f.WriteAt(last, int64(pgno-1)*4096); err != nil {
			t.Fatal(err)
		}
	}
	wantErr := errors.New("upload fails before reading")
	f.client = &failingSyncIndexClient{ReplicaClient: client, err: wantErr}
	done := make(chan error, 1)
	go func() { done <- f.Sync(0) }()
	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("sync error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("encoder blocked after upload stopped")
	}
	if len(f.dirty) != 128 || f.Pos().TXID != 1 {
		t.Fatal("failed stream upload published dirty state")
	}
	f.client = client
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	f.cache.Purge()
	assertSyncIndexPage(t, f, 129, last)
}

func TestVFSFile_EncodingSkipsLockPagePastOneGiB(t *testing.T) {
	for _, pages := range []int{1, 32} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			client := newWriteTestReplicaClient()
			lockPgno := ltx.LockPgno(4096)
			// A sparse >1 GiB logical database exercises the reserved-page boundary
			// without allocating/uploading a GiB for this unit test.
			createTestLTXFile(t, client, 1, 4096, 1, map[uint32][]byte{1: make([]byte, 4096)})
			f := openSyncIndexTestFile(t, client)
			if _, err := f.WriteAt(make([]byte, 4096), int64(lockPgno-1)*4096); err == nil {
				t.Fatal("WriteAt accepted the SQLite lock page")
			}
			for i := 1; i <= pages; i++ {
				pgno := lockPgno + uint32(i)
				if _, err := f.WriteAt(bytes.Repeat([]byte{byte(i + 1)}, 4096), int64(pgno-1)*4096); err != nil {
					t.Fatal(err)
				}
			}
			// Exercise the encoder's defensive skip even if the buffer contains
			// a lock-page entry. SQLite itself must never write this page.
			f.mu.Lock()
			err := f.writeToBuffer(lockPgno, make([]byte, 4096))
			f.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(0); err != nil {
				t.Fatal(err)
			}
			if _, exists := f.index[lockPgno]; exists {
				t.Fatal("lock page encoded into index")
			}
			if size, err := f.FileSize(); err != nil || size <= 1<<30 {
				t.Fatalf("not a >1 GiB database: size=%d err=%v", size, err)
			}
			f.cache.Purge()
			assertSyncIndexPage(t, f, lockPgno+uint32(pages), bytes.Repeat([]byte{byte(pages + 1)}, 4096))
		})
	}
}
