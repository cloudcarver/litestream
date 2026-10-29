package file_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/file"
)

func TestReplicaClient_WriteLTXFile_Cancellation(t *testing.T) {
	for _, when := range []string{"before-header", "during-copy", "replace-existing"} {
		t.Run(when, func(t *testing.T) {
			client := file.NewReplicaClient(t.TempDir())
			original := createLTXData(1, 1, []byte("original"))
			if when == "replace-existing" {
				if _, err := client.WriteLTXFile(context.Background(), 0, 1, 1, bytes.NewReader(original)); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			data := createLTXData(1, 1, bytes.Repeat([]byte("new data"), 16384))
			reader := &cancelingReader{reader: bytes.NewReader(data), cancel: cancel}
			if when == "before-header" {
				cancel()
			}
			if _, err := client.WriteLTXFile(ctx, 0, 1, 1, reader); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled write returned %v", err)
			}
			if files := findTmpFiles(t, client.Path()); len(files) != 0 {
				t.Fatalf("canceled write left temporary files: %v", files)
			}
			got, err := os.ReadFile(client.LTXFilePath(0, 1, 1))
			if when == "replace-existing" {
				if err != nil || !bytes.Equal(got, original) {
					t.Fatalf("canceled write changed existing file: %v", err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("canceled write published a file: %v", err)
			}
		})
	}
}

type cancelingReader struct {
	reader io.Reader
	cancel context.CancelFunc
	read   int
}

func (r *cancelingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p[:min(len(p), 64)])
	r.read += n
	if r.read > ltx.HeaderSize+64 {
		r.cancel()
	}
	return n, err
}
