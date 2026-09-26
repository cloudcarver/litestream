package s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/superfly/ltx"
)

func TestLTXFilesSeekPushdown(t *testing.T) {
	for _, tc := range []struct {
		name        string
		level       int
		seek        ltx.TXID
		want, pages int
	}{
		{"all", 0, 0, 1006, 2},
		{"inclusive", 0, 1000, 7, 1},
		{"multiple-pages", 0, 2, 1005, 2},
		{"empty-tail", 0, 1006, 0, 1},
		{"max-txid", 0, ^ltx.TXID(0), 0, 1},
		{"higher-level-unchanged", 1, 1000, 7, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := fmt.Sprintf("replica/%04x/", tc.level)
			keys := make([]string, 0, 1006)
			for id := 1; id <= 1005; id++ {
				keys = append(keys, fmt.Sprintf("%s%016x-%016x.ltx", prefix, id, id))
			}
			// Equal MinTXID must include both a singleton and a ranged file.
			keys = append(keys, prefix+"00000000000003e8-00000000000003ea.ltx")
			slices.Sort(keys)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				q := r.URL.Query()
				wantStart := ""
				if tc.level == 0 && tc.seek > 0 {
					wantStart = prefix + fmt.Sprintf("%016x", uint64(tc.seek))
				}
				if q.Get("prefix") != prefix || q.Get("start-after") != wantStart {
					t.Errorf("unexpected LIST query: %s", r.URL.RawQuery)
				}
				selected := make([]string, 0, len(keys))
				for _, key := range keys {
					if key > q.Get("start-after") {
						selected = append(selected, key)
					}
				}
				start, _ := strconv.Atoi(q.Get("continuation-token"))
				end := min(start+1000, len(selected))
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprintf(w, "<ListBucketResult><IsTruncated>%t</IsTruncated>", end < len(selected))
				if end < len(selected) {
					fmt.Fprintf(w, "<NextContinuationToken>%d</NextContinuationToken>", end)
				}
				for _, key := range selected[start:end] {
					fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>1</Size></Contents>", key)
				}
				fmt.Fprint(w, "</ListBucketResult>")
			}))
			defer server.Close()
			c := newTestReplicaClient(t, server)
			it, err := c.LTXFiles(context.Background(), tc.level, tc.seek, false)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for it.Next() {
				if it.Item().MinTXID < tc.seek {
					t.Fatal("seek returned older TXID")
				}
				count++
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			if count != tc.want || calls.Load() != int32(tc.pages) {
				t.Fatalf("objects=%d requests=%d", count, calls.Load())
			}
		})
	}
}

func TestLTXFilesSeekPropagatesListError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")
	}))
	defer server.Close()
	c := newTestReplicaClient(t, server)
	it, err := c.LTXFiles(context.Background(), 0, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	if it.Next() {
		t.Fatal("unexpected file")
	}
	if err := it.Close(); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("lost LIST error: %v", err)
	}
}
