//go:build vfs && !race

package main_test

import (
	"testing"
	"time"
)

// Keep the production-build throughput budget independent of race/checkptr
// instrumentation. The mandatory performance gate still uses the original
// random-sort workload, eight readers, five seconds and 100-operation floor.
func TestVFS_HighLoadConcurrentReads(t *testing.T) {
	if ops := exerciseVFSConcurrentReads(t, 5*time.Second, 0); ops < 100 {
		t.Fatalf("expected high write volume, got %d ops", ops)
	}
}
