# Writable VFS: incremental sync index maintenance

Base: upstream `9fae1350700bca063bb31fbb5699266815947bc1` (2026-09-14).
This fork patch is for sqless's single-writer, single-connection runtime experiments.
It does not change the LTX format, object layout, or compactor API.

## Evidence

The original sync path uploaded dirty pages, advanced the TXID, and cleared the
write buffer without installing the new page offsets. An LRU cache was not a
correctness substitute for that index. A poll begun before sync could also
regress the position, and older L1 metadata could replace newer writer state.

The initial six regression cases in `vfs_sync_index_test.go` were run against the unchanged
base using a Go source overlay. All six failed: post-sync size, evicted page
contents, late-poll position, truncation, upload-failure/retry contents, and a
pending truncation's unchanged pages. They pass with this patch. Subsequent
tests cover size-only sync/cold reopen, remote read errors, nonsequential page
growth, and hydrated-reader fallback.

Related upstream proposals (not cherry-picked):

- https://github.com/benbjohnson/litestream/pull/1272
- https://github.com/benbjohnson/litestream/pull/1293
- https://github.com/benbjohnson/litestream/pull/1363

Unlike a retained page-byte overlay, this implementation publishes exact offsets
from the existing streaming LTX encoder. A test asserts zero object reads during
sync index maintenance. Successful uploads still use the existing conflict LIST
and object write; no whole-index refresh is required.

## Invariants and scope

- Install locally encoded page locations before clearing dirty data; stop and
  join the encoder on failed uploads before reusing the buffer.
- Keep file size with the visible snapshot, independent of sparse page indexes.
  This also removes the full-index scan from `FileSize()`.
- Reject polls overtaken by local sync; merge page versions by TXID. Older L1
  files cannot shrink the current writer or replace newer page data.
- Discover L1 files that straddle the polling watermark. L1 uses a full level
  listing because the provider seek contract filters by **MinTXID**. Pagination
  and long-lived listing costs remain an optimization opportunity.
- Stage reader index/size changes until unlock. Truncation removes only obsolete
  pages beyond EOF, including on cold rebuild, and persists size-only changes.
- Preserve actual remote read errors instead of uploading a zero-filled page.

No distributed fencing, source GC, durable local-buffer recovery, new commit
protocol, or multi-connection qualification is added. `Close()` still is not a
remote-durability guarantee; sqless explicitly waits for `SyncRemote()` before
acknowledging writes. Upload timeouts can still leave an ambiguous outcome.
Hydration is off in performance experiments. A hydrated reader with pending
updates falls back to indexed reads after unlock to avoid serving stale data;
this patch does not add automatic rehydration after that fallback.

## Tests

```sh
CGO_ENABLED=1 go test -race -tags vfs -run 'TestVFS|TestSetWriteEnabled' -count=10 -timeout 5m .
CGO_ENABLED=1 go test -race -tags vfs -timeout 5m . ./cmd/litestream-vfs
CGO_ENABLED=1 go vet -tags vfs .
```

The new package tests link the existing `go-sqlite3` dependency for SQLite's C
symbols. The upstream all-packages `-tags vfs ./...` command is not universally
linkable: several other package test executables do not link a C SQLite library
(`undefined reference to sqlite3_vfs_find`, etc.). This patch does not alter their
build setup. sqless's runtime/compactor/CLI race tests and RustFS integration tests
are run separately, including process-kill/remote-only recovery.

The local `pre-commit` executable was unavailable, and the installed Staticcheck
was built with Go 1.25 and cannot analyze this Go 1.26 module. Formatting,
`git diff --check`, and `go vet` are checked directly; these are not represented
as a successful pre-commit or Staticcheck run.
