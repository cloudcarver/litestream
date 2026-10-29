# Replication lifecycle regressions

The fork keeps these ownership contracts explicit:

- An unsuccessful VFS `xOpen` owns no file and must not install `xClose`. A failed
  second open must leave the first connection usable. The regression uses a
  fresh subprocess because file IDs are process-global.
- Canceled LTX copies stop between reads and before Sync/Rename. They remove
  their temporary file and preserve an existing destination. Cleanup errors
  remain observable. Arbitrarily blocking readers and kernel calls require
  their own cancellation/bounds; context checks cannot interrupt them.
- Closing a resumable reader interrupts reconnect and backoff, prevents future
  opens, and closes failed-open bodies without canceling the caller's context.
- Compactor/DB shutdown joins owned work, including writers that return before
  consuming the pipe; stopping a test process is itself an assertion.

Deterministic regressions (ordinary `go.mod`, no experimental replacement):

```sh
go test -race ./file -run '^TestReplicaClient_WriteLTXFile_Cancellation$' -count=5
go test -race ./internal -run 'Test.*ResumableReader' -count=1
go test -race -tags vfs ./cmd/litestream-vfs -run '^TestVFS_FailedOpenKeepsExistingConnection$' -count=5
```

The [sqlite3vfs snapshot](../third_party/sqlite3vfs/README.sqless.md) contains the
minimal failed-open fix and its upstream provenance. A consuming module must
repeat its replacement because Go does not inherit dependency replacements.

Core replication changes also require the existing `TestLTXBehavior` short gate
and applicable >1GiB restoration checks. Concurrent load tests compare the table
the load generator actually writes and require a nonempty result; waiting for a
first published LTX does not replace the existing process-stop deadline.

Zero-timeout shutdown is checked with virtual time: exactly one attempt and no
retry delay, independent of SQLite cleanup or host scheduling latency. Store
restore tests establish WAL/schema and explicitly publish an initial nonempty
backup before starting concurrent restore/load. Subsequent restored data must
advance through automatic replication; a bootstrap-only snapshot cannot pass.
The first-publication delay experiment must fail the old fixture and pass this
one without retrying restore errors or relaxing the active workload.

SQLess's `dev/verify` workflow runs named regressions plus upstream/behavior
negative controls and preserves JSON evidence. A missing test, mandatory skip,
compiler failure or incomplete shutdown cannot be counted as verified behavior.
Default package tests and VFS root/CLI tests remain separate because not every
test binary links SQLite C symbols. Performance assertions retain their original
SQL and threshold; record toolchain, build mode and available CPU before
comparing results across runs.

SQLite CLI hydration tests build from the current checkout once per process and
load the platform-named library with the explicit `sqlite3_litestreamvfs_init`
entrypoint. A preexisting generic `dist/litestream-vfs.so` is not reused: it can
hide a stale binary or a platform-filename entrypoint mismatch. Loading errors
fail the test, rather than becoming platform skips.

Store shutdown cancels and joins background snapshot/compaction before closing
DB handles. The final WAL/replica flush uses the caller context, so canceling
background work cannot lose a pending committed row. The regression pauses an
actual snapshot storage operation, checks that its DB is still open when it is
canceled, then restores a row committed just before Close.

WAL continuity uses the compact LTX index with constant index memory and one
page decode. It preserves the original WAL salt and page-content comparison.
This lookup is not a full-file checksum audit; the prior early-return scan did
not finish/verify the LTX trailer either. Full verification and restore keep
their existing integrity checks. A byte-count regression prevents a return to
reading unrelated multi-GiB page bodies; malformed index ranges, cancellation,
missing/mismatching pages and legacy frames remain covered.

The checkpoint gap regression now holds a real SQLite write transaction until
`checkpoint_exec` proves the pre-checkpoint sync has completed, then commits 100
new rows. It requires all 200 rows after full-integrity restore. Removing the
TRUNCATE boundary snapshot loses those commits and fails the exact assertion.
This replaces the old indefinite writer/5s scheduling race without increasing
its timeout or retrying an unsuccessful checkpoint.

Normal large-database integration checks reject every child ERROR/panic, not
only errors mentioning the historical lock page. Cleanup reports stop failures
and includes failed child logs in the durable Go test output. The 35s stop
limit is unchanged, and explicit Stop followed by Cleanup waits only once.

VFS concurrency correctness and throughput have separate gates. The race case
completes 128 writes with eight active readers within a 30s failure deadline;
it then requires the replica's final TXID and every ID/value/timestamp to match
the primary. The normal-build performance case (`vfs && !race`) keeps the
original random-sort SQL, 2,000-row seed, eight readers, 5s workload and at least
100 writes, followed by the same data checks. Early termination and insufficient
normal-build throughput must fail their named negative controls. Historical
race-instrumented throughput failures are retained in the SQLess verification
report; they are not relabeled as passed tests.
