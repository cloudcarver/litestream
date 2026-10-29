# sqlite3vfs failed-open ownership patch

Snapshot: `github.com/psanford/sqlite3vfs v0.0.0-20260519004904-f9180fa2acc9`.
The full module archive and [license](LICENSE) are retained. [ORIGIN.json](ORIGIN.json)
records its module checksum; [upstream.sha256](upstream.sha256) records original
files; [local.patch](local.patch) contains the single C callback change.

`s3vfsOpen` sets `sqlite3_file.pMethods` only after a successful Go open. A failed
open did not acquire a registered file; SQLite must not call its close callback
using the zero ID and accidentally close the first healthy connection.

The regression uses a fresh subprocess so the first file ID is deterministic:

```sh
go test -race -tags vfs ./cmd/litestream-vfs -run '^TestVFS_FailedOpenKeepsExistingConnection$' -count=5
```

Run it from the Litestream root. SQLess also declares this replacement because
Go does not inherit replacement directives from dependency modules. Its
`make verify-negative` tests the unpatched upstream dependency in a temporary
modfile; `make verify-contracts` tests ordinary fixed builds. Upgrade/remove
the two replacements together after equivalent upstream fixes pass the gates.
