# Durable Cursors

## What it shows

A durable cursor is a **server-checkpointed iterator**. `OpenEntryCursorAsync`
returns an opaque cursor ID whose paging position is persisted to Orleans
storage after every page. Any client that knows the ID can resume exactly where
the last one stopped - no re-scanning, no duplicates, no gaps - even across a
client restart, silo failover, or topology change.

This sample reads the first page as "Client A", then throws away every local
variable **except the opaque cursor ID string** (imagine it was written to a
database or queue) and reconnects as a brand new "Client B" that knows only that
token. Client B resumes paging from the persisted checkpoint and the combined
scan yields every key exactly once, in order.

## Run it

```
dotnet run --project samples/DurableCursors
```

## Expected output

The cursor ID is a server-assigned GUID, so it differs on every run.

```
Silo starting... ready.

Seeding 25 keys (row:00 .. row:24)...

Opened durable entry cursor: 88c0a2923c6c4e559d31810cc7d88633

[Client A] reading page 1...
  got 10 keys: row:00 .. row:09
  HasMore = True

[Client A] crashed / disconnected. The only thing that survives
           is the persisted cursor ID: 88c0a2923c6c4e559d31810cc7d88633
           The server-side checkpoint remembers the last yielded key.

[Client B] reconnecting with only the resume token...
  [Client B] page 2: 10 keys (row:10 .. row:19)
  [Client B] page 3: 5 keys (row:20 .. row:24)

Resumed scan results (page 1 from Client A, rest from Client B):
  total keys yielded  = 25
  no duplicates       = True
  every key exactly once and in order = True

Done: the cursor resumed from its persisted checkpoint after a client restart.
```

## When to use

- Long-running exports, ETL jobs, or migrations that may span minutes and must
  survive silo failovers or client restarts without re-scanning.
- Resumable range deletes (`OpenDeleteRangeCursorAsync`) that must track
  tombstoning progress across interruptions.
- Handing a scan off between processes or services: any client that holds the
  opaque cursor ID can resume it.

## When not to use

- Short, interactive scans where the client stays up: the stateless
  `ScanKeysAsync` / `ScanEntriesAsync` helpers are lower overhead and keep no
  server-side state.
- When later pages must be **isolated** from concurrent writes - use a snapshot
  cursor (see [SnapshotCursors](../SnapshotCursors)). A live durable cursor
  observes writes that land ahead of its current position.

## Feature doc

- [Durable Cursors](../../docs/lattice/durable-cursors.md)
