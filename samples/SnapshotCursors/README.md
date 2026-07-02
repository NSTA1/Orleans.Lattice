# SnapshotCursors

## What it shows

A snapshot cursor gives **strict snapshot isolation**.
`OpenSnapshotEntryCursorAsync` freezes the tree state at open time; every page
the cursor returns reflects that captured instant, and no concurrent write -
foreground `SetAsync` / `DeleteAsync`, atomic saga, or range delete - is ever
visible to the cursor for the rest of its lifetime.

This sample opens a snapshot cursor, reads the first page, then **mutates the
tree mid-iteration** (adds a new key and overwrites an existing one) and keeps
paging. The snapshot cursor never observes either change, while a fresh live
read does - proving the isolation boundary.

## Run it

```
dotnet run --project samples/SnapshotCursors
```

## Expected output

The cursor ID is a server-assigned GUID, so it differs on every run.

```
Silo starting... ready.

Seeding 20 keys (k:00 .. k:19), each value = "v0"...

Opened snapshot entry cursor: 02b5b5b00f594271b0a40016d99770ac
  The tree state is now frozen for this cursor's lifetime.

Snapshot page 1: k:00 .. k:04

Mid-iteration writes (should be INVISIBLE to the snapshot cursor):
  + added new key   k:99 = "brand-new"
  ~ overwrote       k:10 = "MODIFIED"

What the snapshot cursor saw:
  total entries         = 20 (expected 20)
  contains new key k:99 = False (expected False)
  value of k:10         = "v0" (expected "v0")

What a fresh live read sees (for contrast):
  CountAsync()  = 21 (expected 21)
  value of k:10 = "MODIFIED" (expected "MODIFIED")

Snapshot isolation held: True

Done: the snapshot cursor never observed writes made after it was opened.
```

## When to use

- Long-running exports, audits, or reports that must reflect a single instant,
  even though the tree keeps changing while you page through it.
- Strict isolation against **every** concurrent write and saga, not just a
  stable saga-decision view.

## When not to use

- Pagination where the latest writes **should** appear on later pages: use a
  live cursor (`OpenEntryCursorAsync` / see [DurableCursors](../DurableCursors)).
- A single exact aggregate at one instant: `CountAsync` /
  [StronglyConsistentScans](../StronglyConsistentScans) is cheaper than opening
  and draining a snapshot cursor.

## Feature doc

- [Snapshot cursors](../../docs/lattice/snapshot-cursors.md)
