# Strongly Consistent Scans

## What it shows

Lattice's scan primitives - `CountAsync`, `ScanKeysAsync`, and
`ScanEntriesAsync` - are **strongly consistent**: no key is missed or
double-counted because a shard split or rebalanced underneath the call, and a
concurrent `SetManyAtomicAsync` is observed all-or-nothing. Each shard is read at
its own moment, so a reading taken while ordinary writes land is exact shard by
shard rather than one instant's image of the whole tree. For an add-only stream of
writes like this sample's, that still makes every reading a count the tree
genuinely held at some instant, and successive readings never go backwards. This
sample seeds a baseline, hammers the tree with concurrent writes while repeatedly
counting, and confirms the settled state is exact and duplicate-free.

## Run it

```
dotnet run --project samples/StronglyConsistentScans
```

## Expected output

```
Silo starting... ready.

Seeding 500 keys (item:0000 .. item:0499)...
  CountAsync()      = 500
  ScanKeysAsync -> 500 keys
  Agree on baseline: True

Adding 300 keys (extra:0000 .. extra:0299) concurrently while counting...
  All observed counts stayed within [500, 800]: True
  Observed counts were monotonic (never went backwards): True

Settled state after concurrent writes:
  CountAsync()             = 800
  Distinct keys from scan  = 800
  Exact and duplicate-free: True

Done: scans returned the exact live key set throughout concurrent writes.
```

## When to use

- Reporting or dashboards where an aggregate count must be exact rather than
  eventually consistent or best-effort.
- Reconciliation and audit passes that must observe a consistent live key set
  while the tree keeps taking writes.
- Any read path where a key missed or double-counted because a shard split
  mid-read, or an atomic batch seen half-applied, would be a correctness bug.

## When not to use

- If you need a **stable, unchanging** view across a long multi-page scan while
  writes continue, use a snapshot cursor instead (see
  [SnapshotCursors](../SnapshotCursors)). Strong consistency guarantees that a
  reading counts each key once, not that it is one instant's image of the whole
  tree while ordinary writes land, nor that two readings taken at different times
  return the same set.

## Feature doc

- [Consistency](../../docs/lattice/consistency.md)
