# Snapshot cursors (zero observable writes)

`ILattice.OpenSnapshotKeyCursorAsync(...)` and
`ILattice.OpenSnapshotEntryCursorAsync(...)` open a **strict
snapshot-isolation** cursor: every page returned by the cursor reflects
the tree state captured at open time, and no concurrent write -
foreground `SetAsync` / `DeleteAsync`, saga `SetManyAtomicAsync`, range
delete, or replication apply - is ever visible to the cursor for the
remainder of its lifetime.

Snapshot cursors compose with the live cursors documented in
[Durable Cursors](durable-cursors.md): the cursor ID, pagination
contract, and `CloseCursorAsync` lifecycle are identical. Only the
visibility semantics differ.

## When to use a snapshot cursor

| Scenario | Cursor flavour |
|---|---|
| Long-running export, audit, or report that must reflect a single instant | Snapshot (`OpenSnapshot*CursorAsync`) |
| Pagination where the latest writes should appear on later pages | Live (`OpenKeyCursorAsync` / `OpenEntryCursorAsync`) |
| Stable saga-decision view across pages, but mid-page foreground writes are fine | Point-in-time live (`pointInTime: true`) - see [api.md](api.md#point-in-time-cursors) |
| Strict isolation against every concurrent write *and* every concurrent saga | Snapshot |

## How the snapshot is captured

`OpenSnapshot*CursorAsync` performs three deterministic steps before
returning the cursor ID:

1. **Routing capture.** The current `RoutingInfo` (tree map version,
   shard count) is snapshotted so all paging fan-outs target the same
   shard layout.
2. **Per-shard WAL head capture.** Every shard root reports its current
   WAL head offset through `IShardRootGrain.SnapshotWalHeadAsync`. The
   resulting `IReadOnlyDictionary<int, long>` is the bound for each
   shard's replay window.
3. **Registry HLC capture.** The current `IWalCursorRegistry` snapshot
   HLC pins the WAL retention floor.

The three values are packaged as a
`LatticeSnapshotCoordinate` (Orleans-serializable; alias `ol.lsc`) and
persisted on `LatticeCursorState.SnapshotCoordinate`. The coordinate is
deterministic - replaying with the same coordinate yields the same page
sequence, even after silo failover.

## How pages are materialised

On every `NextKeysAsync` / `NextEntriesAsync` call, the cursor grain:

1. Resolves the per-page sub-range from the cursor's persisted
   bookmark.
2. Fans out to the per-shard transient `ISnapshotLeafGrain`s addressed
   by `{treeId}/{shardIndex}/{coordinateHash}`. Each snapshot leaf
   replays its shard's WAL from `0` to `capturedOffset_s` exclusive
   through `ILeafReplayCoordinatorGrain.ReadSliceAsync(...)`, applying
   set / delete / range-delete / saga-commit / saga-abort mutations
   into an in-memory `SortedDictionary`.
3. Performs a k-way merge of the per-shard pages back into the
   cursor's scan order, advancing the persisted bookmark.

The snapshot leaves are activation-cached and idle-evict after
`LatticeOptions.SnapshotLeafIdleTtl` (default 30 minutes). A
subsequent page after eviction transparently rebuilds the leaf -
the underlying WAL prefix is kept alive by the cursor's
`IWalCursorRegistry` pin.

## WAL retention

A snapshot cursor registers a per-cursor WAL retention pin through
`IWalCursorRegistry.ReportCursorAsync(...)` for the lifetime of the
cursor. The pin participates in `LatticeWalGc`'s standard trim
predicate, so the WAL prefix below the captured offset is kept alive
until the cursor is closed (`CloseCursorAsync`), evicted by the
idle-TTL reminder, or expires on `LatticeOptions.MaxCursorSnapshotPinTtl`
(reused from the point-in-time cursor surface).

## Bounding the cost

Open-time replay cost is gated by
`LatticeOptions.MaxSnapshotReplayEntries` (default 10 million entries
per shard). If any shard's `head - 0` exceeds this budget,
`OpenSnapshot*CursorAsync` fails fast with
`LatticeSnapshotReplayBudgetExceededException` and no snapshot leaf is
materialised.

## Observability

| Instrument | Kind | Tags | Description |
|---|---|---|---|
| `orleans.lattice.snapshot.replay.duration` | Histogram (ms) | `tree`, `shard` | Per-shard wall-clock replay time observed during snapshot-leaf open. |
| `orleans.lattice.snapshot.replay.entries` | Counter | `tree`, `shard` | WAL entries consumed during snapshot-leaf replay. |
| `orleans.lattice.snapshot.pins` | UpDownCounter | `tree` | Live WAL retention pins held by snapshot cursors. |

## Examples

### Manual lifecycle (durable cursor shape)

This shape is the one to use when the cursor must outlive the local
scope - the `cursorId` is a `string` so it can be persisted to a
database, sent to a queue, or resumed after a process restart.

```csharp verify
var cursorId = await lattice.OpenSnapshotEntryCursorAsync();
try
{
    while (true)
    {
        var page = await lattice.NextEntriesAsync(cursorId, pageSize: 500);
        foreach (var kv in page.Entries)
        {
            // Process kv.Key / kv.Value. Values reflect the tree state at
            // the moment OpenSnapshotEntryCursorAsync returned, regardless
            // of concurrent writes elsewhere.
        }
        if (!page.HasMore) break;
    }
}
finally
{
    await lattice.CloseCursorAsync(cursorId);
}
```

### Scoped lifecycle (`await using`)

For the common case where the cursor's lifetime is bounded by a
single stack frame, the `LatticeExtensions.Open*CursorScopeAsync`
family returns a `LatticeScopedCursor` that implements
`IAsyncDisposable`. Disposing the scope calls `CloseCursorAsync`
exactly once, even if the body throws. The scope is implicitly
convertible to its underlying `string` cursor ID, so it can be passed
directly to `NextKeysAsync` / `NextEntriesAsync` /
`DeleteRangeStepAsync`.

```csharp verify
await using var scope = await lattice.OpenSnapshotEntryCursorScopeAsync();
while (true)
{
    var page = await lattice.NextEntriesAsync(scope, pageSize: 500);
    foreach (var kv in page.Entries)
    {
        // Same snapshot semantics as the manual shape - the only
        // difference is who calls CloseCursorAsync.
    }
    if (!page.HasMore) break;
}
// scope.DisposeAsync() runs here and closes the cursor.
```

The `*Scope` family covers every cursor flavour - `OpenKeyCursorScopeAsync`,
`OpenEntryCursorScopeAsync`, `OpenSnapshotKeyCursorScopeAsync`,
`OpenSnapshotEntryCursorScopeAsync`, and `OpenDeleteRangeCursorScopeAsync` -
so the choice between scoped and manual is independent of the
cursor's semantics. Pick the scoped shape when the cursor lives and
dies inside one method; pick the manual shape when the cursor ID
must survive a serialization or process boundary.

## Out of scope

- **Cross-cluster snapshot rendezvous.** A snapshot is local to the
  cluster that opened it.
- **Writable snapshots.** `OpenDeleteRangeCursorAsync` with
  `ZeroObservableWrites` is rejected.
- **Snapshot reuse across cursors.** Two cursors opened at logically
  identical coordinates do not share their materialised snapshot leaves;
  each cursor's `coordinateHash` is salted by the cursor ID.
- **Live-vs-snapshot diff.** Compute the diff in the caller by running
  a live cursor against the same range.

