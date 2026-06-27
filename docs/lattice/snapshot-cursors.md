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

`OpenSnapshot*CursorAsync` performs these deterministic steps before
returning the cursor ID:

1. **Routing capture.** The current `RoutingInfo` (tree map version,
   shard count) is snapshotted so all paging fan-outs target the same
   shard layout.
2. **Per-shard frozen-baseline capture.** Every shard root walks its
   leaf chain through `IShardRootGrain.CaptureSnapshotBaselineAsync`,
   freezing each `BPlusLeafGrain`'s committed projection and folding its
   own `(leaf_frontier, capturedHead]` WAL tail exactly once (CRDT folds
   are not idempotent, so each record is applied to a single leaf a
   single time). The per-leaf results are unioned into one fully
   materialised per-shard projection and **seeded in memory** into the
   transient `ISnapshotLeafGrain` for that shard, keyed by
   `{treeId}/{shardIndex}/{baselineToken:N}`. The seed is *not* written
   durably at capture - see [Lazy baseline persistence](#lazy-baseline-persistence)
   below. The uniform `capturedHead` (each shard's per-partition WAL
   head, read after every leaf has frozen) is the bound recorded on the
   coordinate.
3. **Registry HLC capture.** The current `IWalCursorRegistry` snapshot
   HLC pins the WAL retention floor.

The captured values are packaged as a
`LatticeSnapshotCoordinate` (Orleans-serializable; alias `ol.lsc`) and
persisted on `LatticeCursorState.SnapshotCoordinate`. The coordinate
carries a fresh per-open `SnapshotBaselineToken` that identifies the
durable baseline rows. The coordinate is deterministic - replaying with
the same coordinate yields the same page sequence, even after silo
failover.

> **Why a frozen baseline rather than open-time WAL replay?** A snapshot
> scan is an ephemeral reader, not a registered WAL cursor. Earlier
> versions replayed each shard's WAL from offset `0` to the captured head
> at every page; once `LatticeWalGc` trimmed the committed prefix the
> reader depended on, that from-zero replay silently returned empty or
> partial results (and restarted any CRDT counter fold from zero). Freezing
> and materialising the projection once at open, then serving those durable
> rows, removes the dependency on the WAL prefix entirely: a later trim
> cannot perturb an already-frozen baseline, and a leaf rebuilt after
> eviction reloads the same rows for a stable point-in-time view across
> failover.

## How pages are materialised

On every `NextKeysAsync` / `NextEntriesAsync` call, the cursor grain:

1. Resolves the per-page sub-range from the cursor's persisted
   bookmark.
2. Fans out to the per-shard transient `ISnapshotLeafGrain`s addressed
   by `{treeId}/{shardIndex}/{baselineToken:N}`. Each snapshot leaf was
   seeded in memory at capture from the materialised per-shard
   projection. Pages are served straight from that in-memory baseline -
   through the same `IsKeyOwned` donor-orphan / virtual-slot ownership
   filter the live read path uses - with **no WAL replay**. If the leaf
   was evicted and later reloads, it reloads from its durable baseline
   row (written lazily; see below). A coordinate persisted before the
   frozen-baseline store existed (empty `SnapshotBaselineToken`) falls
   back to the legacy from-zero WAL replay for wire compatibility.
3. Performs a k-way merge of the per-shard pages back into the
   cursor's scan order, advancing the persisted bookmark.

## Lazy baseline persistence

A snapshot whose entire range drains in a **single page** is the common
case (small range, or a generous page size). For that case the durable
baseline write at capture plus the durable delete at close are pure
write-amplification: the snapshot never survives past the first page, so
the in-memory seed is sufficient.

The frozen baseline is therefore seeded **in memory** at capture and
persisted **lazily**: the cursor flushes every shard's baseline durably
(`ISnapshotLeafGrain.EnsurePersistedAsync`) the first time a page reports
`HasMore == true`, *before* it returns that page or writes its
continuation bookmark. The client only ever observes a continuation
token after every shard baseline is durable, so any cursor that survives
past page 1 - and can therefore fail over to another silo - is always
backed by a durable baseline.

| Outcome | Durable writes |
|---|---|
| Snapshot drains in one page (`HasMore == false`) | none - served from the in-memory seed, close skips the durable delete |
| Snapshot spans multiple pages | one baseline write per shard on the first `HasMore == true` page, deleted at close |

**Eviction before the first read.** Because nothing is persisted until
the first multi-page boundary, a seeded leaf that idle-evicts *before its
first page is read* loses the unread snapshot. The next read fails fast
with `LatticeSnapshotExpiredException` (an `InvalidOperationException`),
and the caller simply reopens the snapshot. This is an availability edge
only - a wrong or partial point-in-time view is never returned.

The snapshot leaves are activation-cached and idle-evict after
`LatticeOptions.SnapshotLeafIdleTtl` (default 30 minutes). A
subsequent page after eviction transparently rebuilds the leaf by
reloading the same durable frozen baseline, so the view stays stable
regardless of any WAL trimming that happened in the meantime.

## WAL retention

A snapshot cursor still registers a per-cursor WAL retention pin through
`IWalCursorRegistry.ReportCursorAsync(...)` for the lifetime of the
cursor. Because pages are served from the frozen baseline rather than the
WAL, the pin is a defensive retention floor (and a diagnostic anchor)
rather than a correctness dependency: even if the pinned prefix were
trimmed, the already-captured baseline continues to serve the snapshot.
Any durable per-shard baselines (written only once a snapshot spans
multiple pages) are deleted when the cursor is closed
(`CloseCursorAsync`) or evicted by the idle-TTL reminder; a single-page
snapshot persists nothing and so deletes nothing.

### Baseline TTL leak-guard

A durable baseline is normally deleted at close, but an interrupted close
(silo crash, lost client) can orphan a baseline row keyed by a per-open
token no other cursor reuses. To bound that leak, every persisted
baseline carries a sliding time-to-live governed by
`LatticeOptions.SnapshotBaselineTtl` (default 6 hours; set to
`Timeout.InfiniteTimeSpan` to disable). `SnapshotBaselineStorageGrain`
arms a self-clearing Orleans reminder when a baseline is written and
slides it forward while the snapshot is actively served, so a long-running
scan keeps its baseline alive while an abandoned one is reclaimed once the
TTL lapses. Both the slide on write and the serving leaf's keep-alive
touch are throttled (no more than once per `SnapshotBaselineTtl / 2`,
floored at one minute) so the reminder table is not rewritten on every
page.

## Bounding the cost

Open-time cost is gated by
`LatticeOptions.MaxSnapshotReplayEntries` (default 10 million entries
per shard). With the frozen-baseline store the per-shard cost is the
**materialised baseline row count** - what the snapshot leaf seeds into
memory - rather than the captured WAL head: after a GC trim the head can
be arbitrarily large while the real projection is small. If the deepest
shard's baseline exceeds this budget,
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

