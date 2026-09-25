# Durable per-key history views

A **history view** is an opt-in, append-only materialised view that records every
revision of every key in a source tree. Rather than a bespoke per-leaf revision
store, it reuses the [materialised view](materialised-views.md) subsystem: the
view tails the source tree's write-ahead log and re-keys each mutation into a
durable revision row, so the full timeline survives independently of source
WAL garbage collection.

History starts from what the source still holds when the view is created. If no
source write-ahead-log entry has been garbage-collected yet, the maintainer's
first drain replays the log from its beginning, so earlier revisions are
recorded with their original clocks; once garbage collection has trimmed the
start of the log, the maintainer instead seeds one revision per live key from
current source state and tails forward from there. Revisions trimmed from the
log before the view existed are never reconstructed.

## How it works

The history projection re-keys each source mutation to `{sourceKey}/{encodedHlc}`,
where the HLC suffix is a fixed-width, chronologically sortable encoding of the
mutation's hybrid logical clock. Because distinct mutations carry distinct HLCs,
distinct revisions map to distinct view keys, so nothing folds and the complete
per-key timeline is retained.

Each revision is stored as a `HistoryRow` carrying the kind of mutation
(a set, a delete, a CRDT delta, or a range-tombstone marker), the originating
cluster, and - for last-writer-wins values - a content hash and length plus,
depending on the retention mode, the value bytes themselves. A CRDT mutation is
recorded as its author delta (the compact, doubling-free history), never as full
state.

## Enabling history on a tree

A history view is created the **runtime** way - through `ILatticeViewFactory` -
rather than declared at startup, because only a runtime-created view can be torn
down again (the enable/disable contract). `LatticeHistoryView.Definition` builds
the accumulative definition; resolve the factory and the silo service provider
from the cluster.

```csharp verify
using Microsoft.Extensions.DependencyInjection;

var factory = client.ServiceProvider.GetRequiredService<ILatticeViewFactory>();
var source = grainFactory.GetGrain<ILattice>("orders");

// Enable history: a runtime view named "orders-history" tailing "orders".
var history = factory.Create(
    source,
    "orders-history",
    LatticeHistoryView.Definition("orders-history", client.ServiceProvider));

// Disable history later: deleting the runtime view stops recording and
// releases the source WAL pin.
await factory.DeleteAsync("orders-history", cancellationToken);
```

## Retention modes

Storage cost is bounded and configurable per source tree. The retention policy is
read by the maintainer at drain time and applied around the (pure) projection, so
a change takes effect for revisions written after it and never rewrites or
rebuilds existing rows.

| Mode | LWW value bytes | Use when |
|------|-----------------|----------|
| `MetadataOnly` (default) | Stripped to a content hash and length. | The timeline and change detection matter, not past values: history reads never return the stripped bytes (the write-ahead-log fallback read applies the same rule). |
| `FullValue` | Stored verbatim per revision. | Point-in-time values must be served directly from the history view. |
| `Hybrid` | Stored verbatim for recent revisions, stripped to metadata beyond a short window. | A recent full-value tail is needed, with an unbounded metadata-only timeline behind it. |

CRDT revisions are always stored as their delta regardless of mode - the delta
*is* the compact history.

Under `Hybrid` the "short window" is set by `LatticeViewOptions.HistoryHybridFullValueWindow` (default 5 minutes): a revision keeps its full value bytes only while its apply-time age is within this window, and older revisions are shaped to metadata.

An optional **age bound** (a positive retention window) stamps each revision row
with an absolute expiry of `now + window`; the normal entry-expiry path reaps old
rows, so no separate reaper is needed. Without an age bound revisions do not
expire: pass `null` as the window to clear it (`SetHistoryRetentionAsync` rejects
a zero or negative window), and `GetHistoryRetentionAsync` reports that state as a
window of `TimeSpan.Zero`.

Both the mode and the window are live-tunable per tree:

```csharp verify
// Keep the last 30 days of full-value revisions for this tree.
await tree.SetHistoryRetentionAsync(
    HistoryRetentionMode.FullValue,
    TimeSpan.FromDays(30),
    cancellationToken);

HistoryRetentionSettings policy = await tree.GetHistoryRetentionAsync(cancellationToken);
// policy.Mode == HistoryRetentionMode.FullValue, policy.Window == 30 days.
```

A tree with no override resolves to `MetadataOnly` with no age bound.

## Reading a key's history

`ILattice.ScanEntryHistoryAsync` returns one key's revision timeline as a page of
`EntryRevision` records. When a history view is enabled for the tree it is the
primary read: a prefix scan over the view tree's `{sourceKey}/{encodedHlc}` rows,
ordered by encoded clock and paged with a continuation token, reusing the same
range-scan machinery as an ordinary entry scan. The read is side-effect-free and
never perturbs the maintainer or its source WAL pin.

```csharp verify
// Read the first page of a key's revision timeline (oldest first).
EntryHistoryPage page = await tree.ScanEntryHistoryAsync(
    "order-42",
    fromHlc: null,
    toHlc: null,
    limit: 100,
    continuation: null,
    cancellationToken);

foreach (EntryRevision revision in page.Revisions)
{
    // revision.Hlc           - the hybrid-logical-clock stamp of the revision
    // revision.Kind          - Set / Delete / CrdtDelta / RangeTombstone
    // revision.OriginClusterId - authoring cluster, or null for a local write
    // revision.ValueHash     - content fingerprint (all retention modes)
    // revision.ValuePreview  - size-bounded value bytes (FullValue / Hybrid)
    // revision.Delta         - size-bounded CRDT author delta (CrdtDelta rows)
}

// Page through the rest of the timeline with the continuation token.
if (page.Continuation is not null)
{
    EntryHistoryPage next = await tree.ScanEntryHistoryAsync(
        "order-42", null, null, 100, page.Continuation, cancellationToken);
}
```

The optional `fromHlc` / `toHlc` arguments clamp the scan to an inclusive
hybrid-logical-clock window. The returned `EntryHistoryPage` describes where the
data came from and whether it is complete:

| Field | Meaning |
|-------|---------|
| `Source` | `View` when read from the durable history view, `WalWindow` for the best-effort write-ahead-log fallback, or `None` when neither is available - and also when the access gate denies a point read of the key, which returns an empty page rather than throwing. |
| `Truncated` | Always `false` on the `View` path - the timeline is never cut off below by WAL garbage collection; it is bounded by the configured retention age and by any rebuild that collapsed it (see [the accumulative guard](#the-accumulative-guard)). `true` on the `WalWindow` fallback when garbage collection has trimmed older entries. |
| `EarliestAvailable` | On a truncated `WalWindow` read, the oldest hybrid-logical-clock still readable; `HybridLogicalClock.Zero` otherwise. |

### Fallback without a history view

For a tree that has **not** opted into a history view, the same method falls back,
best-effort, to the retained source write-ahead-log window for the key: it
enumerates surviving mutations above the current per-partition garbage-collection
trim point in offset order and reports `Source == EntryHistorySource.WalWindow`.
This window is bounded by WAL garbage collection, so it sets `Truncated` and
`EarliestAvailable` honestly when older revisions have already been trimmed - a
partial window is never presented as a full history. Enable a history view when a
durable, retention-bounded timeline is required.

## The accumulative guard

An ordinary materialised view is rebuilt from *current* source state when its
projection version changes or when an unconstrained range delete is observed -
both of which would collapse a history timeline. A history view's registration
carries an **accumulative** flag that changes exactly those two behaviours:

- **Projection-version change:** the view adopts the new version forward and keeps
  its existing rows, resuming the drain from the durable checkpoint. The worst
  case is a row-shape discontinuity at the version boundary, never data loss.
- **Unconstrained range delete:** the maintainer records a range-tombstone marker
  revision rather than rebuilding, because in an append-only log a range delete
  does not erase the fact that prior values existed. A predicate-filtered range
  delete already carries its matched keys and yields exact per-key delete
  revisions.

An explicit, operator-triggered rebuild remains the only intentional clear: it
knowingly re-derives the view from current source state (collapsing prior
revisions) and is the escape hatch for genuine view-tree corruption. The flag does
not suppress the maintainer's other rebuild triggers, and each of them collapses
the timeline the same way: falling off the source write-ahead log (the view
lagged past garbage collection), the atomic-staging backstop, a source-identity
rebind after a restore or failover repoints the source, lag-budget eviction, and
`ReconcileAsync`, whose re-derivation from current source state differs from any
timeline that still holds earlier revisions. Retention
mode and window are deliberately kept out of the projection version: they encode
live-tunable policy, not code identity, so changing them never trips a rebuild.

## Limitations

- **No reconstruction of trimmed history.** The timeline begins with whatever the
  source still holds at creation - its untrimmed write-ahead log, or a
  one-revision-per-key seed from current state once garbage collection has
  trimmed that log; revisions trimmed before the view existed cannot be
  recovered.
- **Count-based retention ("keep last N per key") is not expressible** in a pure
  per-mutation projection and is out of scope for this substrate.
- **The read path is built in.** `ILattice.ScanEntryHistoryAsync` queries a key's
  timeline directly off this substrate (see "Reading a key's history" above);
  decoding element-level CRDT provenance is layered on top of the stored deltas.
- **Do not enable lag-budget eviction on a history view.** Lag-budget eviction
  rebuilds from current source state, which collapses the timeline; leave
  `MaxLagBudget` at its default of zero for accumulative views.
