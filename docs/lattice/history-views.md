# Durable per-key history views

A **history view** is an opt-in, append-only materialised view that records every
revision of every key in a source tree. Rather than a bespoke per-leaf revision
store, it reuses the [materialised view](materialised-views.md) subsystem: the
view tails the source tree's write-ahead log and re-keys each mutation into a
durable revision row, so the full timeline survives independently of source
WAL garbage collection.

History is **forward-only**: it begins at view creation and there is no
retroactive backfill, matching the semantics of an immutable audit log.

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
| `MetadataOnly` (default) | Stripped to a content hash and length. | The timeline and change-detection matter; full bytes can be fetched lazily from the TTL-pinned source WAL while still retained. |
| `FullValue` | Stored verbatim per revision. | Point-in-time values must be served directly from the history view. |
| `Hybrid` | Stored verbatim for recent revisions, stripped to metadata beyond a short window. | A recent full-value tail is needed, with an unbounded metadata-only timeline behind it. |

CRDT revisions are always stored as their delta regardless of mode - the delta
*is* the compact history.

An optional **age bound** (a non-zero retention window) stamps each revision row
with an absolute expiry of `now + window`; the normal entry-expiry path reaps old
rows, so no separate reaper is needed. A window of `TimeSpan.Zero` means revisions
do not expire.

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
revisions) and is the escape hatch for genuine view-tree corruption. Retention
mode and window are deliberately kept out of the projection version: they encode
live-tunable policy, not code identity, so changing them never trips a rebuild.

## Limitations

- **Forward-only.** History begins at view creation; there is no retroactive
  backfill of revisions that predate it.
- **Count-based retention ("keep last N per key") is not expressible** in a pure
  per-mutation projection and is out of scope for this substrate.
- **The read path is a separate concern.** This substrate stores the durable
  revision rows; querying a key's timeline and decoding CRDT provenance are built
  on top of it.
- **Do not enable lag-budget eviction on a history view.** Lag-budget eviction
  rebuilds from current source state, which collapses the timeline; leave
  `MaxLagBudget` at its default of zero for accumulative views.
