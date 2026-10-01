# Change history

**Change history** is the per-key revision timeline of a lattice tree: for any
key you can ask "how did this value get here?" and read back the ordered list of
revisions that produced the current state. It works for both tree shapes:

- **Last-writer-wins (LWW) keys** yield a timeline of successive values, so a
  consumer can render value-to-value diffs.
- **CRDT keys** (for example an OR-Set) yield a timeline of author deltas, which
  decode to element-level member changes (which element was added or removed, by
  which replica, at which causal ordinal).

The capability is exposed at three layers, all reading the same underlying
revision timeline:

| Layer | Surface | Use it for |
|-------|---------|-----------|
| Core read path | `ILattice.ScanEntryHistoryAsync` | In-cluster code that wants a key's timeline directly. |
| State API | `GetEntryHistoryAsync` (and its gRPC client) | Out-of-cluster read-only tools and dashboards. |
| Explorer | the **History** tab of a tree's workspace in the **Data** area, with live-follow | Interactive, point-and-click inspection of a key's timeline. |

## Where a timeline comes from

A key's timeline can be served from one of two stored sources, and a separate
live stream lets a reader follow new revisions as they happen:

1. **Durable per-key history view (opt-in, preferred).** When a tree has a
   [history view](history-views.md) enabled, the timeline is read from a durable,
   append-only view that re-keys every source mutation into its own revision row.
   This survives source write-ahead-log (WAL) garbage collection and is bounded by
   the view's configured retention age (a rebuild of the view also collapses it to
   current source state). This is the source you enable when
   a complete, retention-bounded audit timeline is required.
2. **Retained WAL-window fallback (best-effort, no setup).** When a tree has *not*
   opted into a history view, the same read falls back to the surviving entries in
   the source tree's retained WAL window. This needs no configuration and is handy
   for ad-hoc inspection, but it is bounded by WAL garbage collection, so older
   revisions may already have been trimmed. It also reads the retained log raw, so it
   lists an atomic batch's staged writes whether the batch commits or aborts, and a
   delete that compaction has since reaped appears twice - see
   [Fallback without a history view](history-views.md#fallback-without-a-history-view).
3. **Live feed (forward-only).** Independently of either stored source, a reader
   can subscribe to a tree's live mutation stream (`ILatticeStateObserver.ObserveAsync`
   on the State API, or `ObserveChangesAsync` on its gRPC client) to be notified of
   new revisions in real time. The Explorer's History
   timeline uses this for its live-follow mode: it renders the stored revisions once,
   then appends new revisions as they arrive rather than polling for them.

## The core read path

`ILattice.ScanEntryHistoryAsync` returns one key's timeline as a page of
`EntryRevision` records, oldest first, paged with a continuation token. The read is
side-effect-free and never perturbs a maintainer or its source WAL pin. See
[Durable per-key history views](history-views.md) for how the page is served and
what its `Source`, `Truncated` and `EarliestAvailable` fields mean, and the
[API reference](api.md#change-history-events-and-diagnostics) for every
`EntryRevision` field.

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
    // revision.Hlc    - the hybrid-logical-clock stamp (the timeline order key)
    // revision.Kind   - Set / Delete / CrdtDelta / RangeTombstone
    // revision.ValueHash / revision.ValuePreview / revision.Delta - per retention mode
}

// page.Source reports where the data came from, and page.Truncated whether the
// oldest revisions have already been trimmed (only possible on the WAL fallback).
bool durable = page.Source == EntryHistorySource.View;
bool maybeIncomplete = page.Source == EntryHistorySource.WalWindow && page.Truncated;
```

## Retention and truncation caveats

How much of a timeline you can read - and how big it is on disk - depends on the
source and, for a durable history view, on its retention policy.

### Value retention modes (durable history view)

A history view stores each LWW revision's value bytes according to a live-tunable,
per-tree retention mode:

| Mode | LWW value bytes | Trade-off |
|------|-----------------|-----------|
| `MetadataOnly` (default) | Stripped to a content hash and length. | Smallest footprint; values are not served from the view. |
| `FullValue` | Stored verbatim per revision. | Point-in-time values come straight from the view; largest footprint. |
| `Hybrid` | Verbatim for a revision the view applies within a short window of its write, metadata-only for one applied later (see [retention modes](history-views.md#retention-modes)). | Full values for promptly applied revisions without paying for them on a backlog or catch-up replay; a stored row is never re-shaped as it ages. |

CRDT revisions are always stored as their compact author delta regardless of mode -
the delta *is* the history. An anti-entropy or bootstrap resync is the exception: it
ships the full CRDT state with no delta, so its revision is a CRDT-mode `Set`, and
like any `Set` revision it keeps those bytes only under a retention mode that
retains them. Where they are retained, the explorer decodes that full state into a
current-membership snapshot (visually distinct from a per-write member diff) rather
than rendering the raw serialized blob.
An optional **age bound** expires revision rows after a positive
window; with no age bound revisions never expire. Clear the bound by passing `null`
(a zero or negative window is rejected); `GetHistoryRetentionAsync` reports an
unbounded policy as a window of `TimeSpan.Zero`. Set both per tree:

```csharp verify
// Keep the last 30 days of full-value revisions for this tree.
await tree.SetHistoryRetentionAsync(
    HistoryRetentionMode.FullValue,
    TimeSpan.FromDays(30),
    cancellationToken);

HistoryRetentionSettings policy = await tree.GetHistoryRetentionAsync(cancellationToken);
```

### How the timeline is bounded

The page's bound tells a reader how complete the timeline is:

- **Bounded by age (durable view).** On the history-view path the timeline is never
  cut off below by WAL garbage collection; it is bounded by the configured retention
  age, and by any rebuild of the view, which collapses it to current source state
  (see [the accumulative guard](history-views.md#the-accumulative-guard)).
  `page.Truncated` is always `false`.
- **Truncated (WAL-window fallback).** On the fallback path, when WAL garbage
  collection has already trimmed older entries, `page.Truncated` is `true` and
  `page.EarliestAvailable` names the oldest hybrid-logical-clock still readable, on
  every page of the read including continuation pages. A
  partial window is never presented as a full history.

Enable a [history view](history-views.md) when you need a durable, retention-bounded
timeline rather than the best-effort WAL window.

## The State API endpoint

Out-of-cluster tools read the same timeline through the read-only state API. See the
[state-API surfaces reference](../lattice.api.state/surfaces.md#change-history) for
the full `EntryRevisionRecord` shape, including the per-row `Retention` descriptor
and the decoded element-level `MemberChanges` carried by retained CRDT revisions.

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

string? continuation = null;
do
{
    var page = await stateClient.GetEntryHistoryAsync(
        new EntryHistoryRequest { TreeId = "factory-floor", Key = "press-7", Limit = 100, ContinuationToken = continuation },
        cancellationToken);

    foreach (var revision in page.Revisions)
    {
        var view = revision.Retention.ValueRetained ? $"{revision.ValueLength} bytes" : $"hash {revision.ValueHash}";
        Console.WriteLine($"{revision.Hlc}  {revision.Kind}  {view}");
    }

    continuation = page.ContinuationToken;
}
while (continuation is not null);
```

The top-level `Bound` on the response distinguishes a clean `BoundedByAge` read (from
a durable history view) from a `Truncated` WAL window (with `EarliestAvailable`) or a
`WalWindowFallback` read taken when no view is enabled. Set `Reverse` on the request to
page newest-first.

## The Explorer History timeline

[Orleans.Lattice.Explorer](../../src/lattice.explorer) renders the timeline
interactively on the **History** tab of a tree's workspace in its **Data** area.
Open a key from the **Keys** tab and follow its **History of this key** link, or
name a key on the History tab itself. The Explorer pages the key's revisions
through the State API, newest first by default (a **Newest first** switch sets the
order), with **Load older revisions** for the rest: each row shows the revision's
time, kind, and origin cluster, with a line diff between retained LWW values and
the decoded member changes of a CRDT revision. The timeline is retention-aware -
it marks a revision that kept only its value's size and hash as metadata only, and
says when older revisions were trimmed or when the tree keeps no durable history
and only the write-ahead-log window is shown - and an **As of (UTC)** field shows
the key as it stood at a point in time, marking the revision then in effect.

**Live-follow** is on by default, behind a **Follow new revisions** switch: once
the first page of the stored timeline has loaded, the Explorer subscribes to the
tree's live change feed and adds each new revision of the key as it arrives - at
the top under the default newest-first order - so an operator watching a key sees
writes land without reloading. A live row only marks the change; the revision's
value appears once the timeline is next loaded from the stored revisions. Nothing
is followed while an as-of time is set. With a key prefix and no key, the History
tab follows every new change under the prefix instead.

## Try it in the sample

The [MultiSiteManufacturing sample](../../samples/MultiSiteManufacturing) enables a
durable history view (with `FullValue` retention) over two CRDT trees on startup and
then seeds change history into them, so the Explorer's History timeline has a
non-trivial, durable history to show out of the box:

- `mfg-part-operator` - a last-writer-wins register. The seeder writes a sequence of
  operator handoffs to one part's key, so the History timeline shows successive values
  plus diffs.
- `mfg-part-labels` - a process-label OR-Set. The seeder interleaves label adds and
  removes on the same part's key, so the History timeline shows element-level member
  changes.

Both timelines are seeded for part `HPT-BLD-S1-2028-00002`. To reproduce:

1. Start the cluster: `./samples/MultiSiteManufacturing/run.ps1`.
2. Launch the explorer: `./samples/MultiSiteManufacturing/run-explorer.ps1` starts the
   web explorer against the `us` cluster (`-Cluster eu` for the other one), seeding its
   endpoint through the Explorer's
   [environment variables](../lattice.explorer/configuration.md#environment-variables).
   Open the printed `http://localhost:5290` once it has started (see
   [Running and hosting the Explorer](../lattice.explorer/running-the-explorer.md)).
3. In the explorer's **Data** area, open tree `mfg-part-operator` (or `mfg-part-labels`),
   open key `HPT-BLD-S1-2028-00002` from its **Keys** tab, and follow
   **History of this key**.
4. Live-follow starts on its own once the timeline has loaded: add or remove a label
   from the part-detail page in the sample UI and watch the new revision appear at
   the top of the timeline.

The sample enables the durable history view through a small startup activator that
calls `SetHistoryRetentionAsync` and `ILatticeViewFactory.Create` for each tree; see
[history views](history-views.md) for the durable substrate and the other retention
modes.

## See also

- [Durable per-key history views](history-views.md) - the durable revision substrate,
  retention modes, and the accumulative rebuild guard.
- [State-API surfaces](../lattice.api.state/surfaces.md#change-history) - the
  `GetEntryHistoryAsync` request/response contract and the change-observation feed.
