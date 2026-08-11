# Surfaces

The state API exposes read-only surfaces: discovery, structure, entry inspection, change history, change observation, metrics, and cluster info. Each is a facade verb with a one-to-one gRPC RPC. The examples below drive them through `LatticeStateApiGrpcClient`; the same request/response records flow when you consume the facade in-process.

Every example assumes a client built like this (see [Client](client.md)):

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);
```

## Discovery

`ListTreesAsync` returns a deterministic, paged catalog of the registered trees. `ListViewsAsync` does the same for materialised views. Paging is driven by `CatalogRequest.PageSize` and the opaque `PageToken` carried forward from the previous page.

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

string? pageToken = null;
do
{
    var page = await stateClient.ListTreesAsync(
        new CatalogRequest { PageSize = 100, PageToken = pageToken, IncludeSystemTrees = false },
        cancellationToken);

    foreach (var tree in page.Entries)
    {
        Console.WriteLine($"{tree.TreeId}  shards={tree.ShardCount}  {tree.Lifecycle}");
    }

    pageToken = page.NextPageToken;
}
while (pageToken is not null);
```

Each `TreeCatalogEntry` carries the tree id, its shard count, and its lifecycle. It also carries `RestoreShadowOfTreeId`: when non-null, the entry is the shadow physical tree of a shadow-cutover restore, and the value is the logical tree the restore was performed for (the alias that now resolves to it). It is stamped from the registry when the shadow tree is created, so a client can classify and group restore shadows as a first-class fact rather than parsing the tree name; it is null for every ordinary tree and for the logical alias itself. Set `IncludeSystemTrees` to surface reserved system trees, and `IncludeViewStats` on a `ListViewsAsync` request to populate per-view statistics. Each `ViewStateSummary` carries the view name, its `SourceTreeId`, and two classification flags: `IsAggregation` (a grouped-reduce view) and `IsHistory` (a change-history / accumulative view whose rows are serialized history records backing the source tree's history timeline rather than directly inspectable value data).

`ListTagIndexesAsync` lists the tag-index membership trees as their own category; set `SourceTreeId` to restrict it to the indexes that cover one tree. `ListTagValuesAsync` then enumerates the distinct tag values carried by a single index over its subject tree, in ascending ordinal order - pass both `SourceTreeId` (the subject tree) and `IndexName` (the index). Both are paged like the other catalogs, so a client can populate a tag picker without scanning the index tree itself.

Three index-wide read methods browse a multi-tree tag index without decoding any internal membership-tree naming convention. Each is scoped to a single `IndexName` and spans every tree the index covers:

- `ListCoveredTreesAsync(CatalogRequest { IndexName })` returns a `CoveredTreeCatalogPage` of the subject-tree ids the index covers, in ascending ordinal order.
- `ListIndexTagsAsync(CatalogRequest { IndexName })` returns a `TagValueCatalogPage` of the distinct tag values across the whole index (the index-wide union of `ListTagValuesAsync` over every covered tree).
- `ScanTagMembersAsync(TagMemberScanRequest { IndexName, Tag })` returns a `TagMemberScanPage` of the live `TagMember` rows (each a `{ TreeId, Key }` pair) carrying that tag, ordered by `(tree id, key)`. Membership rows whose primary key no longer exists (stale until the next reconcile) are filtered out, so the page reflects only live members. `PageSize` is clamped to `[1, 1000]` (default 100); resume with the returned `NextPageToken`.

Because these three span many trees, they present no single target tree to a host authorizer - they authorize like the cluster-wide `ListTrees` / `ListViews` rather than the subject-tree-scoped `ListTagValues`.

## Structure

`GetTreeStructureAsync` returns the structural node graph of a tree as a `StructureResponse`. `Roots` holds one `NodeStateSummary` per shard root; each node reports its kind (leaf or internal), child fan-out, and `SubtreeKeyCount` - the live-key count under that subtree. Summing the roots' subtree counts gives the tree's total live-key count. An unknown tree is part of the typed contract, not a fault: the response carries `Status = TreeNotFound` with empty `Roots` rather than raising a gRPC `NotFound`, matching the found/absent convention `GetEntryAsync` uses.

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

var structure = await stateClient.GetTreeStructureAsync(
    new StructureRequest { TreeId = "factory-floor", DepthLimit = 3 },
    cancellationToken);

long liveKeys = 0;
foreach (var root in structure.Roots)
{
    liveKeys += root.SubtreeKeyCount;
}
Console.WriteLine($"shards={structure.Roots.Count}  liveKeys={liveKeys}");
```

The walk is bounded by `DepthLimit` and `MaxNodes`, and can be focused on a single shard (`ShardIndex`) or rooted at a sub-path node (`SubPathNodeId`) to drill in without materialising the whole graph.

## Entries

`ScanEntriesAsync` returns a key-ordered page of entries. The `Mode` selects the cursor isolation. The default `EntryScanMode.Snapshot` opens a **snapshot-isolated cursor**: every page reflects the tree state captured when the scan opened, isolated from concurrent writes, at the cost of an all-shard baseline capture at open. `EntryScanMode.Live` opens a baseline-free **live cursor** whose paging is keyed on the last yielded key - it never duplicates an already-returned key, but writes committed after the open can appear on later pages and a value reflects its state at read time. `EntryScanMode.LivePointInTime` is a live cursor that additionally pins the in-flight-saga decision view at open without the per-shard baseline capture. Prefer a live mode for casual browsing so a scan does not fan a baseline capture out to every shard root; the Explorer defaults its browse scan to `Live`. The scan can run forward or reverse, be bounded by `StartInclusive` / `EndExclusive`, carry a `ValuePreviewBudget` for inline value previews, and apply a server-side `Predicate`. The mode is fixed when the cursor opens, so it is ignored on a continuation request. The response `Status` distinguishes outcomes that would otherwise be an indistinguishable empty page: an unknown tree returns `TreeNotFound`, and a tag-filtered scan (`IndexName` set) against a tag index that has never been materialised returns `IndexNotFound` - so a mistyped `IndexName` is not silently reported as a real-but-empty `Found` result. Both are typed statuses on a normal response, not a gRPC fault.

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
    var page = await stateClient.ScanEntriesAsync(
        new EntryScanRequest { TreeId = "factory-floor", PageSize = 200, ContinuationToken = continuation },
        cancellationToken);

    foreach (var entry in page.Entries)
    {
        Console.WriteLine($"{entry.Key}  ({entry.ValueLength} bytes)");
    }

    continuation = page.ContinuationToken;
}
while (continuation is not null);
```

Because a **snapshot-isolated** scan opens a per-shard baseline capture that is heavier than a single read, its open can be shed under saturation. When the target tree is WAL-saturated (writes are being back-pressured), the server sheds the snapshot open at admission rather than piling that capture onto shard roots that are already collapsing: `ScanEntriesAsync` then fails with gRPC status `ResourceExhausted` and no cursor is created. This is a transient, retryable back-pressure signal (mapped from the core `LatticeSaturatedException` and gated by [`LatticeOptions.ShedSnapshotOpensWhenSaturated`](../lattice/configuration.md#shedsnapshotopenswhensaturated), on by default) - back off briefly and retry once the tree drains. A `Live` scan opens no baseline and is not subject to this shed, which is another reason to prefer it for casual browsing. The Explorer surfaces the shed as a plain "this table is very busy, try again" notice and leaves the rest of the connection usable.

GetEntryAsync returns the full record for a single key as an EntryGetResponse. Its `Status` reports the outcome: `Found` (the `Entry` carries the full record), `KeyNotFound` (the tree exists but the key does not), or `TreeNotFound` (no such tree). A not-found outcome is part of the typed contract, not a fault - it returns the structured response with a null `Entry` rather than throwing - so a caller distinguishes an unknown tree from a missing key by status. An unauthorised reader sees the same typed not-found a genuine miss returns, so existence is never leaked.

Every `EntryRecord` carries a `CrdtShape` tag: the name of the tree's declared CRDT merge mode (for example `"OrSet"`) when the tree is a typed CRDT, or `null` for an opaque last-writer-wins tree. The shape is the same for every entry on a tree because the merge mode is declared per tree, so a consumer can tell a CRDT entry apart from opaque bytes without decoding the value.

A CRDT entry (`CrdtShape` is non-null) additionally carries `CurrentMembers`: the decoded **current, complete, live members** of the key's folded CRDT state, produced server-side via the registered shape decoder's value-level projection (`ICrdtProvenanceDecoder.DecodeCurrentValue`). This is a point-in-time snapshot of the materialised value, not a per-revision change timeline - that timeline lives on `GetEntryHistoryAsync`. It contains only the shape-specific live members presently in the value, such as set/map entries, register values, sequence nodes, counter totals, or flag state. Each member is a `CrdtMemberValue` (element bytes, the contributing replica id where one exists, and a shape-specific ordinal). `CurrentMembers` is populated on both the single-key `GetEntryAsync` detail and the `ScanEntriesAsync` entry list. An opaque last-writer-wins entry leaves `CurrentMembers` empty and is rendered from its raw value bytes unchanged; the field also degrades to empty when no decoder (or shape registry) is registered.

Materialised-view and tag-index trees reuse the same rendering. A predicate / key-preserving view stores its source tree's value verbatim, so it mirrors the **source** tree's CRDT shape and decodes the same live members; a tag-index membership tree declared as a flag renders its current boolean state. An aggregation view, a history (accumulative) view, and a default last-writer-wins tag index are not member CRDTs and stay opaque blobs without crashing. The Explorer's **Data** tab uses `CurrentMembers` to render a CRDT key's (or CRDT view / tag entry's) current state instead of its raw serialized blob. A scan of an aggregation view (a grouped-reduce or custom-fold view) returns only its materialised group values: the view's internal accumulator / inverse / membership rows, kept under a reserved key prefix, are excluded from the scan exactly as they are from the canonical `ILatticeView` read surface.

## Change history

`GetEntryHistoryAsync` returns a continuation-paged page of a single key's **change-history timeline** as an `EntryHistoryResponse`. Each `EntryRevisionRecord` carries the revision's `Hlc` (the timeline order key), its `Kind` (set, delete, CRDT delta, or range tombstone), the authoring `OriginClusterId`, the mutation `Category`, and a value-or-metadata view bounded by the tree's retention mode: a size-bounded `ValuePreview` (plus full `ValueLength`) when values are retained, or a `ValueHash` and length only under metadata-only retention. A CRDT revision whose bytes were retained in full also carries the decoded element-level `MemberChanges` (added / removed element, replica, and causal ordinal).

Each revision carries a per-row `Retention` descriptor (the mode applied when the row was written, and whether its value bytes were retained), so a consumer can detect a retention-config transition by diffing adjacent revisions of the same key. The top-level `Bound` reports how the timeline is bounded: `BoundedByAge` when sourced from the durable per-key history view (clean, age-bounded, never truncated), `Truncated` when the retained write-ahead-log window has lost its oldest revisions (with `EarliestAvailable` naming the oldest still-readable revision), or `WalWindowFallback` when no history view is enabled. Set `Reverse` to order revisions newest-first within each page. A key that has never been written is reported as `Status = KeyNotFound`, distinct from a key that exists but whose retained timeline is empty or truncated (which stays `Found` with a `Truncated`/`WalWindowFallback` bound) - so an absent key is not mistaken for a real key whose history has aged out.

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
        var retained = revision.Retention.ValueRetained ? $"{revision.ValueLength} bytes" : $"hash {revision.ValueHash}";
        Console.WriteLine($"{revision.Hlc}  {revision.Kind}  {retained}");
    }

    continuation = page.ContinuationToken;
}
while (continuation is not null);
```

## Change observation

`ObserveChangesAsync` subscribes to a tree's live mutation stream and yields a `StateChangeNotification` per mutation until the call is cancelled or the server ends the stream. Each notification carries the `TreeId`, the affected `Key`, and the change `Kind` (set, delete, or range delete). Set `IncludeMaintenance` on the request to also observe maintenance rewrites.

```csharp verify
using System.Threading;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
cts.CancelAfter(TimeSpan.FromSeconds(30));

await foreach (var change in stateClient.ObserveChangesAsync(
    new StateObserveRequest { TreeId = "factory-floor" }, cts.Token))
{
    Console.WriteLine($"{change.Kind} {change.Key}");
}
```

Because the feed is server-streamed, cancel the call (via the `CancellationToken`) to unsubscribe; the server tears the subscription down when the stream ends.

## Metrics

`GetMetricsSnapshotAsync` returns a one-shot `TreeMetricsSnapshot` for the requested trees - live keys and shard count per tree, with optional shard hotness (`IncludeShardHotness`) and view lag (`IncludeViewLag`). `ObserveMetricsAsync` subscribes to a live feed: the server emits the initial full snapshot, then **delta-coalesced** snapshots as the metrics move, on the cadence set by `SampleInterval`.

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

var snapshot = await stateClient.GetMetricsSnapshotAsync(
    new TreeMetricsRequest { TreeIds = new[] { "factory-floor" }, IncludeShardHotness = true },
    cancellationToken);

foreach (var tree in snapshot.Trees)
{
    Console.WriteLine($"{tree.TreeId}  liveKeys={tree.LiveKeys}  shards={tree.ShardCount}");
}
```

Many subscribers requesting the same metrics share a single underlying sampling loop, and a cluster with no metric subscribers samples nothing - see [Efficiency](efficiency.md).

### Detail paused under saturation

Each per-tree snapshot is assembled from a single per-shard diagnostics walk that backs both the tile aggregates and the per-shard hotness rows. When a tree is reporting WAL saturation, the sampler deliberately **skips that walk** so the metrics surface never piles read load onto shard roots that are already contended by the write backlog. Such a snapshot sets `DetailPaused = true`: the registry-sourced fields (`Lifecycle`, `ShardCount`) and any requested view lag are still populated, but the live counts (`LiveKeys`, `Tombstones`, `MinDepth`/`MaxDepth`, `ShardsSplitting`) are reported as zero and `ShardHotness` is empty. This is a transient, best-effort state, not an error - the detail returns automatically on the next sample once the tree settles. A consumer should surface it as a "paused - tree is busy" note (the explorer metrics tab does exactly this) rather than treating the zeros as real counts.

## Cluster info

`GetClusterInfoAsync` returns a single `ClusterInfo` record identifying the cluster the client is connected to - its Orleans `ClusterId` (the deployment's logical cluster identity) and `ServiceId` (stable across rolling deployments of the same logical service). Either field is empty when the host did not configure it. The request envelope (`ClusterInfoRequest`) carries no fields today; it exists so the RPC can grow additive projection options later without changing the method signature. A consumer such as the explorer header uses it to show which cluster it is looking at.

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();
using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

var info = await stateClient.GetClusterInfoAsync(new ClusterInfoRequest(), cancellationToken);
Console.WriteLine($"cluster={info.ClusterId}  service={info.ServiceId}");
```
