# Surfaces

The state API exposes five read-only surfaces: discovery, structure, entry inspection, change observation, and metrics. Each is a facade verb with a one-to-one gRPC RPC. The examples below drive them through `LatticeStateApiGrpcClient`; the same request/response records flow when you consume the facade in-process.

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

Each `TreeCatalogEntry` carries the tree id, its shard count, and its lifecycle. Set `IncludeSystemTrees` to surface reserved system trees, and `IncludeViewStats` on a `ListViewsAsync` request to populate per-view statistics.

`ListTagIndexesAsync` lists the tag-index membership trees as their own category; set `SourceTreeId` to restrict it to the indexes that cover one tree. `ListTagValuesAsync` then enumerates the distinct tag values carried by a single index over its subject tree, in ascending ordinal order - pass both `SourceTreeId` (the subject tree) and `IndexName` (the index). Both are paged like the other catalogs, so a client can populate a tag picker without scanning the index tree itself.

## Structure

`GetTreeStructureAsync` returns the structural node graph of a tree as a `StructureResponse`. `Roots` holds one `NodeStateSummary` per shard root; each node reports its kind (leaf or internal), child fan-out, and `SubtreeKeyCount` - the live-key count under that subtree. Summing the roots' subtree counts gives the tree's total live-key count.

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

`ScanEntriesAsync` returns a key-ordered page of entries under a **snapshot-isolated cursor**: every page reflects the tree state captured when the scan opened, isolated from concurrent writes. The scan can run forward or reverse, be bounded by `StartInclusive` / `EndExclusive`, carry a `ValuePreviewBudget` for inline value previews, and apply a server-side `Predicate`.

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

`GetEntryAsync` returns the full record for a single key as an `EntryGetResponse`, including whether the key was found and the value when present.

Every `EntryRecord` carries a `CrdtShape` tag: the name of the tree's declared CRDT merge mode (for example `"OrSet"`) when the tree is a typed CRDT, or `null` for an opaque last-writer-wins tree. The shape is the same for every entry on a tree because the merge mode is declared per tree, so a consumer can tell a CRDT entry apart from opaque bytes without decoding the value.

A CRDT entry (`CrdtShape` is non-null) additionally carries `CurrentMembers`: the decoded element-level members of the key's **current complete state**, produced server-side by folding the stored CRDT state through the registered shape's provenance decoder. This is a point-in-time snapshot of the materialised value (the current member set / per-replica totals), not a per-revision change timeline - that timeline lives on `GetEntryHistoryAsync`. Each member uses the same `CrdtMemberChange` projection as a history row (added / removed element, replica, and causal ordinal). An opaque last-writer-wins entry leaves `CurrentMembers` empty and is rendered from its raw value bytes unchanged. The Explorer's **Data** tab uses this field to render a CRDT key's current state instead of its raw serialized blob.

## Change history

`GetEntryHistoryAsync` returns a continuation-paged page of a single key's **change-history timeline** as an `EntryHistoryResponse`. Each `EntryRevisionRecord` carries the revision's `Hlc` (the timeline order key), its `Kind` (set, delete, CRDT delta, or range tombstone), the authoring `OriginClusterId`, the mutation `Category`, and a value-or-metadata view bounded by the tree's retention mode: a size-bounded `ValuePreview` (plus full `ValueLength`) when values are retained, or a `ValueHash` and length only under metadata-only retention. A CRDT revision whose bytes were retained in full also carries the decoded element-level `MemberChanges` (added / removed element, replica, and causal ordinal).

Each revision carries a per-row `Retention` descriptor (the mode applied when the row was written, and whether its value bytes were retained), so a consumer can detect a retention-config transition by diffing adjacent revisions of the same key. The top-level `Bound` reports how the timeline is bounded: `BoundedByAge` when sourced from the durable per-key history view (clean, age-bounded, never truncated), `Truncated` when the retained write-ahead-log window has lost its oldest revisions (with `EarliestAvailable` naming the oldest still-readable revision), or `WalWindowFallback` when no history view is enabled. Set `Reverse` to order revisions newest-first within each page.

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
