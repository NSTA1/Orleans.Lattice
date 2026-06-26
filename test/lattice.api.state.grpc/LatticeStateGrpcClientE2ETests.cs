using System.Text;
using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc.Tests;

/// <summary>
/// Full-journey end-to-end test over the public
/// <see cref="LatticeStateApiGrpcClient"/>: discover a tree, read its
/// structure, scan its entries, observe a live mutation, and read live
/// metrics, asserting the counts each stage reports stay consistent with one
/// another. This proves the composition of the whole state API over the real
/// gRPC transport, not just the individual slices.
/// </summary>
[Category("Integration")]
[TestFixture]
public class LatticeStateGrpcClientE2ETests
{
    private const string TreeId = "e2e-explorer";
    private const int KeyCount = 24;
    private const int ShardCount = 2;

    private GrpcStateClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new GrpcStateClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }

    [Test]
    public async Task full_client_journey_is_consistent_across_stages()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);

        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        // Stage 1: discovery. The catalog must surface our tree with its
        // configured shard count.
        var catalog = await client.ListTreesAsync(new CatalogRequest { PageSize = 100 });
        var entry = catalog.Entries.SingleOrDefault(e => e.TreeId == TreeId);
        Assert.That(entry, Is.Not.Null, "the populated tree should appear in the catalog");
        Assert.That(entry!.ShardCount, Is.EqualTo(ShardCount));

        // Stage 2: structure. One root per shard; the live keys summed across
        // shard-root subtrees must equal what we wrote.
        var structure = await client.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = TreeId,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });
        Assert.That(structure.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(structure.Roots, Has.Count.EqualTo(ShardCount));
        var structureLiveKeys = structure.Roots.Sum(r => r.SubtreeKeyCount);
        Assert.That(structureLiveKeys, Is.EqualTo(KeyCount));

        // Stage 3: entry scan. Paging the snapshot cursor to completion must
        // enumerate exactly the keys we wrote, in key order.
        var scanned = await ScanAllAsync(client, TreeId);
        Assert.That(scanned, Has.Count.EqualTo(KeyCount));
        Assert.That(scanned, Is.Ordered.By(nameof(EntryRecord.Key)));
        Assert.That(scanned.Select(e => e.Key), Is.Unique);

        // Stage 4: live change observation. A fresh write must surface on the
        // subscription stream.
        var newKey = GrpcStateClusterFixture.KeyAt(9000);
        using var observeCts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var observed = ObserveOneAsync(client, TreeId, newKey, observeCts.Token);

        // Give the subscription a moment to establish before mutating.
        await Task.Delay(200, observeCts.Token);
        var tree = _fixture.Cluster.Client.GetGrain<Orleans.Lattice.ILattice>(TreeId);
        await tree.SetAsync(newKey, Encoding.UTF8.GetBytes("e2e-live"));

        var sawWrite = await observed;
        Assert.That(sawWrite, Is.True, "the live write should surface on the change stream");

        // Stage 5: live metrics. The snapshot's live-key and shard counts must
        // agree with structure + catalog (now including the live write).
        var metrics = await client.GetMetricsSnapshotAsync(new TreeMetricsRequest
        {
            TreeIds = new[] { TreeId },
            IncludeShardHotness = true,
        });
        var treeMetrics = metrics.Trees.SingleOrDefault(t => t.TreeId == TreeId);
        Assert.That(treeMetrics, Is.Not.Null, "metrics should include the tree");
        Assert.That(treeMetrics!.ShardCount, Is.EqualTo(ShardCount));
        Assert.That(treeMetrics.LiveKeys, Is.EqualTo(KeyCount + 1));

        // Cross-stage closure: re-read structure and confirm it now matches the
        // metrics live-key count after the live write.
        var structureAfter = await client.GetTreeStructureAsync(new StructureRequest
        {
            TreeId = TreeId,
            DepthLimit = StructureRequest.MaxDepthLimit,
            MaxNodes = StructureRequest.MaxNodeBudget,
        });
        Assert.That(
            structureAfter.Roots.Sum(r => r.SubtreeKeyCount),
            Is.EqualTo(treeMetrics.LiveKeys),
            "structure and metrics must agree on live-key count");
    }

    [Test]
    public async Task client_surfaces_views_point_reads_and_metric_streaming_end_to_end()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);
        _fixture.CreateView(TreeId, "e2e-view");

        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        // View discovery through the public client.
        var views = await client.ListViewsAsync(new CatalogRequest { PageSize = 100 });
        var view = views.Entries.SingleOrDefault(v => v.ViewName == "e2e-view");
        Assert.That(view, Is.Not.Null, "the defined view should appear in the view catalog");
        Assert.That(view!.SourceTreeId, Is.EqualTo(TreeId));

        // Single-entry point read through the public client: a present key and a
        // missing key must report distinct outcomes.
        var presentKey = GrpcStateClusterFixture.KeyAt(3);
        var present = await client.GetEntryAsync(new EntryGetRequest { TreeId = TreeId, Key = presentKey });
        Assert.That(present.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(present.Entry, Is.Not.Null);
        Assert.That(present.Entry!.Key, Is.EqualTo(presentKey));

        var missingEx = Assert.ThrowsAsync<RpcException>(async () =>
            await client.GetEntryAsync(new EntryGetRequest { TreeId = TreeId, Key = "no-such-key" }));
        Assert.That(missingEx!.StatusCode, Is.EqualTo(StatusCode.NotFound),
            "a missing key surfaces as a NotFound RPC fault through the public client");

        // Live metric streaming through the public client: the first emitted
        // snapshot is the initial full sample and must carry the tree's counts.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        TreeMetricsSnapshot? first = null;
        await foreach (var snapshot in client.ObserveMetricsAsync(
            new TreeMetricsRequest { TreeIds = new[] { TreeId }, IncludeShardHotness = true },
            cts.Token))
        {
            first = snapshot;
            break;
        }

        Assert.That(first, Is.Not.Null, "the metric stream must emit an initial snapshot");
        var streamed = first!.Trees.SingleOrDefault(t => t.TreeId == TreeId);
        Assert.That(streamed, Is.Not.Null, "the streamed snapshot should include the tree");
        Assert.That(streamed!.ShardCount, Is.EqualTo(ShardCount));
        Assert.That(streamed.LiveKeys, Is.EqualTo(KeyCount));
    }

    [Test]
    public async Task client_surfaces_cluster_info_over_the_transport()
    {
        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        var info = await client.GetClusterInfoAsync(new ClusterInfoRequest());

        Assert.That(info, Is.Not.Null);
        Assert.That(info.ClusterId, Is.Not.Empty,
            "the connected silo's cluster id must round-trip over the gRPC transport");
    }

    [Test]
    public async Task client_lists_tag_indexes_separately_from_trees_end_to_end()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);
        await _fixture.RegisterTreeAsync("tag-e2e-index", shardCount: 2);

        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        var tagIndexes = await client.ListTagIndexesAsync(new CatalogRequest { PageSize = 100 });
        var index = tagIndexes.Entries.SingleOrDefault(t => t.TreeId == "tag-e2e-index");
        Assert.That(index, Is.Not.Null, "the registered tag index should appear in the tag-index catalog");
        Assert.That(index!.IndexName, Is.EqualTo("e2e-index"));
        Assert.That(index.ShardCount, Is.EqualTo(2));

        var trees = await client.ListTreesAsync(new CatalogRequest { PageSize = 100 });
        Assert.That(trees.Entries.Select(t => t.TreeId), Does.Not.Contain("tag-e2e-index"),
            "tag-index trees must not leak into the tree catalog");
    }

    [Test]
    public async Task client_filters_table_rows_by_tag_end_to_end()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);
        var index = _fixture.CreateTagIndex(TreeId, "by-status");
        await index.Key(GrpcStateClusterFixture.KeyAt(1)).AddAsync(["open"]);
        await index.Key(GrpcStateClusterFixture.KeyAt(4)).AddAsync(["open"]);
        await index.Key(GrpcStateClusterFixture.KeyAt(2)).AddAsync(["closed"]);

        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        // The covering tag index is discoverable for the table.
        var covering = await client.ListTagIndexesAsync(new CatalogRequest { SourceTreeId = TreeId, PageSize = 100 });
        Assert.That(covering.Entries.Select(e => e.IndexName), Does.Contain("by-status"));

        // A tag-filtered scan returns only the rows tagged 'open'.
        var open = await client.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = TreeId,
            IndexName = "by-status",
            Tag = "open",
            PageSize = 100,
        });
        Assert.That(open.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(
            open.Entries.Select(e => e.Key),
            Is.EquivalentTo(new[] { GrpcStateClusterFixture.KeyAt(1), GrpcStateClusterFixture.KeyAt(4) }));
    }

    [Test]
    public async Task client_lists_distinct_tag_values_end_to_end()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);
        var index = _fixture.CreateTagIndex(TreeId, "by-status");
        await index.Key(GrpcStateClusterFixture.KeyAt(1)).AddAsync(["open"]);
        await index.Key(GrpcStateClusterFixture.KeyAt(4)).AddAsync(["open"]);
        await index.Key(GrpcStateClusterFixture.KeyAt(2)).AddAsync(["closed"]);

        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        var values = await client.ListTagValuesAsync(new CatalogRequest
        {
            SourceTreeId = TreeId,
            IndexName = "by-status",
            PageSize = 100,
        });

        Assert.That(values.Entries, Is.EqualTo(new[] { "closed", "open" }));
        Assert.That(values.NextPageToken, Is.Null);
    }

    [Test]
    public async Task client_cancels_a_scan_cursor_over_the_transport()
    {
        await _fixture.CreatePopulatedTreeAsync(TreeId, KeyCount, ShardCount);

        await using var host = await _fixture.CreateGrpcHostAsync();
        var client = LatticeStateApiGrpcClient.Create(host.Channel.CreateCallInvoker(), host.Services);

        // Open a multi-page scan and read only the first page, leaving a live cursor.
        var open = await client.ScanEntriesAsync(new EntryScanRequest { TreeId = TreeId, PageSize = 5 });
        Assert.That(open.ContinuationToken, Is.Not.Null.And.Not.Empty, "a multi-page scan must leave a live cursor");

        // Cancel returns a clean ack over the wire.
        var ack = await client.CancelScanAsync(new EntryScanCancelRequest
        {
            TreeId = TreeId,
            ContinuationToken = open.ContinuationToken,
        });
        Assert.That(ack, Is.Not.Null);

        // The cursor is gone: replaying its token now surfaces an InvalidArgument fault.
        var replay = Assert.ThrowsAsync<RpcException>(async () => await client.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = TreeId,
            PageSize = 5,
            ContinuationToken = open.ContinuationToken,
        }));
        Assert.That(replay!.StatusCode, Is.EqualTo(StatusCode.InvalidArgument),
            "replaying a cancelled cursor's token is a rejected client request");

        // Cancelling an unknown cursor is a tolerated no-op, not a fault.
        Assert.DoesNotThrowAsync(async () => await client.CancelScanAsync(new EntryScanCancelRequest
        {
            TreeId = TreeId,
            ContinuationToken = "not-a-real-cursor",
        }));
    }

    private static async Task<List<EntryRecord>> ScanAllAsync(LatticeStateApiGrpcClient client, string treeId)
    {
        var all = new List<EntryRecord>();
        string? token = null;
        do
        {
            var page = await client.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = treeId,
                PageSize = 10,
                ContinuationToken = token,
            });
            all.AddRange(page.Entries);
            token = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(token));

        return all;
    }

    private static async Task<bool> ObserveOneAsync(
        LatticeStateApiGrpcClient client,
        string treeId,
        string expectedKey,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var notification in client.ObserveChangesAsync(
                new StateObserveRequest { TreeId = treeId },
                cancellationToken))
            {
                if (notification.Key == expectedKey)
                {
                    return true;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
        {
        }

        return false;
    }
}
