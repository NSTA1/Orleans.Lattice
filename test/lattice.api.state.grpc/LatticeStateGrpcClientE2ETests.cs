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
