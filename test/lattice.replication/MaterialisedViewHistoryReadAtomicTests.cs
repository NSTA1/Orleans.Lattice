using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Public snapshots over two revision-addressed history rows are indivisible.</summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewHistoryReadAtomicTests
{
    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Committed_history_batch_never_splits_its_revision_pair_in_one_public_GetMany_snapshot() =>
        await AssertHistorySnapshotsAsync(splitPublication: false);

    [Test]
    public void History_snapshot_detector_rejects_split_publication_at_the_captured_atomic_call()
    {
        var failure = Assert.ThrowsAsync<AssertionException>(() =>
            AssertHistorySnapshotsAsync(splitPublication: true));
        Assert.That(failure!.Message, Does.Contain("public GetMany snapshot split the history revision pair"));
    }

    private async Task AssertHistorySnapshotsAsync(bool splitPublication)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var treeId = $"history-reader-{splitPublication}-source";
        var viewName = $"history-reader-{splitPublication}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(treeId);
        var codec = new HistoryRowCodec(_fixture.SiloServices.GetRequiredService<Serializer<HistoryRow>>());
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, viewName, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(
            new LatticeViewDefinition(viewName, new HistoryLatticeViewProjection(codec), accumulative: true)));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();

        const string first = "history-a";
        var firstShard = LatticeSharding.GetShardIndex(first, LatticeConstants.DefaultShardCount);
        var second = Enumerable.Range(0, 64).Select(index => $"history-b{index}")
            .First(key => LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount) != firstShard);
        var secondShard = LatticeSharding.GetShardIndex(second, LatticeConstants.DefaultShardCount);
        var timestamp = new HybridLogicalClock { WallClockTicks = 200 };
        var keys = new List<string>
        {
            HistoryKey.Encode(first, timestamp),
            HistoryKey.Encode(second, timestamp),
        };
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);
        var transaction = Guid.NewGuid();
        await apply.ApplyPreparedSetAsync(first, [1], timestamp, "history-origin", null, 0,
            transaction, atomicBatchSize: 2, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(second, [2], timestamp, "history-origin", null, 0,
            transaction, atomicBatchSize: 2, atomicBatchIndex: 1);
        await apply.ApplyTxTerminalAsync(transaction, true, firstShard, timestamp, "history-origin", atomicShardCount: 2);
        await apply.ApplyTxTerminalAsync(transaction, true, secondShard, timestamp, "history-origin", atomicShardCount: 2);

        var activeTreeId = await maintainer.GetActiveTreeIdAsync();
        var historyTree = _fixture.Cluster.Client.GetGrain<ILattice>(activeTreeId);
        Assert.That(await historyTree.GetManyAsync(keys), Is.Empty);
        var gate = HistoryPublicationHoldFilter.Arm(activeTreeId, keys, splitPublication);
        var drain = maintainer.DrainAsync();
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            AssertSnapshot(await historyTree.GetManyAsync(keys), keys);
            gate.Release();
            while (!drain.IsCompleted)
            {
                AssertSnapshot(await historyTree.GetManyAsync(keys), keys);
                await Task.Delay(1);
            }
            await drain;
            var final = await historyTree.GetManyAsync(keys);
            AssertSnapshot(final, keys);
            Assert.That(final.Keys, Is.EquivalentTo(keys));
        }
        finally
        {
            gate.Release();
            HistoryPublicationHoldFilter.Disarm(activeTreeId);
            await drain.WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    private static void AssertSnapshot(Dictionary<string, byte[]> snapshot, List<string> keys)
    {
        var visible = keys.Count(snapshot.ContainsKey);
        Assert.That(visible, Is.EqualTo(0).Or.EqualTo(2),
            "A public GetMany snapshot split the history revision pair.");
    }
}
