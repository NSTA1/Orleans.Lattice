using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Crossed prepare clocks close in one real public snapshot, not a scalar batch order.</summary>
[TestFixture]
[Category("Integration")]
public sealed class MaterialisedViewReadyClosureSnapshotTests
{
    private sealed record Row(int Age);
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
    public async Task Crossed_prepare_clocks_publish_one_complete_winner_cut_in_public_GetMany() =>
        await AssertSnapshotsAsync(splitPublication: false);

    [Test]
    public void Public_snapshot_detector_rejects_split_crossed_clock_closure()
    {
        var failure = Assert.ThrowsAsync<AssertionException>(() => AssertSnapshotsAsync(splitPublication: true));
        Assert.That(failure!.Message, Does.Contain("public snapshot split the crossed-clock closure"));
    }

    private async Task AssertSnapshotsAsync(bool splitPublication)
    {
        using var scope = ViewReadContext.BeginScope();
        var sourceId = $"ready-closure-{splitPublication}-source";
        var viewName = $"ready-closure-{splitPublication}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceId);
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var predicate = LatticePredicateTranslator.Translate<Row>(row => row.Age >= 0);
        _ = factory.Create(source, viewName,
            new LatticeViewDefinition(viewName, new PredicateLatticeViewProjection(predicate)));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();

        const string firstKey = "closure-first";
        var firstShard = LatticeSharding.GetShardIndex(firstKey, LatticeConstants.DefaultShardCount);
        var secondKey = Enumerable.Range(0, 64).Select(index => $"closure-second-{index}")
            .First(key => LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount) != firstShard);
        var secondShard = LatticeSharding.GetShardIndex(secondKey, LatticeConstants.DefaultShardCount);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(sourceId);
        await StageAsync(apply, Guid.NewGuid(), firstKey, secondKey, firstShard, secondShard, 20, 40);
        await StageAsync(apply, Guid.NewGuid(), firstKey, secondKey, firstShard, secondShard, 30, 10);
        var keys = new List<string> { firstKey, secondKey };
        var viewTreeId = await maintainer.GetActiveTreeIdAsync();
        var viewTree = _fixture.Cluster.Client.GetGrain<ILattice>(viewTreeId);
        Assert.That(await viewTree.GetManyAsync(keys), Is.Empty);
        var gate = HistoryPublicationHoldFilter.Arm(viewTreeId, keys, splitPublication);
        var drain = DrainAsync(maintainer);
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            AssertSnapshot(await viewTree.GetManyAsync(keys), firstKey, secondKey);
            gate.Release();
            while (!drain.IsCompleted)
            {
                AssertSnapshot(await viewTree.GetManyAsync(keys), firstKey, secondKey);
                await Task.Delay(1);
            }

            await drain;
            var final = await viewTree.GetManyAsync(keys);
            AssertSnapshot(final, firstKey, secondKey);
            Assert.That(final.Keys, Is.EquivalentTo(keys));
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0));
        }
        finally
        {
            gate.Release();
            HistoryPublicationHoldFilter.Disarm(viewTreeId);
            await drain.WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    private static async Task DrainAsync(IViewMaintainerGrain maintainer)
    {
        for (var pass = 0; pass < 6; pass++)
        {
            await maintainer.DrainAsync();
        }
    }

    private static async Task StageAsync(IReplicationApplyGrain apply, Guid transaction,
        string firstKey, string secondKey, int firstShard, int secondShard, long firstTicks, long secondTicks)
    {
        await apply.ApplyPreparedSetAsync(firstKey, Bytes(firstTicks),
            new HybridLogicalClock { WallClockTicks = firstTicks }, "closure-origin", null, 0,
            transaction, atomicBatchSize: 2, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(secondKey, Bytes(secondTicks),
            new HybridLogicalClock { WallClockTicks = secondTicks }, "closure-origin", null, 0,
            transaction, atomicBatchSize: 2, atomicBatchIndex: 1);
        var terminal = new HybridLogicalClock { WallClockTicks = Math.Max(firstTicks, secondTicks) + 1 };
        await apply.ApplyTxTerminalAsync(transaction, true, firstShard, terminal, "closure-origin", atomicShardCount: 2);
        await apply.ApplyTxTerminalAsync(transaction, true, secondShard, terminal, "closure-origin", atomicShardCount: 2);
    }

    private static byte[] Bytes(long value) => JsonLatticeSerializer<Row>.Default.Serialize(new Row((int)value));

    private static void AssertSnapshot(Dictionary<string, byte[]> snapshot, string firstKey, string secondKey)
    {
        Assert.That(snapshot.ContainsKey(firstKey), Is.EqualTo(snapshot.ContainsKey(secondKey)),
            "A public snapshot split the crossed-clock closure.");
        if (snapshot.TryGetValue(firstKey, out var first))
        {
            Assert.Multiple(() =>
            {
                Assert.That(first, Is.EqualTo(Bytes(30)), "the first key's winner belongs to the second batch");
                Assert.That(snapshot[secondKey], Is.EqualTo(Bytes(40)), "the second key's winner belongs to the first batch");
            });
        }
    }
}
