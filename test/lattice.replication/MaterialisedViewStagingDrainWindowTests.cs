using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for the atomic-staging drain window. The maintainer holds its
/// persisted resume offset back below a still-staged prepare so a restart
/// re-stages the batch. A drain pass must nevertheless keep reading past the
/// staged entry within the activation: were every pass to restart from the
/// held-back checkpoint and read one batch, a commit terminal more than one
/// batch past its prepare on the same WAL partition would never be read, the
/// batch would stay staged forever, and every later write on that partition
/// would stay invisible.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingDrainWindowTests
{
    private const string Origin = "remote-origin";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ViewPerson(int Age, string Tag);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    [Test]
    public async Task A_commit_terminal_beyond_one_drain_batch_of_its_staged_prepare_still_surfaces_the_batch()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-narrow-batch-src";
        var view = MaterialisedViewClusterFixture.NarrowBatchStagingViewName;
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18));
        _ = factory.Create(source, view, new LatticeViewDefinition(view, projection));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();

        // Place the prepare, three ordinary writes and the commit terminal on
        // one WAL partition. Data entries partition by key hash; a saga
        // terminal partitions by its shard index modulo the partition count.
        var partitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(tree);
        var keys = Enumerable.Range(0, 100_000).Select(i => $"k-{i}").ToArray();
        var stagedKey = keys.First(k =>
            WalPartitionHash.Compute(k, partitions) == ShardOf(k) % partitions);
        var shard = ShardOf(stagedKey);
        var partition = WalPartitionHash.Compute(stagedKey, partitions);
        var siblings = keys
            .Where(k => k != stagedKey && WalPartitionHash.Compute(k, partitions) == partition)
            .Take(3)
            .ToArray();
        Assert.That(siblings, Has.Length.EqualTo(3));

        var txId = Guid.NewGuid();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            stagedKey, Person(30, "atomic"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0,
            txId, atomicBatchSize: 1, atomicBatchIndex: 0);
        foreach (var sibling in siblings)
        {
            await source.SetAsync(sibling, Person(40, sibling));
        }

        await apply.ApplyTxTerminalAsync(
            txId, committed: true, shard, Hlc(1000), Origin, atomicShardCount: 1);

        for (var attempt = 0; attempt < 20 && await maintainer.GetLagAsync() > 0; attempt++)
        {
            await maintainer.DrainAsync();
        }

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync(stagedKey), Is.EqualTo(Person(30, "atomic")),
                "The committed atomic batch must surface once its terminal is read.");
            Assert.That(await viewTree.GetAsync(siblings[^1]), Is.EqualTo(Person(40, siblings[^1])),
                "Ordinary writes past the staged prepare must reach the view.");
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0),
                "The view must catch up to the source head.");
        });
    }
}