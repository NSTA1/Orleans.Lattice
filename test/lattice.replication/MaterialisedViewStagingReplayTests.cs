using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for replaying a held-back checkpoint after the maintainer
/// reactivates. An unresolved atomic batch holds the persisted resume offset
/// below its prepare, so a reactivated maintainer re-reads entries it already
/// applied. Re-applying an ordinary write that an already-flushed atomic batch
/// superseded on the same key, while the replayed batch flush re-attaches to its
/// completed view saga as a no-op, would leave the view permanently holding the
/// older ordinary value.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewStagingReplayTests
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
    public async Task A_reactivated_maintainer_does_not_replay_an_ordinary_write_over_a_flushed_batch()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-replay-src";
        const string view = "mv-atomic-replay-view";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18));
        _ = factory.Create(source, view, new LatticeViewDefinition(view, projection));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        // Place the held prepare, the ordinary write, the flushed prepare and its
        // commit terminal on one WAL partition so the hold-back really lands below
        // every one of them. Data entries partition by key hash; a saga terminal
        // partitions by its shard index modulo the partition count.
        var partitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(tree);
        var keys = Enumerable.Range(0, 100_000).Select(i => $"replay-{i}").ToArray();
        var key = keys.First(k => WalPartitionHash.Compute(k, partitions) == ShardOf(k) % partitions);
        var partition = WalPartitionHash.Compute(key, partitions);
        var heldKey = keys.First(k => k != key && WalPartitionHash.Compute(k, partitions) == partition);
        var held = Guid.NewGuid();
        var flushed = Guid.NewGuid();

        // An unresolved batch holds the checkpoint back below everything after it.
        await apply.ApplyPreparedSetAsync(
            heldKey, Person(50, "held"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, held, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplySetAsync(key, Person(20, "ordinary"), Hlc(200), Origin, null, 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "atomic"), Hlc(300), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, flushed, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(flushed, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        for (var pass = 0; pass < 5; pass++)
        {
            await maintainer.DrainAsync();
        }

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync(key), Is.EqualTo(Person(30, "atomic")),
            "Before the reactivation the flushed batch is the view's value.");

        await maintainer.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        await Task.Delay(500);
        for (var pass = 0; pass < 5; pass++)
        {
            await maintainer.DrainAsync();
        }

        var sourceValue = await source.GetAsync(key);
        viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Person(30, "atomic")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(sourceValue),
                "Replaying the held-back window must not leave the superseded ordinary write in the view.");
        });
    }

    [Test]
    public async Task A_reactivated_maintainer_does_not_re_stage_a_flushed_batch_whose_terminal_it_cannot_re_read()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-atomic-replay-terminal-src";
        const string view = "mv-atomic-replay-terminal-view";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18));
        _ = factory.Create(source, view, new LatticeViewDefinition(view, projection));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        // The held prepare and the flushed batch's prepare share a WAL partition,
        // so the replay re-reads the flushed prepare; the flushed batch's commit
        // terminal lands on a different partition whose checkpoint advances past it.
        var partitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(tree);
        Assume.That(partitions, Is.GreaterThan(1), "The scenario needs a second WAL partition.");
        var keys = Enumerable.Range(0, 100_000).Select(i => $"replay-terminal-{i}").ToArray();
        var key = keys.First(k => WalPartitionHash.Compute(k, partitions) != ShardOf(k) % partitions);
        var partition = WalPartitionHash.Compute(key, partitions);
        var heldKey = keys.First(k => k != key && WalPartitionHash.Compute(k, partitions) == partition);
        var held = Guid.NewGuid();
        var flushed = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            heldKey, Person(50, "held"), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, held, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Person(30, "atomic"), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, flushed, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(flushed, committed: true, ShardOf(key), Hlc(1000), Origin, atomicShardCount: 1);
        for (var pass = 0; pass < 5; pass++)
        {
            await maintainer.DrainAsync();
        }

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync(key), Is.EqualTo(Person(30, "atomic")),
            "Before the reactivation the flushed batch is the view's value.");

        await maintainer.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        await Task.Delay(500);
        await maintainer.DrainAsync();

        // Resolve the held batch: nothing is legitimately staged after this.
        await apply.ApplyTxTerminalAsync(held, committed: true, ShardOf(heldKey), Hlc(2000), Origin, atomicShardCount: 1);
        for (var attempt = 0; attempt < 20 && await maintainer.GetLagAsync() > 0; attempt++)
        {
            await maintainer.DrainAsync();
        }

        viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync(heldKey), Is.EqualTo(Person(50, "held")));
            Assert.That(await viewTree.GetAsync(key), Is.EqualTo(Person(30, "atomic")));
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0),
                "A replay must not re-stage an already-flushed batch whose terminal lies below an advanced checkpoint.");
        });
    }
}