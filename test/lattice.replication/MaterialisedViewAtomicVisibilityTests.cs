using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Phase 4 atomic-write visibility for single-tree materialised views. A source
/// tree is driven into precise prepared / committed / aborted WAL states through
/// the <see cref="IReplicationApplyGrain"/> prepare+terminal seam (the same seam
/// the cross-cluster receiver uses), and the filter view maintainer is asserted
/// to:
/// <list type="bullet">
///   <item><description>never surface a prepared-but-uncommitted atomic batch;</description></item>
///   <item><description>surface the whole batch atomically once every shard terminal commits;</description></item>
///   <item><description>never surface an aborted batch;</description></item>
///   <item><description>reassemble a multi-shard batch across partition cursors;</description></item>
///   <item><description>re-drain idempotently from the held-back checkpoint without double-applying or losing a batch;</description></item>
///   <item><description>pin the source WAL via <c>BlockedAtHlc</c> while staging and release it after commit;</description></item>
///   <item><description>fall back to a rebuild (and converge) when the bounded staging buffer is exceeded.</description></item>
/// </list>
/// Convergence is driven through the maintainer's <c>DrainAsync</c> so assertions
/// are deterministic rather than timer-dependent.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewAtomicVisibilityTests
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

    private static LatticePredicateNode AdultFilter() =>
        LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18);

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private ILatticeView CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private IReplicationApplyGrain Apply(string tree) =>
        _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

    private ILattice ViewTree(string viewName) =>
        _fixture.Cluster.Client.GetGrain<ILattice>($"view-{viewName}");

    private async Task<IViewMaintainerGrain> MaintainerAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        return maintainer;
    }

    /// <summary>Stages one prepared entry of an atomic batch on the source tree's WAL.</summary>
    private async Task StagePreparedAsync(
        string tree, string key, byte[] value, Guid txId, int batchSize, int batchIndex, long ticks)
        => await Apply(tree).ApplyPreparedSetAsync(
            key, value, Hlc(ticks), Origin,
            sourceVectorClock: null, expiresAtTicks: 0,
            txId, atomicBatchSize: batchSize, atomicBatchIndex: batchIndex);

    /// <summary>Emits one per-shard <c>TxCommit</c> terminal for the batch.</summary>
    private async Task CommitShardAsync(string tree, Guid txId, int shardIndex, long ticks, int shardCount)
        => await Apply(tree).ApplyTxTerminalAsync(
            txId, committed: true, shardIndex, Hlc(ticks), Origin, atomicShardCount: shardCount);

    /// <summary>Emits one <c>TxAbort</c> terminal for the batch.</summary>
    private async Task AbortShardAsync(string tree, Guid txId, int shardIndex, long ticks, int shardCount)
        => await Apply(tree).ApplyTxTerminalAsync(
            txId, committed: false, shardIndex, Hlc(ticks), Origin, atomicShardCount: shardCount);

    private static async Task DrainAsync(IViewMaintainerGrain maintainer, int times)
    {
        for (var i = 0; i < times; i++)
        {
            await maintainer.DrainAsync();
        }
    }

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail("View did not catch up to the source head.");
    }

    [Test]
    public async Task Prepared_but_uncommitted_batch_is_not_visible_in_the_view()
    {
        const string tree = "mv-atomic-uncommitted-src";
        const string view = "mv-atomic-uncommitted-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        await StagePreparedAsync(tree, "p", Person(30, "p1"), txId, batchSize: 2, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, "q", Person(40, "q1"), txId, batchSize: 2, batchIndex: 1, ticks: 101);

        await DrainAsync(maintainer, times: 3);

        var viewTree = ViewTree(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("p"), Is.Null, "A prepared-but-uncommitted entry must not be visible.");
            Assert.That(await viewTree.GetAsync("q"), Is.Null, "A prepared-but-uncommitted entry must not be visible.");
            Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0), "The checkpoint must be held back below the staged prepares.");
        });
    }

    [Test]
    public async Task TxCommit_makes_the_whole_batch_visible_atomically()
    {
        const string tree = "mv-atomic-commit-src";
        const string view = "mv-atomic-commit-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        // Three keys; a minor is filtered out by the projection even though it is
        // part of the committed batch (staging defers WHEN, not HOW, we project).
        var keys = new[] { "ca", "cb", "cm" };
        await StagePreparedAsync(tree, "ca", Person(30, "a1"), txId, 3, 0, 100);
        await StagePreparedAsync(tree, "cb", Person(45, "b1"), txId, 3, 1, 101);
        await StagePreparedAsync(tree, "cm", Person(10, "m1"), txId, 3, 2, 102);

        // Before any terminal: nothing visible.
        await DrainAsync(maintainer, times: 2);
        var viewTree = ViewTree(view);
        Assert.That(await viewTree.GetAsync("ca"), Is.Null, "Pre-commit the batch must be invisible.");

        // Commit every distinct shard the batch touched.
        var shards = keys.Select(ShardOf).Distinct().ToArray();
        var ticks = 1000L;
        foreach (var shard in shards)
        {
            await CommitShardAsync(tree, txId, shard, ticks++, shards.Length);
        }

        await DrainToZeroAsync(maintainer);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("ca"), Is.EqualTo(Person(30, "a1")));
            Assert.That(await viewTree.GetAsync("cb"), Is.EqualTo(Person(45, "b1")));
            Assert.That(await viewTree.GetAsync("cm"), Is.Null, "The minor is filtered by the projection.");
        });
    }

    [Test]
    public async Task Aborted_batch_is_never_surfaced()
    {
        const string tree = "mv-atomic-abort-src";
        const string view = "mv-atomic-abort-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        await StagePreparedAsync(tree, "xa", Person(30, "a1"), txId, 2, 0, 100);
        await StagePreparedAsync(tree, "xb", Person(40, "b1"), txId, 2, 1, 101);

        // Drain once so the prepares are staged, then abort.
        await DrainAsync(maintainer, times: 1);
        await AbortShardAsync(tree, txId, ShardOf("xa"), 1000, shardCount: 1);
        await AbortShardAsync(tree, txId, ShardOf("xb"), 1001, shardCount: 1);

        await DrainToZeroAsync(maintainer);

        var viewTree = ViewTree(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("xa"), Is.Null, "An aborted batch must never be surfaced.");
            Assert.That(await viewTree.GetAsync("xb"), Is.Null, "An aborted batch must never be surfaced.");
        });
    }

    [Test]
    public async Task Multi_shard_batch_is_reassembled_across_partitions()
    {
        const string tree = "mv-atomic-multishard-src";
        const string view = "mv-atomic-multishard-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        // Pick four keys guaranteed to span >=2 distinct shards.
        var keys = new[] { "ms-a", "ms-b", "ms-c", "ms-d" };
        var shards = keys.Select(ShardOf).Distinct().ToArray();
        Assert.That(shards.Length, Is.GreaterThan(1), "Test requires a multi-shard batch.");

        for (var i = 0; i < keys.Length; i++)
        {
            await StagePreparedAsync(tree, keys[i], Person(20 + i, $"v{i}"), txId, keys.Length, i, 100 + i);
        }

        // Commit all but the last shard: the batch is incomplete, so nothing shows.
        var ticks = 1000L;
        for (var s = 0; s < shards.Length - 1; s++)
        {
            await CommitShardAsync(tree, txId, shards[s], ticks++, shards.Length);
        }

        await DrainAsync(maintainer, times: 2);
        var viewTree = ViewTree(view);
        Assert.That(await viewTree.GetAsync(keys[0]), Is.Null, "A partially-committed multi-shard batch must stay invisible.");

        // Commit the final shard: the whole batch reassembles and appears.
        await CommitShardAsync(tree, txId, shards[^1], ticks, shards.Length);
        await DrainToZeroAsync(maintainer);

        await Assert.MultipleAsync(async () =>
        {
            for (var i = 0; i < keys.Length; i++)
            {
                Assert.That(await viewTree.GetAsync(keys[i]), Is.EqualTo(Person(20 + i, $"v{i}")));
            }
        });
    }

    [Test]
    public async Task Re_drain_from_held_back_checkpoint_does_not_double_apply_or_lose_the_batch()
    {
        const string tree = "mv-atomic-replay-src";
        const string view = "mv-atomic-replay-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        await StagePreparedAsync(tree, "ra", Person(30, "a1"), txId, 2, 0, 100);
        await StagePreparedAsync(tree, "rb", Person(40, "b1"), txId, 2, 1, 101);

        // Re-drain several times while uncommitted: each pass re-reads and
        // re-stages the prepares from the held-back checkpoint. Nothing leaks.
        var viewTree = ViewTree(view);
        for (var i = 0; i < 4; i++)
        {
            await maintainer.DrainAsync();
            Assert.That(await viewTree.GetAsync("ra"), Is.Null, "Re-draining a staged batch must not surface it.");
            Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0));
        }

        // Commit, then drain again repeatedly: the deterministic view-saga
        // operation id makes the apply idempotent, so no double-apply.
        await CommitShardAsync(tree, txId, ShardOf("ra"), 1000, shardCount: 1);
        await CommitShardAsync(tree, txId, ShardOf("rb"), 1001, shardCount: 1);
        await DrainToZeroAsync(maintainer);
        await DrainAsync(maintainer, times: 3);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("ra"), Is.EqualTo(Person(30, "a1")));
            Assert.That(await viewTree.GetAsync("rb"), Is.EqualTo(Person(40, "b1")));
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0), "After commit the checkpoint must advance to the head.");
        });
    }

    [Test]
    public async Task BlockedAtHlc_is_reported_while_staging_and_cleared_after_commit()
    {
        const string tree = "mv-atomic-pin-src";
        const string view = "mv-atomic-pin-view";
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var cursors = _fixture.SiloServices.GetRequiredService<IWalCursorRegistry>();
        var txId = Guid.NewGuid();

        await StagePreparedAsync(tree, "pa", Person(30, "a1"), txId, 2, 0, 100);
        await StagePreparedAsync(tree, "pb", Person(40, "b1"), txId, 2, 1, 105);

        await DrainAsync(maintainer, times: 1);

        var pinned = await cursors.GetBlockedFloorAsync(tree);
        Assert.That(pinned, Is.Not.Null, "While staging, the maintainer must pin the WAL at the oldest prepared HLC.");
        Assert.That(pinned!.Value, Is.EqualTo(Hlc(100)), "The pin must be the oldest staged prepared entry's HLC.");

        await CommitShardAsync(tree, txId, ShardOf("pa"), 1000, shardCount: 1);
        await CommitShardAsync(tree, txId, ShardOf("pb"), 1001, shardCount: 1);
        await DrainToZeroAsync(maintainer);

        var afterCommit = await cursors.GetBlockedFloorAsync(tree);
        Assert.That(afterCommit, Is.Null, "After commit nothing is staged, so the WAL pin must be cleared.");
    }

    [Test]
    public async Task Bounded_staging_buffer_forces_a_rebuild_and_the_view_still_converges()
    {
        const string tree = "mv-atomic-backstop-src";
        var view = MaterialisedViewClusterFixture.BackstopViewName;
        _ = CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var viewTree = ViewTree(view);

        using var backstop = new MeterCollector<long>(
            LatticeMetrics.MeterName, "orleans.lattice.view.atomic_staging_backstop");

        // A committed ordinary write that the rebuild must reproduce.
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("committed-adult", Person(50, "ok"));

        // Two distinct un-terminated atomic transactions exceed MaxStagedTransactions=1.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        await StagePreparedAsync(tree, "b1", Person(30, "t1"), tx1, 1, 0, 100);
        await StagePreparedAsync(tree, "b2", Person(31, "t2"), tx2, 1, 0, 101);

        await DrainToZeroAsync(maintainer);

        Assert.That(backstop.Measurements.Count, Is.GreaterThanOrEqualTo(1), "The bounded-buffer backstop metric must fire.");

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("committed-adult"), Is.EqualTo(Person(50, "ok")), "The rebuild must reproduce committed source state.");
            Assert.That(await viewTree.GetAsync("b1"), Is.Null, "Uncommitted prepares must not survive the rebuild.");
            Assert.That(await viewTree.GetAsync("b2"), Is.Null, "Uncommitted prepares must not survive the rebuild.");
        });
    }
}
