using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regressions for an atomic batch whose prepares fall below a rebuild's resume
/// floor while its commit terminal lands above it. The rebuild captures the floor
/// at the WAL head and lowers it only below batches the maintainer currently has
/// staged, so prepares the maintainer had not yet read (or had just discarded on
/// the bounded-buffer backstop) are never re-read, while their later terminal is.
/// That terminal alone can never complete the batch, so without recovery the
/// committed values never reach the view and the checkpoint is held back forever.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewRebuildSplitSagaTests
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

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private IReplicationApplyGrain Apply(string tree) =>
        _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

    private void CreateAdultView(string sourceTreeId, string viewName)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var projection = new PredicateLatticeViewProjection(AdultFilter());
        _ = factory.Create(source, viewName, new LatticeViewDefinition(viewName, projection));
    }

    private async Task<IViewMaintainerGrain> MaintainerAsync(string viewName)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        return maintainer;
    }

    private Task StagePreparedAsync(string tree, string key, byte[] value, Guid txId, int batchSize, int batchIndex, long ticks)
        => Apply(tree).ApplyPreparedSetAsync(
            key, value, Hlc(ticks), Origin,
            sourceVectorClock: null, expiresAtTicks: 0,
            txId, atomicBatchSize: batchSize, atomicBatchIndex: batchIndex);

    private Task CommitShardAsync(string tree, Guid txId, int shardIndex, long ticks, int shardCount)
        => Apply(tree).ApplyTxTerminalAsync(
            txId, committed: true, shardIndex, Hlc(ticks), Origin, atomicShardCount: shardCount);

    // Appends one shard's commit terminal to the source WAL alone, as the saga's
    // per-partition terminal append does before the next partition's lands.
    private Task AppendCommitTerminalAsync(string tree, Guid txId, int shardIndex, long ticks, int shardCount)
        => _fixture.SiloServices.GetRequiredService<ICommitLogWriter>().AppendAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.TxCommit,
            Key = shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = Hlc(ticks),
            OriginClusterId = Origin,
            TransactionId = txId,
            IsPrepared = false,
            ShardIndex = shardIndex,
            AtomicShardCount = shardCount,
        });

    private Task<int> WalPartitionsAsync(string tree) =>
        _fixture.SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree).AsTask();

    private static async Task<bool> DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return true;
            }

            await Task.Delay(20);
        }

        return false;
    }

    [Test]
    public async Task A_rebuild_before_the_maintainer_reads_a_batch_does_not_strand_its_later_commit()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-rebuild-split-src";
        const string view = "mv-rebuild-split-view";
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);
        var txId = Guid.NewGuid();

        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await source.SetAsync("base-adult", Person(50, "base"));

        // The prepares reach the WAL but the maintainer has not drained them, so
        // nothing is staged when the rebuild captures its resume floor at the head.
        await StagePreparedAsync(tree, "rb-a", Person(30, "a1"), txId, batchSize: 2, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, "rb-b", Person(40, "b1"), txId, batchSize: 2, batchIndex: 1, ticks: 101);

        await maintainer.RebuildAsync();

        // The batch commits after the rebuild's source scan: only its terminals
        // lie above the resume floor.
        await CommitShardAsync(tree, txId, ShardOf("rb-a"), ticks: 1000, shardCount: 1);
        await CommitShardAsync(tree, txId, ShardOf("rb-b"), ticks: 1001, shardCount: 1);

        var caughtUp = await DrainToZeroAsync(maintainer);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(caughtUp, Is.True, "The view must catch up; a stranded commit terminal must not hold the checkpoint back forever.");
            Assert.That(await viewTree.GetAsync("rb-a"), Is.EqualTo(Person(30, "a1")), "The committed batch must reach the view.");
            Assert.That(await viewTree.GetAsync("rb-b"), Is.EqualTo(Person(40, "b1")), "The committed batch must reach the view.");
            Assert.That(await viewTree.GetAsync("base-adult"), Is.EqualTo(Person(50, "base")));
        });
    }

    [Test]
    public async Task A_batch_discarded_by_the_staging_backstop_still_reaches_the_view_when_it_later_commits()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-backstop-split-src";
        var view = MaterialisedViewClusterFixture.BackstopSplitViewName;
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);

        // Two un-terminated batches exceed MaxStagedTransactions = 1: the drain
        // trips the backstop, which discards both and rebuilds from source state.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        await StagePreparedAsync(tree, "b1", Person(30, "t1"), tx1, batchSize: 1, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, "b2", Person(31, "t2"), tx2, batchSize: 1, batchIndex: 0, ticks: 101);
        await maintainer.DrainAsync();

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await viewTree.GetAsync("b1"), Is.Null, "Uncommitted prepares must not survive the rebuild.");

        // tx1 commits after the backstop rebuild; tx2 stays in flight.
        await CommitShardAsync(tree, tx1, ShardOf("b1"), ticks: 1000, shardCount: 1);

        var caughtUp = false;
        for (var attempt = 0; attempt < 50 && !caughtUp; attempt++)
        {
            await maintainer.DrainAsync();
            viewTree = await _fixture.ActiveViewTreeAsync(view);
            caughtUp = await viewTree.GetAsync("b1") is not null && await maintainer.GetLagAsync() == 0;
            await Task.Delay(20);
        }

        viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await viewTree.GetAsync("b1"), Is.EqualTo(Person(30, "t1")), "The batch committed after the backstop must reach the view.");
            Assert.That(await viewTree.GetAsync("b2"), Is.Null, "The still-uncommitted batch must stay invisible.");
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0), "The view must catch up to the source head.");
        });
    }

    [Test]
    public async Task A_replay_fence_does_not_resolve_a_batch_the_backstop_abandoned()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-backstop-fence-src";
        var view = MaterialisedViewClusterFixture.BackstopFenceViewName;
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);

        // A key on b1's shard, so every entry below lands on one WAL partition in log order.
        var shard = ShardOf("f1");
        var sameShard = Enumerable.Range(0, 10_000).Select(i => $"f-same-{i}").First(k => ShardOf(k) == shard);

        // Two un-terminated batches trip the backstop (MaxStagedTransactions = 1), which
        // abandons both and rebuilds with the checkpoint at the head.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        await StagePreparedAsync(tree, "f1", Person(30, "t1a"), tx1, batchSize: 2, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, "f2", Person(31, "t2"), tx2, batchSize: 1, batchIndex: 0, ticks: 101);
        await maintainer.DrainAsync();

        // A third batch, then tx1's second prepare, both drained (tx3 staged, tx1's
        // prepare ignored as abandoned) below the next rebuild's head. That rebuild lowers the resume floor below tx3's prepare and fences the
        // replay at the head with tx3 recorded as staged; tx1 stays abandoned.
        var tx3 = Guid.NewGuid();
        await StagePreparedAsync(tree, sameShard, Person(40, "t3"), tx3, batchSize: 1, batchIndex: 0, ticks: 200);
        await StagePreparedAsync(tree, sameShard, Person(45, "t1b"), tx1, batchSize: 2, batchIndex: 1, ticks: 201);
        await maintainer.DrainAsync();
        await maintainer.RebuildAsync();

        // The resumed tail re-reads the fenced window before either decision lands:
        // tx3's prepare is re-staged, and tx1's fenced prepare must not resolve tx1.
        await maintainer.DrainAsync();

        // tx1 commits and tx3 aborts, above the fence.
        await CommitShardAsync(tree, tx1, shard, ticks: 1000, shardCount: 1);
        await Apply(tree).ApplyTxTerminalAsync(tx3, committed: false, shard, Hlc(1001), Origin, atomicShardCount: 1);

        var caughtUp = await DrainToZeroAsync(maintainer);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(caughtUp, Is.True, "The view must catch up to the source head.");
            Assert.That(await viewTree.GetAsync("f1"), Is.EqualTo(Person(30, "t1a")),
                "The abandoned batch's commit must still reach the view; the replay fence must not resolve it silently.");
            Assert.That(await viewTree.GetAsync(sameShard), Is.EqualTo(Person(45, "t1b")),
                "The abandoned batch's fenced prepare must be exposed once the batch commits.");
            Assert.That(await viewTree.GetAsync("f2"), Is.Null, "The still-uncommitted batch must stay invisible.");
        });
    }

    [Test]
    public async Task A_batch_evicted_from_the_abandoned_list_still_reaches_the_view_when_it_commits()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-abandoned-evict-src";
        var view = MaterialisedViewClusterFixture.AbandonedEvictionViewName;
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);

        // tx1's two keys share a shard, so tx1 commits with a single terminal. kx is on
        // e1's WAL partition, so the drain reads txX's prepare before tx1's.
        var partitions = await WalPartitionsAsync(tree);
        var shard = ShardOf("e1");
        var e1b = Enumerable.Range(0, 10_000).Select(i => $"e-same-{i}").First(k => ShardOf(k) == shard);
        var kx = Enumerable.Range(0, 100_000).Select(i => $"e-x-{i}")
            .First(k => WalPartitionHash.Compute(k, partitions) == WalPartitionHash.Compute("e1", partitions));

        // Two un-terminated batches trip the backstop (MaxStagedTransactions = 1). The
        // one-entry abandoned memory keeps tx1; txX, evicted, is replayed by the rebuild.
        var txX = Guid.NewGuid();
        var tx1 = Guid.NewGuid();
        await StagePreparedAsync(tree, kx, Person(20, "x"), txX, batchSize: 1, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, "e1", Person(30, "t1a"), tx1, batchSize: 2, batchIndex: 0, ticks: 101);
        await maintainer.DrainAsync();
        await maintainer.DrainAsync();

        // txX aborts, so the checkpoint advances past tx1's (abandoned, unread) prepare.
        await Apply(tree).ApplyTxTerminalAsync(txX, committed: false, ShardOf(kx), Hlc(150), Origin, atomicShardCount: 1);
        Assert.That(await DrainToZeroAsync(maintainer), Is.True);

        // Two more batches trip the backstop again; the abandoned memory now holds only
        // the newest of them, so tx1 is forgotten while its prepare is below the checkpoint.
        var tx3 = Guid.NewGuid();
        var tx4 = Guid.NewGuid();
        await StagePreparedAsync(tree, "e3", Person(33, "t3"), tx3, batchSize: 1, batchIndex: 0, ticks: 200);
        await StagePreparedAsync(tree, "e4", Person(34, "t4"), tx4, batchSize: 1, batchIndex: 0, ticks: 201);
        await maintainer.DrainAsync();
        await maintainer.DrainAsync();
        await Apply(tree).ApplyTxTerminalAsync(tx3, committed: false, ShardOf("e3"), Hlc(250), Origin, atomicShardCount: 1);
        await Apply(tree).ApplyTxTerminalAsync(tx4, committed: false, ShardOf("e4"), Hlc(251), Origin, atomicShardCount: 1);
        await maintainer.DrainAsync();

        // tx1's second prepare and its commit then stage as an ordinary batch whose first
        // prepare is never re-read.
        await StagePreparedAsync(tree, e1b, Person(32, "t1b"), tx1, batchSize: 2, batchIndex: 1, ticks: 300);
        await CommitShardAsync(tree, tx1, shard, ticks: 1000, shardCount: 1);

        var caughtUp = await DrainToZeroAsync(maintainer);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(caughtUp, Is.True, "A batch whose prepare was forgotten must not hold the checkpoint back forever.");
            Assert.That(await viewTree.GetAsync("e1"), Is.EqualTo(Person(30, "t1a")), "The committed batch must reach the view.");
            Assert.That(await viewTree.GetAsync(e1b), Is.EqualTo(Person(32, "t1b")), "The committed batch must reach the view.");
            Assert.That(await viewTree.GetAsync(kx), Is.Null, "An aborted batch must stay invisible.");
            Assert.That(await viewTree.GetAsync("e3"), Is.Null, "An aborted batch must stay invisible.");
            Assert.That(await viewTree.GetAsync("e4"), Is.Null, "An aborted batch must stay invisible.");
        });
    }

    [Test]
    public async Task A_rebuild_whose_prepare_scan_is_truncated_does_not_strand_a_later_commit()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-truncated-scan-src";
        var view = MaterialisedViewClusterFixture.TruncatedRebuildScanViewName;
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);

        var partitions = await WalPartitionsAsync(tree);
        var partition = WalPartitionHash.Compute("ts-a", partitions);
        var siblings = Enumerable.Range(0, 100_000).Select(i => $"ts-sib-{i}")
            .Where(k => WalPartitionHash.Compute(k, partitions) == partition)
            .Take(3)
            .ToArray();
        Assert.That(siblings, Has.Length.EqualTo(3));

        // The prepare is followed on its partition by ordinary writes, so a capture
        // that examines only the newest entry below the head does not see it.
        var txId = Guid.NewGuid();
        await StagePreparedAsync(tree, "ts-a", Person(30, "a1"), txId, batchSize: 1, batchIndex: 0, ticks: 100);
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        foreach (var sibling in siblings)
        {
            await source.SetAsync(sibling, Person(50, sibling));
        }

        await maintainer.RebuildAsync();
        await CommitShardAsync(tree, txId, ShardOf("ts-a"), ticks: 1000, shardCount: 1);

        var caughtUp = await DrainToZeroAsync(maintainer);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(caughtUp, Is.True, "A commit whose prepare fell outside the capture scan must not hold the checkpoint back forever.");
            Assert.That(await viewTree.GetAsync("ts-a"), Is.EqualTo(Person(30, "a1")), "The committed batch must reach the view.");
            Assert.That(await viewTree.GetAsync(siblings[0]), Is.EqualTo(Person(50, siblings[0])));
        });
    }

    [Test]
    public async Task A_rebuild_between_two_shard_commits_does_not_strand_the_later_shard()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-rebuild-multishard-src";
        const string view = "mv-rebuild-multishard-view";
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);

        var shardA = ShardOf("ms-a");
        var keyB = Enumerable.Range(0, 10_000).Select(i => $"ms-b-{i}").First(k => ShardOf(k) != shardA);
        var shardB = ShardOf(keyB);

        // A two-shard batch whose shard A terminal reaches the log before shard B's
        // (the saga appends each shard's terminal on its own WAL partition), with the
        // rebuild's head captured between the two.
        var txId = Guid.NewGuid();
        await StagePreparedAsync(tree, "ms-a", Person(30, "a1"), txId, batchSize: 2, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, keyB, Person(40, "b1"), txId, batchSize: 2, batchIndex: 1, ticks: 101);
        await AppendCommitTerminalAsync(tree, txId, shardA, ticks: 1000, shardCount: 2);

        await maintainer.RebuildAsync();
        await maintainer.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        await Task.Delay(500);

        await CommitShardAsync(tree, txId, shardA, ticks: 1000, shardCount: 2);
        await CommitShardAsync(tree, txId, shardB, ticks: 1001, shardCount: 2);

        var caughtUp = await DrainToZeroAsync(maintainer);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(caughtUp, Is.True, "A partially committed batch must not hold the checkpoint back forever.");
            Assert.That(await viewTree.GetAsync("ms-a"), Is.EqualTo(Person(30, "a1")), "The committed batch must reach the view.");
            Assert.That(await viewTree.GetAsync(keyB), Is.EqualTo(Person(40, "b1")), "The later shard's commit must reach the view.");
        });
    }

    [Test]
    public async Task An_abandoned_batch_committing_shard_by_shard_reaches_the_view_after_every_commit()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-abandoned-multishard-src";
        var view = MaterialisedViewClusterFixture.AbandonedMultiShardViewName;
        CreateAdultView(tree, view);
        var maintainer = await MaintainerAsync(view);

        var shardA = ShardOf("am-a");
        var keyB = Enumerable.Range(0, 10_000).Select(i => $"am-b-{i}").First(k => ShardOf(k) != shardA);
        var shardB = ShardOf(keyB);

        // A two-shard batch and a second batch trip the backstop (MaxStagedTransactions = 1),
        // which abandons both.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        await StagePreparedAsync(tree, "am-a", Person(30, "a1"), tx1, batchSize: 2, batchIndex: 0, ticks: 100);
        await StagePreparedAsync(tree, keyB, Person(40, "b1"), tx1, batchSize: 2, batchIndex: 1, ticks: 101);
        await StagePreparedAsync(tree, "am-c", Person(31, "c1"), tx2, batchSize: 1, batchIndex: 0, ticks: 102);
        await maintainer.DrainAsync();

        // Shard A's terminal reaches the log first: the drain rebuilds to read the batch
        // from source, which does not yet expose it.
        await AppendCommitTerminalAsync(tree, tx1, shardA, ticks: 1000, shardCount: 2);
        Assert.That(await DrainToZeroAsync(maintainer), Is.True);

        // The rest of the batch's terminals must be answered too, not dropped as resolved.
        await CommitShardAsync(tree, tx1, shardA, ticks: 1000, shardCount: 2);
        await CommitShardAsync(tree, tx1, shardB, ticks: 1001, shardCount: 2);
        var caughtUp = await DrainToZeroAsync(maintainer);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(caughtUp, Is.True, "The view must catch up to the source head.");
            Assert.That(await viewTree.GetAsync("am-a"), Is.EqualTo(Person(30, "a1")), "The first shard's commit must reach the view.");
            Assert.That(await viewTree.GetAsync(keyB), Is.EqualTo(Person(40, "b1")), "The later shard's commit must reach the view.");
            Assert.That(await viewTree.GetAsync("am-c"), Is.Null, "The still-uncommitted batch must stay invisible.");
        });
    }
}
