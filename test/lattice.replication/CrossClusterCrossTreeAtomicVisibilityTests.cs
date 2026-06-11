using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage of <b>cross-tree</b> atomic visibility on the receiver
/// side. A cross-tree atomic write spans multiple trees; each tree's terminal
/// replicates independently, so without a receiver-side barrier a reader could
/// observe one tree committed while a sibling tree is still pre-saga. These
/// tests drive the receiver's
/// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/> seam with the
/// cross-tree operation id + receiver-scoped wait set and assert:
/// <list type="bullet">
///   <item><description>Neither tree becomes visible until <b>every</b>
///     participating tree's terminal has arrived (all-or-nothing flip).</description></item>
///   <item><description>An abort on any participating tree drops the whole
///     batch on the receiver.</description></item>
///   <item><description>A partial-replication batch (wait set scoped to the
///     trees replicated here) flips the present subset on its own.</description></item>
/// </list>
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrossClusterCrossTreeAtomicVisibilityTests
{
    private TwoSiteClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new TwoSiteClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private async Task PrepareSetAsync(string tree, string key, byte[] value, HybridLogicalClock hlc, Guid txid)
    {
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            key, value, hlc, TwoSiteClusterFixture.SiteAClusterId,
            sourceVectorClock: null, expiresAtTicks: 0, txid,
            atomicBatchSize: 0, atomicBatchIndex: 0);
    }

    private Task TerminalAsync(
        string tree, Guid txid, bool committed, string key, HybridLogicalClock hlc,
        string operationId, IReadOnlyList<string> waitSet)
    {
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        return apply.ApplyTxTerminalAsync(
            txid, committed, ShardOf(key), hlc, TwoSiteClusterFixture.SiteAClusterId,
            atomicShardCount: 0, crossTreeOperationId: operationId, crossTreeWaitSet: waitSet);
    }

    [Test]
    public async Task Cross_tree_batch_stays_invisible_until_every_tree_terminal_arrives()
    {
        const string treeA = "xt-commit-a";
        const string treeB = "xt-commit-b";
        const string opId = "xt-op-commit";
        var waitSet = new[] { treeA, treeB };
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        await PrepareSetAsync(treeA, "k", [1], Hlc(100), txA);
        await PrepareSetAsync(treeB, "k", [2], Hlc(100), txB);

        var latticeA = _fixture.SiteB.Client.GetGrain<ILattice>(treeA);
        var latticeB = _fixture.SiteB.Client.GetGrain<ILattice>(treeB);

        // First tree's terminal arrives: the barrier is incomplete, so NEITHER
        // tree may become visible.
        await TerminalAsync(treeA, txA, committed: true, "k", Hlc(200), opId, waitSet);

        Assert.Multiple(() =>
        {
            Assert.That(latticeA.GetAsync("k").GetAwaiter().GetResult(), Is.Null,
                "tree A must stay invisible until tree B's terminal also arrives");
            Assert.That(latticeB.GetAsync("k").GetAwaiter().GetResult(), Is.Null,
                "tree B must stay invisible until its own terminal arrives");
        });

        // Second tree's terminal completes the barrier: both flip together.
        await TerminalAsync(treeB, txB, committed: true, "k", Hlc(200), opId, waitSet);

        var a = await latticeA.GetAsync("k");
        var b = await latticeB.GetAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(new byte[] { 1 }));
            Assert.That(b, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task Cross_tree_batch_abort_drops_every_tree()
    {
        const string treeA = "xt-abort-a";
        const string treeB = "xt-abort-b";
        const string opId = "xt-op-abort";
        var waitSet = new[] { treeA, treeB };
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        await PrepareSetAsync(treeA, "k", [1], Hlc(100), txA);
        await PrepareSetAsync(treeB, "k", [2], Hlc(100), txB);

        // Tree A commits but tree B aborts: the global verdict is abort, so
        // NEITHER tree's prepared write may become visible.
        await TerminalAsync(treeA, txA, committed: true, "k", Hlc(200), opId, waitSet);
        await TerminalAsync(treeB, txB, committed: false, "k", Hlc(200), opId, waitSet);

        var latticeA = _fixture.SiteB.Client.GetGrain<ILattice>(treeA);
        var latticeB = _fixture.SiteB.Client.GetGrain<ILattice>(treeB);
        Assert.Multiple(() =>
        {
            Assert.That(latticeA.GetAsync("k").GetAwaiter().GetResult(), Is.Null,
                "an abort on any participating tree must drop tree A's prepared write");
            Assert.That(latticeB.GetAsync("k").GetAwaiter().GetResult(), Is.Null,
                "an abort on any participating tree must drop tree B's prepared write");
        });
    }

    [Test]
    public async Task Cross_tree_batch_with_single_tree_wait_set_flips_present_subset()
    {
        // Partial replication: tree B is a participant of the cross-tree batch
        // but is NOT replicated on this receiver, so the wait set is scoped to
        // tree A only. Tree A must flip visible on its own terminal - the batch
        // is valid on the present subset.
        const string treeA = "xt-partial-a";
        const string opId = "xt-op-partial";
        var waitSet = new[] { treeA };
        var txA = Guid.NewGuid();

        await PrepareSetAsync(treeA, "k", [7], Hlc(100), txA);
        var latticeA = _fixture.SiteB.Client.GetGrain<ILattice>(treeA);

        Assert.That(await latticeA.GetAsync("k"), Is.Null, "prepared write is invisible pre-terminal");

        await TerminalAsync(treeA, txA, committed: true, "k", Hlc(200), opId, waitSet);

        Assert.That(await latticeA.GetAsync("k"), Is.EqualTo(new byte[] { 7 }),
            "a single-tree wait set completes the barrier on the present subset");
    }
}
