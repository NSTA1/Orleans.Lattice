using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage of cross-cluster saga atomic visibility on the
/// receiver side. Drives the receiver's
/// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/> seam
/// directly with a remote-cluster origin and asserts the receiver's
/// public reader observes the same atomic-visibility semantics that
/// a local saga produces:
/// <list type="bullet">
///   <item><description>Prepared writes are invisible to public reads.</description></item>
///   <item><description>A TxCommit terminal flips them into the visible projection,
///     stamped with the source cluster's HLC verbatim (not the
///     receiver's wall-clock-derived HLC).</description></item>
///   <item><description>A TxAbort terminal drops the prepared writes - they
///     never become visible.</description></item>
///   <item><description>Repeated terminal delivery is idempotent: a second
///     TxCommit with the same transaction id is a no-op.</description></item>
/// </list>
/// </summary>
[TestFixture]
public class CrossClusterAtomicVisibilityTests
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

    /// <summary>
    /// Computes the shard index a given key routes to on the
    /// configured tree. Both Site A and Site B use the default
    /// <see cref="LatticeConstants.DefaultShardCount"/> shard count
    /// because the fixture does not override it, so a key hashes
    /// identically on both clusters and the same numeric shard slot
    /// addresses the matching shard root on the receiver side.
    /// </summary>
    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    [Test]
    public async Task Cross_cluster_prepared_set_lands_invisible_until_TxCommit_arrives()
    {
        const string tree = "ccv-prep-commit";
        const string key = "k";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(100);
        var terminalHlc = Hlc(200);
        var txid = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            key,
            new byte[] { 1, 2, 3 },
            sourceHlc,
            TwoSiteClusterFixture.SiteAClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);

        var preCommit = await lattice.GetAsync(key);
        Assert.That(
            preCommit,
            Is.Null,
            "Prepared set must be invisible to public reads until the TxCommit terminal arrives.");

        await apply.ApplyTxTerminalAsync(
            txid,
            committed: true,
            shardIndex: ShardOf(key),
            terminalHlc,
            TwoSiteClusterFixture.SiteAClusterId);

        var postCommit = await lattice.GetWithVersionAsync(key);
        Assert.That(postCommit.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Cross_cluster_prepared_set_remains_invisible_after_TxAbort()
    {
        const string tree = "ccv-prep-abort";
        const string key = "k";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(100);
        var terminalHlc = Hlc(200);
        var txid = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            key,
            new byte[] { 9 },
            sourceHlc,
            TwoSiteClusterFixture.SiteAClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);

        await apply.ApplyTxTerminalAsync(
            txid,
            committed: false,
            shardIndex: ShardOf(key),
            terminalHlc,
            TwoSiteClusterFixture.SiteAClusterId);

        var afterAbort = await lattice.GetAsync(key);
        Assert.That(
            afterAbort,
            Is.Null,
            "TxAbort terminal must drop the pending entry - it must never be observable to public reads.");
    }

    [Test]
    public async Task Cross_cluster_TxCommit_preserves_source_HLC_on_visible_value()
    {
        // The visible value's version slot must reflect the SOURCE
        // cluster's prepare-phase HLC, not the receiver's wall-clock
        // HLC. This proves the LatticeHlcOverrideContext propagation
        // through ApplyPreparedSetAsync re-stamped the source HLC
        // bit-identically onto the receiver leaf's persisted state.
        const string tree = "ccv-source-hlc";
        const string key = "k";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(987_654_321, 13);
        var terminalHlc = Hlc(987_654_999, 7);
        var txid = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            key,
            new byte[] { 7 },
            sourceHlc,
            TwoSiteClusterFixture.SiteAClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(
            txid,
            committed: true,
            shardIndex: ShardOf(key),
            terminalHlc,
            TwoSiteClusterFixture.SiteAClusterId);

        var versioned = await lattice.GetWithVersionAsync(key);
        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(new byte[] { 7 }));
            Assert.That(
                versioned.Version,
                Is.EqualTo(sourceHlc),
                "Visible value's version must equal the source prepare HLC, not the terminal HLC or the receiver's wall-clock HLC.");
        });
    }

    [Test]
    public async Task Cross_cluster_repeated_TxCommit_is_idempotent()
    {
        // Repeat delivery via the second channel (or any retry) must
        // surface as a no-op. The per-tree TxRegistry repeat-same-
        // outcome guard plus the per-leaf _recentlyTerminal HashSet
        // dedup the second terminal. The receiver state must be
        // bit-identical to the single-delivery case.
        const string tree = "ccv-idempotent";
        const string key = "k";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(100);
        var terminalHlc = Hlc(200);
        var txid = Guid.NewGuid();

        await apply.ApplyPreparedSetAsync(
            key,
            new byte[] { 42 },
            sourceHlc,
            TwoSiteClusterFixture.SiteAClusterId,
            sourceVectorClock: null,
            expiresAtTicks: 0,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(
            txid,
            committed: true,
            shardIndex: ShardOf(key),
            terminalHlc,
            TwoSiteClusterFixture.SiteAClusterId);

        var afterFirst = await lattice.GetWithVersionAsync(key);

        // Second delivery of the same terminal - the registry's
        // repeat-same-outcome no-op + the per-leaf _recentlyTerminal
        // HashSet must absorb it without throwing or mutating state.
        await apply.ApplyTxTerminalAsync(
            txid,
            committed: true,
            shardIndex: ShardOf(key),
            terminalHlc,
            TwoSiteClusterFixture.SiteAClusterId);

        var afterSecond = await lattice.GetWithVersionAsync(key);
        Assert.Multiple(() =>
        {
            Assert.That(afterSecond.Value, Is.EqualTo(afterFirst.Value));
            Assert.That(afterSecond.Version, Is.EqualTo(afterFirst.Version));
            Assert.That(afterSecond.Value, Is.EqualTo(new byte[] { 42 }));
            Assert.That(afterSecond.Version, Is.EqualTo(sourceHlc));
        });
    }

    [Test]
    public async Task Cross_cluster_prepared_delete_lands_invisible_until_TxCommit_makes_tombstone_visible()
    {
        // Mirrors the Set path but for the Delete shape: a prepared
        // tombstone must NOT erase the existing local value until
        // TxCommit arrives. This guards the prepared-delete routing
        // through ApplyPreparedDeleteAsync, the leaf's per-tx pending
        // bucket holds a tombstone, and the terminal flips it.
        //
        // Seed strategy: we seed via the LOCAL public API and then
        // compute the prepared-delete HLC relative to the actual
        // seeded version. Using a fixed Hlc(50) seed via the apply
        // grain is unsafe here because the receiver's wall-clock
        // HLC may already be far past 50 by the time the apply lands,
        // and the visible value's effective HLC after replay may
        // exceed any fixed value we pick for the prepared delete -
        // causing the LWW comparison on the terminal flip to discard
        // the tombstone. Reading the actual seed HLC and adding a
        // generous offset guarantees the prepared delete wins LWW
        // regardless of receiver wall-clock progression.
        const string tree = "ccv-prep-delete";
        const string key = "k";
        var apply = _fixture.SiteB.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);

        // Seed Site B with a value via the LOCAL public API so the
        // visible HLC reflects the receiver's actual state.
        await lattice.SetAsync(key, new byte[] { 1 });
        var seeded = await lattice.GetWithVersionAsync(key);

        // Compute the prepared-delete HLC relative to the seeded
        // version so LWW is guaranteed regardless of wall-clock.
        var sourceHlc = seeded.Version with { WallClockTicks = seeded.Version.WallClockTicks + 1_000 };
        var terminalHlc = sourceHlc with { WallClockTicks = sourceHlc.WallClockTicks + 1 };
        var txid = Guid.NewGuid();

        await apply.ApplyPreparedDeleteAsync(
            key,
            sourceHlc,
            TwoSiteClusterFixture.SiteAClusterId,
            sourceVectorClock: null,
            txid,
            atomicBatchSize: 0,
            atomicBatchIndex: 0);

        var preCommit = await lattice.GetAsync(key);
        Assert.That(
            preCommit,
            Is.EqualTo(new byte[] { 1 }),
            "Prepared delete must NOT erase the visible value until the TxCommit terminal arrives.");

        await apply.ApplyTxTerminalAsync(
            txid,
            committed: true,
            shardIndex: ShardOf(key),
            terminalHlc,
            TwoSiteClusterFixture.SiteAClusterId);

        var postCommit = await lattice.GetAsync(key);
        Assert.That(
            postCommit,
            Is.Null,
            "TxCommit on a prepared delete must flip the tombstone into the visible projection.");
    }
}