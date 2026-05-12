using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the receiver-side prepared-apply seam - installs saga
/// prepare-phase mutations authored on a remote cluster into this tree's
/// per-leaf pending-tx bucket and then resolves the saga via the
/// terminal-apply seam. Covers commit, abort, idempotency, and argument
/// validation.
/// </summary>
public partial class LatticeGrainReplicationApplyTests
{
    [Test]
    public async Task ApplyPreparedSetAsync_followed_by_commit_terminal_makes_value_visible()
    {
        const string tree = "rapply-prep-commit";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var hlc = Hlc(50_000, 0);

        await apply.ApplyPreparedSetAsync(
            "k", new byte[] { 9 }, hlc, "site-x", sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid,
            atomicBatchSize: 1, atomicBatchIndex: 0);

        // Pending - not yet visible: the registry has not been marked
        // and strict atomic visibility hides the pending entry.
        var midSaga = await lattice.GetAsync("k");
        Assert.That(midSaga, Is.Null, "Prepared write must not be visible before terminal mark.");

        // Apply commit terminal - registry mark + per-leaf flip.
        await apply.ApplyTxTerminalAsync(
            txid, committed: true, shardIndex: 0,
            terminalHlc: hlc with { WallClockTicks = hlc.WallClockTicks + 1 },
            originClusterId: "site-x");

        var afterCommit = await lattice.GetAsync("k");
        Assert.That(afterCommit, Is.EqualTo(new byte[] { 9 }));
    }

    [Test]
    public async Task ApplyPreparedSetAsync_followed_by_abort_terminal_keeps_value_invisible()
    {
        const string tree = "rapply-prep-abort";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var hlc = Hlc(60_000, 0);

        await apply.ApplyPreparedSetAsync(
            "k", new byte[] { 9 }, hlc, "site-x", sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid,
            atomicBatchSize: 1, atomicBatchIndex: 0);

        await apply.ApplyTxTerminalAsync(
            txid, committed: false, shardIndex: 0,
            terminalHlc: hlc with { WallClockTicks = hlc.WallClockTicks + 1 },
            originClusterId: "site-x");

        var afterAbort = await lattice.GetAsync("k");
        Assert.That(afterAbort, Is.Null, "Aborted prepared write must remain invisible after terminal mark.");
    }

    [Test]
    public async Task ApplyPreparedDeleteAsync_followed_by_commit_terminal_tombstones_existing_value()
    {
        const string tree = "rapply-prep-del-commit";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Seed a visible value first.
        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var deleteHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks + 1_000 };
        var txid = Guid.NewGuid();

        await apply.ApplyPreparedDeleteAsync(
            "k", deleteHlc, "site-x", sourceVectorClock: null,
            transactionId: txid, atomicBatchSize: 1, atomicBatchIndex: 0);

        // Pending tombstone - value still visible until terminal.
        var midSaga = await lattice.GetAsync("k");
        Assert.That(midSaga, Is.EqualTo(new byte[] { 1 }), "Existing value must remain visible while delete is pending.");

        await apply.ApplyTxTerminalAsync(
            txid, committed: true, shardIndex: 0,
            terminalHlc: deleteHlc with { WallClockTicks = deleteHlc.WallClockTicks + 1 },
            originClusterId: "site-x");

        var afterCommit = await lattice.GetAsync("k");
        Assert.That(afterCommit, Is.Null);
    }

    [Test]
    public async Task ApplyTxTerminalAsync_idempotent_on_repeated_commit()
    {
        const string tree = "rapply-term-idempotent";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var hlc = Hlc(70_000, 0);
        var terminalHlc = hlc with { WallClockTicks = hlc.WallClockTicks + 1 };

        await apply.ApplyPreparedSetAsync(
            "k", new byte[] { 5 }, hlc, "site-x", sourceVectorClock: null,
            expiresAtTicks: 0, transactionId: txid,
            atomicBatchSize: 1, atomicBatchIndex: 0);

        await apply.ApplyTxTerminalAsync(txid, committed: true, shardIndex: 0, terminalHlc, "site-x");
        await apply.ApplyTxTerminalAsync(txid, committed: true, shardIndex: 0, terminalHlc, "site-x");
        await apply.ApplyTxTerminalAsync(txid, committed: true, shardIndex: 0, terminalHlc, "site-x");

        var afterCommit = await lattice.GetAsync("k");
        Assert.That(afterCommit, Is.EqualTo(new byte[] { 5 }));
    }

    [Test]
    public void ApplyPreparedSetAsync_throws_on_empty_transaction_id()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-prep-validate-set");

        Assert.That(
            async () => await apply.ApplyPreparedSetAsync(
                "k", new byte[] { 1 }, Hlc(1), "site-x", sourceVectorClock: null,
                expiresAtTicks: 0, transactionId: Guid.Empty,
                atomicBatchSize: 1, atomicBatchIndex: 0),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyPreparedDeleteAsync_throws_on_empty_transaction_id()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-prep-validate-del");

        Assert.That(
            async () => await apply.ApplyPreparedDeleteAsync(
                "k", Hlc(1), "site-x", sourceVectorClock: null,
                transactionId: Guid.Empty, atomicBatchSize: 1, atomicBatchIndex: 0),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyTxTerminalAsync_throws_on_empty_transaction_id()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-term-validate");

        Assert.That(
            async () => await apply.ApplyTxTerminalAsync(
                Guid.Empty, committed: true, shardIndex: 0,
                terminalHlc: Hlc(1), originClusterId: "site-x"),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyPreparedSetAsync_throws_on_null_or_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-prep-validate-origin");

        Assert.That(
            async () => await apply.ApplyPreparedSetAsync(
                "k", new byte[] { 1 }, Hlc(1), originClusterId: "",
                sourceVectorClock: null, expiresAtTicks: 0,
                transactionId: Guid.NewGuid(),
                atomicBatchSize: 1, atomicBatchIndex: 0),
            Throws.ArgumentException);
    }
}
