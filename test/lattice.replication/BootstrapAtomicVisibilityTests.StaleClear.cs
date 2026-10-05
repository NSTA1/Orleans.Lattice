using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4533: a re-seed's receiver settles every leftover pending bucket from
/// the re-seeding source - drained by the decision the export carries, or
/// discarded with no outcome when the source purged its saga - while a saga the
/// export carries in flight and every other origin's buckets are left alone; and it refuses the source's saga records while the re-seed is
/// outstanding, so a straggler cannot stage after the clear. Runs the real
/// leaves, registry and bootstrap coordinator.
/// </summary>
public partial class BootstrapAtomicVisibilityTests
{
    private const string StaleOrigin = "snap-stale-origin";
    private const string OtherOrigin = "snap-other-origin";

    private Task StagePrepareAsync(string tree, string key, byte value, Guid txid, string origin) =>
        _cluster.Client.GetGrain<IReplicationApplyGrain>(tree).ApplyPreparedSetAsync(
            key, new[] { value }, Hlc(8_000), origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 1, atomicBatchIndex: 0);

    private Task<TxStatus> RecordedAsync(string tree, Guid txid) =>
        TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid).GetRecordedStatusAsync(txid);

    [Test]
    public async Task Reseed_clear_discards_a_purged_sagas_buckets_and_leaves_carried_and_foreign_ones()
    {
        const string tree = "snap-stale-clear";
        var purged = Guid.NewGuid();
        var live = Guid.NewGuid();
        var foreign = Guid.NewGuid();
        await StagePrepareAsync(tree, "stale-purged", 1, purged, StaleOrigin);
        await StagePrepareAsync(tree, "stale-live", 2, live, StaleOrigin);
        await StagePrepareAsync(tree, "stale-foreign", 3, foreign, OtherOrigin);

        // The export carried the live saga as a prepared row, and nothing of the
        // purged one.
        var cleared = await StalePendingClearer.ClearAsync(
            _cluster.Client, tree, StaleOrigin, new HashSet<Guid> { live }, new Dictionary<Guid, bool>(), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(cleared, Is.EqualTo(1), "exactly the purged saga is left over");
            Assert.That(await PendingKeysForAsync(tree, purged), Is.Empty, "a purged saga's bucket is discarded");
            Assert.That(await RecordedAsync(tree, purged), Is.EqualTo(TxStatus.InFlight),
                "the discard records no outcome: the source may have committed it");
            Assert.That(await PendingKeysForAsync(tree, live), Does.Contain("stale-live"),
                "a saga the export carries stays staged for its terminal");
            Assert.That(await PendingKeysForAsync(tree, foreign), Does.Contain("stale-foreign"),
                "another origin's pending saga is never touched");
        });
    }

    [Test]
    public async Task Reseed_clear_drains_a_leftover_bucket_by_the_exported_decision()
    {
        // The source decided the saga and trimmed its terminal; the export
        // carries the decision row, and the rewind cannot re-ship the terminal.
        const string tree = "snap-stale-decided";
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        await StagePrepareAsync(tree, "decided-commit", 4, committed, StaleOrigin);
        await StagePrepareAsync(tree, "decided-abort", 5, aborted, StaleOrigin);

        var cleared = await StalePendingClearer.ClearAsync(
            _cluster.Client, tree, StaleOrigin, new HashSet<Guid>(),
            new Dictionary<Guid, bool> { [committed] = true, [aborted] = false }, CancellationToken.None);

        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(cleared, Is.EqualTo(2));
            Assert.That(await PendingKeysForAsync(tree, committed), Is.Empty, "the committed saga's bucket is drained");
            Assert.That(await PendingKeysForAsync(tree, aborted), Is.Empty, "the aborted saga's bucket is drained");
            Assert.That(await lattice.GetAsync("decided-commit"), Is.EqualTo(new byte[] { 4 }), "a committed bucket drains into the projection");
            Assert.That(await lattice.GetAsync("decided-abort"), Is.Null, "an aborted bucket is dropped");
        });
    }

    [Test]
    public void Every_silo_of_the_cluster_honours_the_decision_purge_hold()
    {
        foreach (var silo in _cluster.Silos.OfType<InProcessSiloHandle>())
        {
            Assert.That(PurgeHoldSupport.AllSilosHonour(silo.SiloHost.Services), Is.True,
                "a cluster of current silos reads every silo's manifest and finds the hold grain on each");
        }
    }

    [Test]
    public async Task Saga_undecided_at_the_cut_survives_the_reseed_clear_and_commits_after_it()
    {
        const string tree = "snap-stale-commit-after-cut";
        const string key = "stale-straddler";
        var txid = Guid.NewGuid();

        // Staged before the re-seed; the export carries it as a prepared row
        // because it was still in flight at the cut.
        await StagePrepareAsync(tree, key, 7, txid, StaleOrigin);
        var cleared = await StalePendingClearer.ClearAsync(
            _cluster.Client, tree, StaleOrigin, new HashSet<Guid> { txid }, new Dictionary<Guid, bool>(), CancellationToken.None);
        Assert.That(cleared, Is.Zero, "precondition: the clear leaves the exported saga alone");

        // It commits after the cut; its terminal ships after the re-seed.
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(tree).ApplyTxTerminalAsync(
            txid, true, LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount), Hlc(8_100), StaleOrigin);

        Assert.That(await _cluster.Client.GetGrain<ILattice>(tree).GetAsync(key), Is.EqualTo(new byte[] { 7 }),
            "a saga undecided at the cut commits whole after the re-seed");
    }

    [Test]
    public async Task Receiver_refuses_a_re_seeding_senders_saga_records_until_the_drain_consumes_the_request()
    {
        const string tree = "snap-stale-straggler";
        var coordinator = _cluster.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        WalRecord[] saga =
        [
            new WalRecord { TreeId = tree, Op = MutationKind.Set, Key = "k", Value = [1], IsPrepared = true, TransactionId = Guid.NewGuid(), AtomicBatchSize = 1 },
        ];
        WalRecord[] plain = [new WalRecord { TreeId = tree, Op = MutationKind.Set, Key = "k", Value = [1] }];

        var before = await ReplicationReseedResponder.RefusesStragglerAsync(_cluster.Client, tree, StaleOrigin, saga, NullLogger.Instance);
        await coordinator.BootstrapForReseedAsync(StaleOrigin, 5, start: false);

        Assert.Multiple(async () =>
        {
            Assert.That(before, Is.False, "no re-seed is outstanding yet");
            Assert.That(await coordinator.IsReseedPendingAsync(StaleOrigin), Is.True);
            Assert.That(await ReplicationReseedResponder.RefusesStragglerAsync(_cluster.Client, tree, StaleOrigin, saga, NullLogger.Instance),
                Is.True, "a saga record from the re-seeding sender is a straggler and is refused");
            Assert.That(await ReplicationReseedResponder.RefusesStragglerAsync(_cluster.Client, tree, StaleOrigin, plain, NullLogger.Instance),
                Is.False, "plain records keep applying");
            Assert.That(await ReplicationReseedResponder.RefusesStragglerAsync(_cluster.Client, tree, OtherOrigin, saga, NullLogger.Instance),
                Is.False, "another sender's saga records are unaffected");
        });
    }
}
