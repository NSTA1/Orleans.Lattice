using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the stamp a saga commit drain gives its rows during
/// activation replay.
/// <para>
/// A multi-partition replay defers every saga terminal to pass 2, after every
/// partition's prepares are absorbed, so by the time an earlier saga's commit
/// drains the leaf clock already includes the prepares of LATER sagas on the
/// same keys. Stamping the drained rows with that clock lifted the earlier
/// saga's values above the later saga's prepare, and when the later saga's
/// terminal arrived the commit drain's orphan-drain guard read the row as "a
/// strictly-later saga already drained this key" and discarded the later
/// saga's acknowledged value on this leaf. A silo restart is what produces the
/// shape in practice: the leaf reactivates with one saga committed and the
/// next one prepared but undecided.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Partition 0 holds saga A's prepare and commit; partition 1 holds saga
    /// B's later prepare plus enough other records to be the larger backlog, so
    /// pass 1 absorbs partition 0 first and defers A's commit to pass 2.
    /// </summary>
    private static ILeafReplayCoordinatorGrain[] BuildDeferredDrainCoordinators(Guid sagaA, Guid sagaB) =>
    [
        BuildObservableCoordinator(
            head: 3,
            sliceSize: 8,
            tail: 0,
            onRead: null,
            new CommitLogSliceEntry(1, BuildPreparedSet(
                sagaA, "k", Encoding.UTF8.GetBytes("a"), hlcPhysical: 100, treeId: FlushCeilingTreeId)),
            new CommitLogSliceEntry(2, BuildTerminal(sagaA, committed: true, treeId: FlushCeilingTreeId))),
        BuildObservableCoordinator(
            head: 5,
            sliceSize: 8,
            tail: 0,
            onRead: null,
            new CommitLogSliceEntry(1, BuildPreparedSet(
                sagaB, "k", Encoding.UTF8.GetBytes("b"), hlcPhysical: 200, treeId: FlushCeilingTreeId)),
            FlushSet(2, "other-1", hlcPhysical: 150),
            FlushSet(3, "other-2", hlcPhysical: 150),
            FlushSet(4, "other-3", hlcPhysical: 150)),
    ];

    [Test]
    public async Task A_commit_after_activation_lands_over_an_earlier_saga_drained_in_replay_pass_two()
    {
        var sagaA = Guid.NewGuid();
        var sagaB = Guid.NewGuid();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(
            NewFlushCeilingState(), BuildDeferredDrainCoordinators(sagaA, sagaB), store.Stub);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [sagaB] = TxStatus.InFlight }))
        {
            Assert.That(await grain.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("a")),
                "precondition: replay drained saga A and saga B is still pending");
        }

        // Saga B commits after the leaf reactivated, the way a saga parked
        // across a silo restart resumes.
        await grain.ApplyTxTerminalAsync(sagaB, committed: true);

        Assert.That(await grain.GetAsync("k"), Is.EqualTo(Encoding.UTF8.GetBytes("b")),
            "Saga B's acknowledged commit must land. Seeing saga A's value means the replay drain "
            + "stamped A's row above B's prepare and the orphan-drain guard discarded B.");
    }

    [Test]
    public async Task A_replay_drain_stamps_its_rows_below_a_later_sagas_prepare()
    {
        var sagaA = Guid.NewGuid();
        var sagaB = Guid.NewGuid();
        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(
            NewFlushCeilingState(), BuildDeferredDrainCoordinators(sagaA, sagaB), store.Stub);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [sagaB] = TxStatus.InFlight }))
        {
            var row = await grain.GetWithVersionAsync("k");

            Assert.That(row.Value, Is.EqualTo(Encoding.UTF8.GetBytes("a")));
            Assert.That(row.Version.CompareTo(new HybridLogicalClock { WallClockTicks = 100 }), Is.GreaterThan(0),
                "the drained row must still dominate saga A's own prepare");
            Assert.That(row.Version.CompareTo(new HybridLogicalClock { WallClockTicks = 200 }), Is.LessThan(0),
                "the drained row must not be stamped above saga B's later prepare");
        }
    }
}
