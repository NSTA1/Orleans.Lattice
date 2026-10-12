using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// A replayed pass must never rewrite part of an already flipped atomic batch
/// slice with a non-atomic point write, on any path, whether or not the source
/// can report the version that superseded the replayed write (issue #4869). The
/// source last-writer check (#4885) drops such a write only when the source still
/// holds the batch's version; a compacted tombstone or a read that fails open
/// reports none, so the in-pass guard must drop it from the batch itself.
/// </summary>
public partial class ViewMaintainerReplayFlushTests
{
    /// <summary>
    /// A coordinator that performs the joint flip of the registered slice exactly
    /// once, then answers every redelivered registration with the memoized
    /// decision without re-issuing it, as the real coordinator does.
    /// </summary>
    private sealed class MemoizingCoordinator(FakeViewTree view)
    {
        public bool Applied { get; private set; }

        public IViewCrossTreeCoordinatorGrain Grain { get; } = Substitute.For<IViewCrossTreeCoordinatorGrain>();

        public MemoizingCoordinator Arm()
        {
            ArmMemoizedRepair(Grain, view);
            Grain.RegisterReadyAsync(Arg.Any<ViewCrossTreeReadiness>()).Returns(call =>
            {
                if (!Applied)
                {
                    var readiness = call.ArgAt<ViewCrossTreeReadiness>(0);
                    foreach (var (key, value) in readiness.Upserts)
                    {
                        view.Rows[key] = value;
                    }

                    foreach (var key in readiness.Deletes)
                    {
                        view.Rows.Remove(key);
                    }

                    Applied = true;
                }

                return Task.FromResult(ViewCrossTreeDecision.Committed);
            });
            return this;
        }
    }

    [Test]
    public async Task A_range_path_replay_never_rewrites_the_flushed_batch_slice_non_atomically(
        [Values] bool sourceVersionsReadable)
    {
        var view = new FakeViewTree { LoseNextAtomicResponse = true };
        var txId = Guid.NewGuid();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, OrdinaryThenTwoKeyBatch(txId, withRangeDelete: true));
        if (!sourceVersionsReadable)
        {
            ReportNoStandaloneSourceVersions(h);
        }

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.DrainAsync());
        var pointWritesBeforeReplay = view.PointWrites.Count;
        var atomicBeforeReplay = view.AtomicOperations.Count;

        await h.Grain.DrainAsync();

        var replayAtomic = view.AtomicOperations.Skip(atomicBeforeReplay).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(view.PointWrites.Skip(pointWritesBeforeReplay), Is.Empty,
                "the replay must not re-apply the older ordinary write to the batch's key: a reader would observe k = 2 beside j = 4, a slice that never existed at the source");
            Assert.That(replayAtomic, Does.Contain($"mv-tx-{txId:N}"), "the flip re-attached to the completed saga");
            Assert.That(replayAtomic.Where(id => id.StartsWith("mv-reassert-", StringComparison.Ordinal)), Is.Empty,
                "nothing overwrote the slice, so it already matches and needs no repair");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }));
        });
    }

    [Test]
    public async Task A_replayed_older_range_delete_never_removes_part_of_the_flushed_batch_slice(
        [Values] bool sourceVersionsReadable)
    {
        var view = new FakeViewTree { LoseNextAtomicResponse = true };
        var txId = Guid.NewGuid();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());

        // The range covers the batch's key k but not its key j, and is older than
        // the batch, so at the source the batch's k survives it.
        ArmLog(h,
        [
            RangeDelete(Key, Key + "\0", ticks: 10),
            Prepared(txId, Key, 3, ticks: 20, size: 2, index: 0),
            Prepared(txId, OtherKey, 4, ticks: 20, size: 2, index: 1),
            Commit(txId, ticks: 21),
        ]);
        if (!sourceVersionsReadable)
        {
            ReportNoStandaloneSourceVersions(h);
        }

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.DrainAsync());
        var atomicBeforeReplay = view.AtomicOperations.Count;
        var rowsAfterFlush = new Dictionary<string, byte[]>(view.Rows);

        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(rowsAfterFlush[Key], Is.EqualTo(new byte[] { 3 }), "the lost-response flush landed the slice");
            Assert.That(view.DeletedKeys, Does.Not.Contain(Key),
                "the replayed older range delete must not remove the batch's k beside its j = 4: a slice that never existed at the source");
            Assert.That(view.AtomicOperations.Skip(atomicBeforeReplay).Where(id => id.StartsWith("mv-reassert-", StringComparison.Ordinal)), Is.Empty,
                "nothing overwrote the slice, so it already matches and needs no repair");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }));
        });
    }

    [Test]
    public async Task A_cross_tree_reactivation_replay_never_rewrites_the_jointly_flipped_slice_non_atomically(
        [Values] bool sourceVersionsReadable)
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        var peer = new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection());
        var txId = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(txId, OtherKey, 1, ticks: 20, size: 2, index: 0),
            Ordinary(2, ticks: 10),
            Prepared(txId, Key, 3, ticks: 20, size: 2, index: 1),
            CrossTreeCommit(txId, ticks: 21),
        ];

        // First activation: the joint flip lands the slice, then the activation
        // crashes before its checkpoint persists.
        var checkpoint = new FakePersistentState<ViewCheckpointState>();
        ViewCheckpointState durable = new();
        checkpoint.OnWriteState = written => durable = CloneCheckpoint(written);
        checkpoint.BeforeWrite = () =>
        {
            if (coordinator.Applied)
            {
                throw new InvalidOperationException("simulated crash after the joint flip before completion persists");
            }

            return Task.CompletedTask;
        };
        var first = Create(
            view,
            checkpoint,
            peer,
            coordinator.Grain);
        ArmLog(first, log);
        if (!sourceVersionsReadable)
        {
            ReportNoStandaloneSourceVersions(first);
        }

        Assert.ThrowsAsync<InvalidOperationException>(() => first.Grain.DrainAsync());
        Assert.That(coordinator.Applied, Is.True, "the first activation's registration performed the joint flip");
        var pointWritesBeforeReplay = view.PointWrites.Count;
        var atomicBeforeReplay = view.AtomicOperations.Count;

        // A fresh activation replays the whole pass; the coordinator answers the
        // redelivered registration with its memoized decision and writes nothing.
        var second = Create(view, new FakePersistentState<ViewCheckpointState> { State = CloneCheckpoint(durable) }, peer, coordinator.Grain);
        ArmLog(second, log);
        if (!sourceVersionsReadable)
        {
            ReportNoStandaloneSourceVersions(second);
        }

        await second.Grain.DrainAsync();

        Assert.That(second.State.State.CrossTreeRepairHolds, Has.Count.EqualTo(1),
            "The completed repair still owns replay until the ordinary checkpoint retires.");
        Assert.That(second.State.State.CrossTreeRepairHolds[CrossTreeOperationId].RepairApplied, Is.True);
        await second.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.PointWrites.Skip(pointWritesBeforeReplay), Is.Empty,
                "the replay must not re-apply the older ordinary write over the jointly flipped slice: a reader would observe k = 2 beside j = 1");
            Assert.That(view.AtomicOperations.Skip(atomicBeforeReplay).Where(id => id.StartsWith("mv-reassert-", StringComparison.Ordinal)), Is.Empty,
                "nothing overwrote the slice, so it already matches and needs no repair");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 1 }));
        });
    }
}