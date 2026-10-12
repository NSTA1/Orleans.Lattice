using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// A cross-tree batch the joint flip resolves in a pass that reads no new
/// entries. Its staged prepares held the checkpoint back below an older
/// ordinary write that the WAL logged between them; the resolving pass read
/// nothing, so the drain reported no advanced offset, the checkpoint stayed
/// held back, and the emptied staging buffer cleared the replay fence. The next
/// pass re-read from the held-back checkpoint, skipped the resolved batch, and
/// re-applied the older ordinary write over the jointly flipped value, leaving
/// the view permanently behind the source.
/// </summary>
public partial class ViewMaintainerReplayFlushTests
{
    private const string PeerViewName = "peer-view";
    private const string PeerSourceTreeId = "peer-src";
    private const string CrossTreeOperationId = "xt-op";

    private static LatticeMutation CrossTreeCommit(Guid txId, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.TxCommit,
        Key = string.Empty,
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
        TransactionId = txId,
        CrossTreeOperationId = CrossTreeOperationId,
        CrossTreeParticipants = [SourceTreeId, PeerSourceTreeId],
    };

    /// <summary>
    /// A coordinator that reports the wait set incomplete until the peer view
    /// registers, then performs the joint flip of the slice this view registered
    /// (as the real coordinator does) and reports it committed.
    /// </summary>
    private sealed class PeerGatedCoordinator(FakeViewTree view)
    {
        public bool PeerRegistered { get; set; }

        public int Registrations { get; private set; }

        public IViewCrossTreeCoordinatorGrain Grain { get; } = Substitute.For<IViewCrossTreeCoordinatorGrain>();

        public PeerGatedCoordinator Arm()
        {
            Grain.RegisterReadyAsync(Arg.Any<ViewCrossTreeReadiness>()).Returns(call =>
            {
                Registrations++;
                if (!PeerRegistered)
                {
                    return Task.FromResult(ViewCrossTreeDecision.NotReady);
                }

                var readiness = call.ArgAt<ViewCrossTreeReadiness>(0);
                foreach (var (key, value) in readiness.Upserts)
                {
                    view.Rows[key] = value;
                }

                foreach (var key in readiness.Deletes)
                {
                    view.Rows.Remove(key);
                }

                return Task.FromResult(ViewCrossTreeDecision.Committed);
            });
            return this;
        }
    }

    [Test]
    public async Task A_cross_tree_batch_resolved_by_a_pass_that_reads_nothing_is_not_overwritten_by_a_later_replay_of_an_older_ordinary_write()
    {
        var view = new FakeViewTree();
        var coordinator = new PeerGatedCoordinator(view).Arm();
        var h = Create(
            view,
            new FakePersistentState<ViewCheckpointState>(),
            peer: new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection()),
            coordinator: coordinator.Grain);

        // The batch's first prepare is logged before an older ordinary write to the
        // key its second prepare overwrites; the source last-writer is the batch.
        var txId = Guid.NewGuid();
        ArmLog(h,
        [
            Prepared(txId, OtherKey, 1, ticks: 20, size: 2, index: 0),
            Ordinary(2, ticks: 10),
            Prepared(txId, Key, 3, ticks: 20, size: 2, index: 1),
            CrossTreeCommit(txId, ticks: 21),
        ]);

        // Report no source versions (the fail-open read), so the older ordinary
        // write is not dropped against the source (#4885) and this fixture keeps
        // isolating the read-position fence it exists to pin.
        ReportNoStandaloneSourceVersions(h);

        // Pass 1 reads the whole log and registers the completed batch; the peer has
        // not registered, so the batch stays staged and holds the checkpoint back
        // below its first prepare. The older ordinary write is held off by the
        // committed batch that overwrites it (issue #4869), so the view keeps the
        // pre-batch state of the slice while the joint flip is pending.
        await h.Grain.DrainAsync();
        Assert.That(view.Rows, Does.Not.ContainKey(Key), "the batch is still pending its joint flip");

        // The peer registers. Pass 2 reads nothing new; its re-registration observes
        // the joint flip, which writes the batch slice, and resolves the batch.
        coordinator.PeerRegistered = true;
        await h.Grain.DrainAsync();
        Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }), "the joint flip landed the batch slice");

        // Pass 3 must not re-read below the resolved batch and re-apply the older
        // ordinary write over the jointly flipped value.
        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.ReadFrom[1], Is.EqualTo(4), "pass 2 resumed at the staged read position and read nothing");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }),
                "the view must keep the source last-writer (the jointly flipped batch value), not a replayed older ordinary value");
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 1 }));
            Assert.That(h.State.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(3),
                "once the batch resolved the checkpoint must cover everything the drains read");
        });
    }
}
