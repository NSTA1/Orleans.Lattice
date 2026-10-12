using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// A reactivation whose replay fence is still unpassed when every re-staged
/// batch resolves. The fenced pass marks an already-flushed batch resolved from
/// its fenced prepare and stops reading below the fence head (a bounded drain
/// window); before the fix the emptied staging buffer cleared the fence anyway,
/// so the next pass re-applied an older ordinary write logged inside the old
/// fence while ignoring the flushed batch's terminal, leaving the view
/// permanently behind the source.
/// </summary>
public partial class ViewMaintainerReplayFlushTests
{
    /// <summary>
    /// Like <see cref="ArmLog"/>, but each drain reads at most the next window of
    /// entries (the last window repeats), the bounded per-pass read of the real
    /// subscriber.
    /// </summary>
    private static void ArmWindowedLog(Harness h, IReadOnlyList<LatticeMutation> log, params int[] windows)
    {
        SeedSource(h, log);
        var pass = 0;
        h.Subscriber
            .DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var drainContext = call.ArgAt<WalSubscriptionContext>(0);
                h.Contexts.Add(drainContext);
                var handler = call.ArgAt<IWalSubscriptionHandler>(1);
                var from = drainContext.Checkpoints.GetValueOrDefault(0, -1) + 1;
                h.ReadFrom.Add(from);
                var window = windows[Math.Min(pass++, windows.Length - 1)];
                var to = Math.Min(log.Count, from + window);
                var highest = HybridLogicalClock.Zero;
                for (var offset = from; offset < to; offset++)
                {
                    handler.OnEntry(new WalSubscriptionEntry(0, offset, log[(int)offset]));
                    highest = log[(int)offset].Timestamp;
                }

                return Task.FromResult(new WalDrainResult
                {
                    EntriesRead = Math.Max(0, to - from),
                    HighestTimestamp = highest,
                    AdvancedOffsets = from < to
                        ? new Dictionary<int, long> { [0] = to - 1 }
                        : new Dictionary<int, long>(),
                });
            });
    }

    [Test]
    public async Task A_replay_fence_left_unpassed_when_staging_empties_still_fences_the_rest_of_the_old_pass()
    {
        var view = new FakeViewTree();
        var coordinator = new PeerGatedCoordinator(view).Arm();
        var state = new FakePersistentState<ViewCheckpointState>();
        var peer = new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection());

        // A cross-tree batch t1 (waiting on its peer), then a flushed batch t2 on k,
        // then an older ordinary write to k the WAL logged after it. The source
        // last-writer of k is t2 (HLC 20 over 10).
        var t1 = Guid.NewGuid();
        var t2 = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(t1, OtherKey, 1, ticks: 5, size: 1, index: 0),
            CrossTreeCommit(t1, ticks: 6),
            Prepared(t2, 3, ticks: 20),
            Commit(t2, ticks: 21),
            Ordinary(2, ticks: 10),
        ];

        // First activation reads the whole log: t2 flushes (k = 3, the older
        // ordinary is dropped beneath it), t1 stays staged, so the checkpoint is
        // held below t1 and the replay fence records the read head with t1 staged.
        var first = Create(view, state, peer, coordinator.Grain);
        ArmLog(first, log);
        await first.Grain.DrainAsync();
        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(state.State.ReplayFenceOffsets?.GetValueOrDefault(0, -1), Is.EqualTo(4));
        });

        // The peer registers and the view reactivates. Its first pass reads only
        // up to t2's fenced prepare: t1 re-stages and resolves through the joint
        // flip, t2 is marked resolved from its fenced prepare, and the pass stops
        // below the fence head with nothing staged.
        coordinator.PeerRegistered = true;
        var second = Create(view, state, peer, coordinator.Grain);
        ArmWindowedLog(second, log, 3, 100);
        await second.Grain.DrainAsync();
        Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 1 }), "t1 resolved through the joint flip");
        Assert.That(state.State.ReplayFenceOffsets?.GetValueOrDefault(0, -1), Is.EqualTo(4),
            "resolving every staged batch cannot retire the durable fence before its unread suffix is consumed");

        // Later passes re-read the rest of the old pass; the ordinary write at
        // offset 4 was already applied (and superseded) and must stay fenced.
        await second.Grain.DrainAsync();
        await second.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(second.ReadFrom[1], Is.EqualTo(3), "the second pass resumed below the old fence head");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }),
                "the view must keep the source last-writer (t2), not the older ordinary write re-read inside the old fence");
            Assert.That(state.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(4));
            Assert.That(state.State.ReplayFenceOffsets, Is.Null, "the fence clears once the read position passes it");
        });
    }
}
