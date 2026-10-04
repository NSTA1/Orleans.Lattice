using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4523: a never-written leaf must not be bricked by a two-fault
/// composition of its #3453 release with a later cold-rebuild capture.
/// </summary>
/// <remarks>
/// <para>
/// <b>The trace</b> (the WAL durability TLA+ model, <c>RecoveryNeverFallsOffLog</c>
/// at <c>MaxFaults == 2</c>). Activation A of a leaf that owns nothing in the
/// partition scans it through a persisted checkpoint <c>X</c> with no snapshot
/// (its capture fails) and publishes the never-written release. Fault one ends A.
/// Activation B starts cold, and a capture taken part-way through its rebuild (or
/// the #2280 bank of an interrupted one) claims only its re-read frontier
/// <c>F &lt; X</c>, which the store keeps. The WAL GC trims through everything the
/// pin store holds. Fault two ends B. Activation C rehydrates the low snapshot,
/// restarts from <c>F</c>, finds the WAL trimmed past <c>F + 1</c> and latches
/// <see cref="LeafProjectionStaleException"/> over a prefix the leaf never owned.
/// </para>
/// <para>
/// <b>The observable.</b> The pin store is the monotonic maximum of every offset
/// any activation published, and the GC is entitled to trim through it; the
/// property is that activation C, against that trimmed WAL, comes up healthy.
/// The stronger invariant behind it - every published trim entitlement is backed
/// by a durable snapshot that covers it - is asserted alongside, for every
/// snapshot activation B left in the store.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>How activation B's low-coverage snapshot is taken.</summary>
    public enum ColdLowClaim
    {
        /// <summary>An ordinary capture part-way through the cold rebuild (issue #4451's claim).</summary>
        MidRebuildCapture,

        /// <summary>The #2280 bank of a cold rebuild cut short by cancellation.</summary>
        CancelledRebuildBank,
    }

    [TestCase(ColdLowClaim.MidRebuildCapture)]
    [TestCase(ColdLowClaim.CancelledRebuildBank)]
    public async Task Never_written_leaf_is_not_latched_stale_by_a_cold_capture_below_its_published_release(ColdLowClaim how)
    {
        var pinStore = new List<long>();

        // Activation A: never-written, no snapshot (every capture faults), and a
        // drive scans the partition through the persisted checkpoint X = 3.
        var wal = new GrowingWal();
        var (first, firstState, firstPublished) = CreateNeverWrittenLeafWithPinCapture(
            wal.Coordinator, failSnapshotCapture: true);
        await ActivateAsync(first);
        wal.GrowTo(3);
        await first.DriveStarvedCheckpointAsync();
        pinStore.AddRange(firstPublished.Select(p => p.PublishedOffset));

        var persisted = firstState.State.ProjectionCheckpointOffset;
        Assert.Multiple(() =>
        {
            Assert.That(firstState.State.Clock, Is.EqualTo(HybridLogicalClock.Zero), "precondition: never-written.");
            Assert.That(first.EntriesForTest, Is.Empty, "precondition: every entry belonged to another leaf.");
            Assert.That(persisted, Is.EqualTo(3L), "precondition: the drive persisted the scanned-through X.");
        });

        // Fault one: A is gone. Activation B starts cold from the persisted X over
        // the same WAL and re-reads it one entry per read.
        var entries = new List<CommitLogSliceEntry>();
        for (var offset = 1L; offset <= 3; offset++)
            entries.Add(new CommitLogSliceEntry(offset, BuildCommittedSet($"k{offset}", Encoding.UTF8.GetBytes($"v{offset}"))));

        using var cts = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        async Task<IReadOnlyList<CommitLogSliceEntry>> Serve(long from, long to, int budget, WalKeyFilter? filter)
        {
            if (++reads == 2)
            {
                if (how == ColdLowClaim.CancelledRebuildBank)
                {
                    cts.Cancel();
                }
                else
                {
                    entered.TrySetResult();
                    await release.Task;
                }
            }

            var width = Math.Min(budget, 1);
            return filter is { } f
                ? ReplaySliceStub.Filtered(entries, from, to, width, f)
                : ReplaySliceStub.Unfiltered(entries, from, to, width);
        }

        var coldWal = Substitute.For<ILeafReplayCoordinatorGrain>();
        coldWal.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(4L));
        coldWal.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(1L));
        coldWal.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Serve(call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), null));
        coldWal.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(call => Serve(call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), call.ArgAt<WalKeyFilter>(3)));

        var kept = new List<LeafSnapshotBlob>();
        var keptAgainstPinStore = new List<(long Coverage, long PinStore)>();
        var (second, _, secondPublished) = CreateNeverWrittenLeafWithPinCapture(
            coldWal, persistedCheckpoint: persisted, keptSnapshots: kept);

        if (how == ColdLowClaim.CancelledRebuildBank)
        {
            Assert.ThrowsAsync<OperationCanceledException>(
                async () => await LeafActivationHarness.ActivateAsync(second, cts.Token),
                "precondition: the cold rebuild was cut short, which is what the #2280 bank answers.");
        }
        else
        {
            await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);
            _ = await second.GetTreeIdAsync();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await second.CaptureSnapshotAsync();
        }

        // Fault two: B is gone, holding whatever the store kept.
        var keptByB = kept.ToList();
        release.TrySetResult();
        pinStore.AddRange(secondPublished.Select(p => p.PublishedOffset));
        var pinFloor = pinStore.Count == 0 ? -1L : pinStore.Max();

        foreach (var blob in keptByB)
            keptAgainstPinStore.Add((blob.SnapshotOffsetsByPartition![0], pinFloor));

        // The GC trims through everything the pin store holds.
        var tail = pinFloor + 1;
        var survivors = entries.Where(e => e.Offset >= tail).ToArray();
        var trimmed = BuildCoordinator(4L, survivors);
        trimmed.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(tail));
        var reader = Substitute.For<ICommitLogReader>();
        reader.GetHeadOffsetAsync(NeverWrittenTreeId, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(4L));
        reader.GetTailOffsetAsync(NeverWrittenTreeId, 0, Arg.Any<CancellationToken>()).Returns(Task.FromResult(tail));
        var detector = new LatticeFallOffLogDetector(
            new ServiceCollection().AddSingleton<ICommitLogReader>(reader).BuildServiceProvider());

        var storeHolds = keptByB.Count == 0
            ? null
            : keptByB.MaxBy(b => b.SnapshotOffsetsByPartition![0]);
        var (third, _, _) = CreateNeverWrittenLeafWithPinCapture(
            trimmed, persistedCheckpoint: persisted, detector: detector, snapshot: storeHolds);

        Assert.Multiple(() =>
        {
            Assert.That(keptAgainstPinStore.Where(k => k.Coverage < k.PinStore), Is.Empty,
                "every trim entitlement the pin store holds must be backed by the durable snapshot the next "
                    + "activation rehydrates. Kept (coverage, pin store): "
                    + string.Join(", ", keptAgainstPinStore));
            Assert.DoesNotThrowAsync(async () => await ActivateAsync(third),
                $"the leaf owns nothing in the partition, yet a release at {pinFloor} let the GC trim to tail "
                    + $"{tail} while the store held a snapshot covering only "
                    + $"{(storeHolds is null ? "nothing" : storeHolds.SnapshotOffsetsByPartition![0].ToString())}.");
        });
    }
}
