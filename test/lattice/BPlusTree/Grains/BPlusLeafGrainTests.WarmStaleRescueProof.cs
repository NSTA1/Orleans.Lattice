using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase("complete")]
    [TestCase("partial-snapshot")]
    [TestCase("second-partition-gap")]
    public async Task Warm_stale_rescue_requires_every_partition_to_be_proven(string scenario)
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(stale && call.ArgAt<int>(1) == 1
                ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, _, published, writes) =
            CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store, partitionCount: 2);
        if (scenario == "partial-snapshot")
            store.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(new()
            {
                SnapshotOffset = 0,
                SnapshotOffsetsByPartition = [0, -1],
                Rows = [],
                CapturedAtTicks = 1,
            }));
        LeafSnapshotBlob? captured = null;
        store.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            captured = call.ArgAt<LeafSnapshotBlob>(0);
            return Task.FromResult(LeafSnapshotSaveOutcome.Kept);
        });
        wal.GrowTo(3);
        await ActivateAsync(grain);
        stale = true;
        var probes = 0;
        wal.Coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            Task.FromResult(++probes == 2 && scenario == "second-partition-gap" ? 5L : 3L));
        writes.Clear();
        published.Clear();
        captured = null;
        if (scenario == "complete")
        {
            Assert.That(await grain.DriveStarvedCheckpointAsync(), Is.EqualTo(LeafStarvationDriveOutcome.Lifted));
            Assert.That(captured, Is.Not.Null);
            Assert.That(captured!.SnapshotOffsetsByPartition, Is.EqualTo(new[] { 3L, 3L }));
            Assert.That(published.Select(pin => pin.PublishedOffset), Is.All.EqualTo(3L));
            var (cold, _, _, _) =
                CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store, partitionCount: 2);
            store.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(captured));
            stale = false;
            await ActivateAsync(cold);
            Assert.That(cold.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k1", "k2", "k3" }));
            Assert.That(cold.GetCurrentCheckpointForPartition(1), Is.EqualTo(3L));
        }
        else
        {
            Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
            Assert.Multiple(() =>
            {
                Assert.That(grain.LastWarmRescueDecline, Is.EqualTo(scenario == "partial-snapshot"
                    ? WarmRescueDeclineReason.UnprovenBaseline : WarmRescueDeclineReason.WalGapBeyondCache));
                Assert.That(captured, Is.Null);
                Assert.That(writes, Is.Empty);
                Assert.That(published, Is.Empty);
            });
        }
    }

    [Test]
    public async Task Warm_stale_rescue_detached_snapshot_does_not_hold_the_drive_or_publish_after_timeout()
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, _, published, writes) =
            CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store, driveBudget: TimeSpan.FromSeconds(2));
        wal.GrowTo(3);
        await ActivateAsync(grain);
        stale = true;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<LeafSnapshotSaveOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>()).Returns(_ =>
        {
            entered.TrySetResult();
            return release.Task;
        });
        writes.Clear();
        published.Clear();
        var drive = grain.DriveStarvedCheckpointAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var gate = (SemaphoreSlim)WarmRescueField("_splitGate").GetValue(grain)!;
        try
        {
            Assert.That(await drive.WaitAsync(TimeSpan.FromSeconds(10)), Is.EqualTo(LeafStarvationDriveOutcome.TimedOut));
            Assert.That(await grain.DriveStarvedCheckpointAsync(), Is.EqualTo(LeafStarvationDriveOutcome.AlreadyDriving));
            Assert.That(gate.CurrentCount, Is.Zero);
        }
        finally
        {
            release.TrySetResult(LeafSnapshotSaveOutcome.Kept);
            await gate.WaitAsync(TimeSpan.FromSeconds(10));
            gate.Release();
        }
        Assert.That(published, Is.Empty);
        Assert.That(writes, Is.Empty);
    }

    [Test]
    public async Task Warm_stale_rescue_propagates_checkpoint_failure_after_kept_snapshot_without_publishing_pin()
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, state, published, writes) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store);
        wal.GrowTo(3);
        await ActivateAsync(grain);
        stale = true;
        writes.Clear();
        published.Clear();
        state.ThrowOnWrite = new IOException("checkpoint unavailable");
        Assert.ThrowsAsync<IOException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L));
        Assert.That(published, Is.Empty);
        Assert.That(writes, Is.Empty);
        Assert.That(((SemaphoreSlim)WarmRescueField("_splitGate").GetValue(grain)!).CurrentCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Warm_stale_rescue_waits_for_the_durable_pin_acknowledgement()
    {
        var wal = new GrowingWal();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grain, _, _, _) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector,
            beforePinFlush: () =>
            {
                if (!stale)
                    return Task.CompletedTask;
                entered.TrySetResult();
                return release.Task;
            });
        wal.GrowTo(3);
        await ActivateAsync(grain);
        WarmRescueField("_durableFrontierBarriered").SetValue(grain, true);
        stale = true;
        var drive = grain.DriveStarvedCheckpointAsync();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(drive.IsCompleted, Is.False);
        }
        finally
        {
            release.TrySetResult();
        }
        Assert.That(await drive, Is.EqualTo(LeafStarvationDriveOutcome.Lifted));
    }

    [Test]
    public async Task Warm_stale_rescue_excludes_a_normal_capture_already_past_its_entry_guard()
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var (grain, state, _, _) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, snapshotStore: store);
        wal.GrowTo(3);
        await ActivateAsync(grain);
        store.ClearReceivedCalls();
        WarmRescueField("_lastStaleReplayPartition").SetValue(grain, 0);
        var crossedEntryGuard = false;
        // The first state read follows the early guard but precedes the options
        // await and capture admission. Model rescue starting in that interval.
        state.OnStateAccess = () =>
        {
            crossedEntryGuard = true;
            WarmRescueField("_warmRescueInFlight").SetValue(grain, true);
        };
        try
        {
            await grain.CaptureSnapshotAsync();
            Assert.That(crossedEntryGuard, Is.True);
            await store.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        }
        finally
        {
            state.OnStateAccess = null;
            WarmRescueField("_warmRescueInFlight").SetValue(grain, false);
        }
    }

    [Test]
    public async Task Warm_stale_rescue_declines_a_cold_origin_without_a_snapshot_anchor()
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, state, published, writes) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store);
        store.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        wal.GrowTo(3);
        wal.Coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(Enumerable.Range(0, 4)
                .Where(offset => offset > call.ArgAt<long>(0) && offset <= call.ArgAt<long>(1))
                .Select(offset => new CommitLogSliceEntry(offset, BuildCommittedSet($"k{offset}", [(byte)offset])))
                .ToArray()));
        await ActivateAsync(grain);
        var checkpoint = state.State.ProjectionCheckpointOffset;
        var coverage = grain.DurableSnapshotCoverageForPartition(0);
        stale = true;
        writes.Clear();
        published.Clear();
        store.ClearReceivedCalls();
        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.Multiple(() =>
        {
            Assert.That(grain.LastWarmRescueDecline, Is.EqualTo(WarmRescueDeclineReason.UnprovenBaseline));
            Assert.That(writes, Is.Empty);
            Assert.That(published, Is.Empty);
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(checkpoint));
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(coverage));
        });
        await store.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }

    [TestCase("tree-3454")]
    [TestCase("_lattice_repocontext_vectors-3454")]
    public async Task Warm_stale_rescue_does_not_certify_a_replay_that_skipped_a_missing_offset(string treeId)
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, state, published, writes) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store);
        state.State.TreeId = treeId;
        wal.GrowTo(3);
        wal.Coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(call.ArgAt<long>(0) >= 3
                ? []
                : [
                    new(2, BuildCommittedSet("k2", [2])),
                    new(3, BuildCommittedSet("k3", [3])),
                ]));
        await ActivateAsync(grain);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3));
        stale = true;
        writes.Clear();
        published.Clear();
        store.ClearReceivedCalls();
        Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
        Assert.Multiple(() =>
        {
            Assert.That(grain.LastWarmRescueDecline, Is.EqualTo(WarmRescueDeclineReason.UnprovenBaseline));
            Assert.That(writes, Is.Empty);
            Assert.That(published, Is.Empty);
        });
        await store.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
    }
}
