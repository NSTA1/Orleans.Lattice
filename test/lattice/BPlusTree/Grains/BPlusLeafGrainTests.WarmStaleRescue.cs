using NSubstitute;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    private static FieldInfo WarmRescueField(string name) =>
        typeof(BPlusLeafGrain).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException($"Missing leaf field {name}.");

    [TestCase(false)]
    [TestCase(true)]
    public async Task Warm_stale_rescue_holds_topology_gate_through_capture_and_checkpoint_persist(bool unseal)
    {
        var wal = new GrowingWal();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, state, _, _) = CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store);
        wal.GrowTo(3);
        await ActivateAsync(grain);
        stale = true;
        var saving = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<LeafSnapshotSaveOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        store.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                saving.TrySetResult();
                return release.Task;
            });
        var gate = (SemaphoreSlim)WarmRescueField("_splitGate").GetValue(grain)!;
        var heldAtPersist = false;
        state.OnWriteState = _ => heldAtPersist = gate.CurrentCount == 0;
        var drive = grain.DriveStarvedCheckpointAsync();
        await saving.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var seal = unseal ? grain.UnmarkSlotsMovedAwayAsync([0], 1) : grain.MarkSlotsMovedAwayAsync([0], 1);
        try
        {
            Assert.That(seal.IsCompleted, Is.False, "Seal operations must queue behind the rescue's topology gate.");
        }
        finally
        {
            release.TrySetResult(LeafSnapshotSaveOutcome.Kept);
        }
        Assert.That(await drive, Is.EqualTo(LeafStarvationDriveOutcome.Lifted));
        Assert.That(heldAtPersist, Is.True);
        await seal;
    }

    [TestCase("UnprovenBaseline")]
    [TestCase("CacheRehydratedOrReset")]
    [TestCase("TopologyChanged")]
    [TestCase("ReplayIncomplete")]
    [TestCase("UnknownPartition")]
    [TestCase("CheckpointUnproven")]
    [TestCase("ActivationAnchorAhead")]
    [TestCase("WalGapBeyondCache")]
    [TestCase("PendingTransactions")]
    [TestCase("MutationInFlight")]
    [TestCase("CaptureInFlight")]
    [TestCase("SplitInFlight")]
    [TestCase("SplitInFlight", true)]
    [TestCase("RetiredOrSealed")]
    [TestCase("CaptureDeclined")]
    [TestCase("StorageFailure")]
    public async Task Warm_stale_rescue_declines_without_advancing_durability(string reasonName, bool interruptedSplit = false)
    {
        var reason = Enum.Parse<WarmRescueDeclineReason>(reasonName);
        var wal = new GrowingWal();
        var logger = Substitute.For<ILogger<BPlusLeafGrain>>();
        var store = Substitute.For<ILeafSnapshotStorageGrain>();
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale
                ? FallOffLogDecision.SnapshotThenWal : FallOffLogDecision.TailReplay));
        var (grain, state, published, writes) =
            CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector, store, logger);
        wal.GrowTo(3);
        await ActivateAsync(grain);
        stale = true;
        wal.Coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(3L));
        SemaphoreSlim? heldGate = null;
        switch (reason)
        {
            case WarmRescueDeclineReason.UnprovenBaseline:
                WarmRescueField("_warmCacheProvenOffsets").SetValue(grain, null);
                break;
            case WarmRescueDeclineReason.CacheRehydratedOrReset:
                await grain.TryRehydrateFromSnapshotAsync(CancellationToken.None);
                break;
            case WarmRescueDeclineReason.TopologyChanged:
                await grain.SetNextSiblingAsync(null);
                break;
            case WarmRescueDeclineReason.ReplayIncomplete:
                WarmRescueField("_warmCacheReplayFailed").SetValue(grain, true);
                break;
            case WarmRescueDeclineReason.UnknownPartition:
                stale = false;
                wal.OnRead = () => throw new LeafProjectionStaleException("read refused");
                break;
            case WarmRescueDeclineReason.CheckpointUnproven:
                await ((ILeafProjection)grain).SetCheckpointOffsetAsync(4L, CancellationToken.None);
                break;
            case WarmRescueDeclineReason.ActivationAnchorAhead:
                WarmRescueField("_warmCacheOriginOffsets").SetValue(grain, new[] { 1L });
                break;
            case WarmRescueDeclineReason.WalGapBeyondCache:
                wal.Coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>())
                    .Returns(Task.FromResult(5L));
                break;
            case WarmRescueDeclineReason.PendingTransactions:
                state.State.UnresolvedReplayWork = [new()];
                break;
            case WarmRescueDeclineReason.MutationInFlight:
                WarmRescueField("_mutationsInFlight").SetValue(grain, 1);
                break;
            case WarmRescueDeclineReason.CaptureInFlight:
                WarmRescueField("_snapshotCaptureInFlight").SetValue(grain, true);
                break;
            case WarmRescueDeclineReason.SplitInFlight:
                if (interruptedSplit)
                    state.State.SplitInFlight = true;
                else
                {
                    heldGate = (SemaphoreSlim)WarmRescueField("_splitGate").GetValue(grain)!;
                    Assert.That(heldGate.Wait(0), Is.True);
                }
                break;
            case WarmRescueDeclineReason.RetiredOrSealed:
                state.State.MovedAwaySlots = [0];
                break;
            case WarmRescueDeclineReason.CaptureDeclined:
                store.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                    .Returns(Task.FromResult(LeafSnapshotSaveOutcome.Declined));
                break;
            case WarmRescueDeclineReason.StorageFailure:
                store.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                    .Returns(Task.FromException<LeafSnapshotSaveOutcome>(new IOException("store unavailable")));
                break;
        }
        var checkpoint = state.State.ProjectionCheckpointOffset;
        var coverage = grain.DurableSnapshotCoverageForPartition(0);
        writes.Clear();
        published.Clear();
        store.ClearReceivedCalls();
        try
        {
            Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
            Assert.Multiple(() =>
            {
                Assert.That(grain.LastWarmRescueDecline, Is.EqualTo(reason));
                Assert.That(writes, Is.Empty);
                Assert.That(published, Is.Empty);
                Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(checkpoint));
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(coverage));
                Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.True);
            });
            if (reason is not (WarmRescueDeclineReason.CaptureDeclined or WarmRescueDeclineReason.StorageFailure))
                await store.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
            WarmRescueField("_projectionStaleDriveLatch").SetValue(grain, null);
            Assert.ThrowsAsync<LeafProjectionStaleException>(() => grain.DriveStarvedCheckpointAsync());
            Assert.That(logger.ReceivedCalls().Count(call =>
                call.GetMethodInfo().Name == "Log"
                && call.GetArguments()[2]?.ToString()?.Contains("Warm stale-leaf rescue declined", StringComparison.Ordinal) == true),
                Is.EqualTo(1), "A repeating decline reason must be logged only once.");
        }
        finally
        {
            heldGate?.Release();
        }
    }

    [TestCase("tree-3454")]
    [TestCase("_lattice_repocontext_vectors-3454")]
    public async Task Warm_stale_rescue_captures_proven_cache_and_advances_durable_pin(string treeId)
    {
        var wal = new GrowingWal();
        var coordinatorKeys = new List<string>();
        var durablePinOffset = 0L;
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        var stale = false;
        detector.ClassifyAsync(
                Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(),
                Arg.Any<TimeSpan>(), Arg.Any<ResolvedLatticeOptions>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(stale
                ? FallOffLogDecision.SnapshotThenWal
                : FallOffLogDecision.TailReplay));
        var (grain, state, published, writes) =
            CreateCoalescingLeafWithPinCapture(wal.Coordinator, detector,
                onCoordinatorLookup: coordinatorKeys.Add, onDurablePinFlushed: offset => durablePinOffset = offset);
        state.State.TreeId = treeId;
        wal.GrowTo(3);
        await ActivateAsync(grain);
        coordinatorKeys.Clear();
        WarmRescueField("_durableFrontierBarriered").SetValue(grain, true);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero);
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3));
        published.Clear();
        writes.Clear();
        stale = true;
        wal.Coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(3L));

        var provider = new InMemoryWalStorageProvider();
        var timestamp = new HybridLogicalClock { WallClockTicks = 100 };
        await provider.AppendBatchAsync(treeId, 0,
            Enumerable.Range(0, 4).Select(offset => new WalEntry
            {
                Offset = offset,
                Mutation = new LatticeMutation
                {
                    TreeId = treeId, Kind = MutationKind.Set, Key = $"k{offset}",
                    Value = [1], Timestamp = timestamp, OriginClusterId = "test",
                },
            }).ToArray(), CancellationToken.None);
        await provider.TrimAsync(treeId, 0, 2, CancellationToken.None);
        var consumer = $"_lattice_materialiser_{treeId}_leaf";
        var pins = Substitute.For<IWalMaterialiserPinGrain>();
        pins.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock> { [consumer] = timestamp }));
        pins.GetPinOffsetsAsync().Returns(_ => Task.FromResult<IReadOnlyDictionary<string, long>>(
            new Dictionary<string, long> { [consumer] = durablePinOffset }));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pins);
        var services = new ServiceCollection().AddSingleton<IWalStorageProvider>(provider)
            .AddSingleton(factory).BuildServiceProvider();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(treeId, consumer, timestamp);
        await registry.ReportCursorAsync(treeId, "shipper", timestamp);
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalPartitions = 1 });
        var gc = new LatticeWalGc(services, registry, options);
        Assert.That((await gc.RunOnceAsync(treeId)).EntriesTrimmed, Is.Zero);

        var outcome = LeafStarvationDriveOutcome.NotDriven;
        LeafProjectionStaleException? staleFault = null;
        try
        {
            outcome = await grain.DriveStarvedCheckpointAsync();
        }
        catch (LeafProjectionStaleException fault)
        {
            staleFault = fault;
        }
        var sweep = await gc.RunOnceAsync(treeId);

        Assert.Multiple(() =>
        {
            Assert.That(staleFault, Is.Null);
            Assert.That(outcome, Is.EqualTo(LeafStarvationDriveOutcome.Lifted));
            Assert.That(sweep.EntriesTrimmed, Is.EqualTo(1), "The real sweep must reclaim the formerly pinned WAL suffix.");
            Assert.That(writes, Does.Contain(3L));
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L));
            Assert.That(grain.IsProjectionStaleDriveLatched(1), Is.False);
            Assert.That(published, Is.Not.Empty);
            Assert.That(published.LastOrDefault().PublishedOffset, Is.EqualTo(3L));
            Assert.That(coordinatorKeys, Is.Not.Empty);
            Assert.That(coordinatorKeys, Is.All.EqualTo($"{treeId}/0"));
        });
    }
}
