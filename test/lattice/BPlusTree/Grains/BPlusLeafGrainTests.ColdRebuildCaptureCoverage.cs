using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issues #4451 and #4467: a leaf whose cache does not
/// yet provably hold its checkpointed prefix - an unconverged cold rebuild - must
/// neither claim the persisted checkpoint as snapshot coverage nor resume warm.
/// <para>
/// #4451: <c>CaptureSnapshotCoreAsync</c> stamped <c>max(persisted, pending)</c>
/// over a cache that held only what the cold rebuild had re-read. The store kept
/// the non-regressing claim, the durable pin rose to it, and the WAL GC was
/// licensed to trim rows that existed nowhere else. Found by the WAL durability
/// TLA+ model (mutation <c>TrimCoveredBySnapshotColdCaptureOverclaims</c>).
/// </para>
/// <para>
/// #4467: a cold rebuild that FAULTED part-way left a partial cache, so the
/// re-armed replay saw a non-empty cache, took the warm path and resumed from the
/// persisted checkpoint, skipping everything between the re-read frontier and the
/// checkpoint (mutation <c>ReadPositionHonestFaultedColdReplayResumesWarm</c>).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot, ILeafReplayCoordinatorGrain Coordinator)
        CreateColdRebuildLeaf(
            long persistedCheckpoint,
            long head,
            IReadOnlyList<CommitLogSliceEntry> entries,
            Func<int, Task>? onRead = null,
            int maxEntriesPerRead = int.MaxValue)
    {
        var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LeafSnapshotSaveOutcome.Kept));

        var reads = 0;
        async Task<IReadOnlyList<CommitLogSliceEntry>> Serve(long from, long to, int budget, WalKeyFilter? filter)
        {
            var read = ++reads;
            if (onRead is not null)
                await onRead(read);
            var width = Math.Min(budget, maxEntriesPerRead);
            return filter is { } f
                ? ReplaySliceStub.Filtered(entries, from, to, width, f)
                : ReplaySliceStub.Unfiltered(entries, from, to, width);
        }

        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
        coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => Serve(call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), null));
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(call => Serve(call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), call.ArgAt<WalKeyFilter>(3)));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshot);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<ICommitLogReader>());
        services.AddSingleton(Substitute.For<ILeafCursorReporter>());

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = MaterialiserTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        state.State.ProjectionCheckpointOffsetAssigned = true;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaterialiserCheckpointInterval = TimeSpan.FromHours(1),
                MaterialiserCheckpointEntries = 1_000_000,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, state, snapshot, coordinator);
    }

    private static CommitLogSliceEntry ColdRebuildEntry(long offset) =>
        new(offset, BuildCommittedSet($"k{offset}", Encoding.UTF8.GetBytes($"v{offset}")));

    private static IReadOnlyList<CommitLogSliceEntry> ColdRebuildEntries(long count)
    {
        var list = new List<CommitLogSliceEntry>();
        for (var offset = 0L; offset < count; offset++)
            list.Add(ColdRebuildEntry(offset));
        return list;
    }

    /// <summary>
    /// Fails when <paramref name="blob"/> claims coverage of an offset whose row it
    /// does not carry. Every entry these fixtures write is a distinct key
    /// <c>k{offset}</c>, so an honest claim <c>c</c> carries <c>k0..kc</c>.
    /// </summary>
    private static void AssertClaimHeldByRows(LeafSnapshotBlob blob)
    {
        var claim = blob.SnapshotOffsetsByPartition is { Length: > 0 } p ? p.Max() : blob.SnapshotOffset ?? -1L;
        var rows = new HashSet<string>();
        foreach (var row in blob.EnumerateRows())
            rows.Add(row.Key);

        for (var offset = 0L; offset <= claim; offset++)
        {
            Assert.That(rows, Does.Contain($"k{offset}"),
                $"The blob claims coverage {claim} but carries only [{string.Join(", ", rows.Order())}]; the pin "
                + "would license the WAL GC to trim rows that exist nowhere else.");
        }
    }

    private static List<LeafSnapshotBlob> RecordSaves(ILeafSnapshotStorageGrain snapshot)
    {
        var saved = new List<LeafSnapshotBlob>();
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                saved.Add(call.ArgAt<LeafSnapshotBlob>(0));
                return Task.FromResult(LeafSnapshotSaveOutcome.Kept);
            });
        return saved;
    }

    [Test]
    public async Task Capture_during_a_cold_rebuild_never_claims_coverage_its_rows_lack()
    {
        // No snapshot; persisted checkpoint 2; WAL [0, 2] intact. The cold replay
        // parks on its first read while a capture runs (issue #4451's repro).
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grain, _, snapshot, _) = CreateColdRebuildLeaf(
            persistedCheckpoint: 2, head: 3, entries: ColdRebuildEntries(3),
            onRead: async _ => { entered.TrySetResult(); await release.Task; });
        var saved = RecordSaves(snapshot);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        _ = await grain.GetTreeIdAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await grain.CaptureSnapshotAsync();
        var duringRebuild = saved.ToList();
        release.TrySetResult();
        await grain.ReplayBarrierForTest!.WaitAsync(TimeSpan.FromSeconds(10));

        foreach (var blob in duringRebuild)
            AssertClaimHeldByRows(blob);
    }

    [Test]
    public async Task Deactivation_capture_during_a_cold_rebuild_never_claims_coverage_its_rows_lack()
    {
        // The cold replay sets _cacheRebuiltFromWalStartThisActivation at the START
        // of the rebuild, which used to open the graceful-deactivation capture over
        // a cache holding nothing yet.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grain, _, snapshot, _) = CreateColdRebuildLeaf(
            persistedCheckpoint: 2, head: 3, entries: ColdRebuildEntries(3),
            onRead: async _ => { entered.TrySetResult(); await release.Task; });
        var saved = RecordSaves(snapshot);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        _ = await grain.GetTreeIdAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        var duringRebuild = saved.ToList();
        release.TrySetResult();

        foreach (var blob in duringRebuild)
            AssertClaimHeldByRows(blob);
    }

    [Test]
    public async Task Capture_while_the_snapshot_rehydrate_is_in_flight_never_claims_coverage_its_rows_lack()
    {
        // Before step 0.5 has decided anything the cache is empty while the
        // persisted checkpoint still reads 2.
        var load = new TaskCompletionSource<LeafSnapshotBlob?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grain, _, snapshot, _) = CreateColdRebuildLeaf(
            persistedCheckpoint: 2, head: 3, entries: ColdRebuildEntries(3));
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(load.Task);
        var saved = RecordSaves(snapshot);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        _ = await grain.GetTreeIdAsync();
        await Task.Yield();

        await grain.CaptureSnapshotAsync();
        var beforeAnchor = saved.ToList();
        load.TrySetResult(null);
        await grain.ReplayBarrierForTest!.WaitAsync(TimeSpan.FromSeconds(10));

        foreach (var blob in beforeAnchor)
            AssertClaimHeldByRows(blob);
    }

    [Test]
    public async Task Capture_part_way_through_a_cold_rebuild_banks_only_the_re_read_frontier()
    {
        // The rebuild has re-read k0 and parks on its second read. The only honest
        // claim is the re-read frontier, which banking stamps; the checkpoint (2)
        // would claim k1 and k2, which the cache does not hold yet.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (grain, _, snapshot, _) = CreateColdRebuildLeaf(
            persistedCheckpoint: 2, head: 3, entries: ColdRebuildEntries(3),
            onRead: async read =>
            {
                if (read < 2)
                    return;
                entered.TrySetResult();
                await release.Task;
            },
            maxEntriesPerRead: 1);
        var saved = RecordSaves(snapshot);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        _ = await grain.GetTreeIdAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await grain.CaptureSnapshotAsync();
        var duringRebuild = saved.ToList();
        release.TrySetResult();
        await grain.ReplayBarrierForTest!.WaitAsync(TimeSpan.FromSeconds(10));

        foreach (var blob in duringRebuild)
        {
            AssertClaimHeldByRows(blob);
            Assert.That(blob.SnapshotOffsetsByPartition![0], Is.LessThan(2L),
                "A mid-rebuild capture must never stamp the persisted checkpoint.");
        }
    }

    [Test]
    public async Task Capture_after_the_cold_rebuild_converges_claims_the_checkpoint()
    {
        // Liveness control: once the rebuild has read the whole window the cache
        // holds [0, checkpoint], and an ordinary capture stamps it as before.
        var (grain, _, snapshot, _) = CreateColdRebuildLeaf(
            persistedCheckpoint: 2, head: 3, entries: ColdRebuildEntries(3));
        var saved = RecordSaves(snapshot);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        saved.Clear();
        await grain.CaptureSnapshotAsync();

        Assert.That(saved, Has.Count.EqualTo(1), "A converged leaf must still capture.");
        Assert.That(saved[0].SnapshotOffsetsByPartition![0], Is.EqualTo(2L));
        AssertClaimHeldByRows(saved[0]);
    }

    [Test]
    public async Task A_cold_rebuild_that_faults_part_way_is_retried_cold_not_resumed_warm()
    {
        // Issue #4467. The rebuild re-reads k0, then its second read faults. The
        // partial cache must not pass for an anchor: the re-armed replay has to
        // re-read from the WAL start, or k1 and k2 - below the persisted
        // checkpoint 2 - are never rebuilt.
        var (grain, state, _, _) = CreateColdRebuildLeaf(
            persistedCheckpoint: 2, head: 3, entries: ColdRebuildEntries(3),
            onRead: read => read == 2
                ? Task.FromException(new TimeoutException("wal read transient"))
                : Task.CompletedTask,
            maxEntriesPerRead: 1);

        Assert.ThrowsAsync<TimeoutException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));
        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k0" }),
            "Precondition: the rebuild faulted part-way, leaving a partial cache.");

        // The first data operation observes the faulted replay and disarms it, as
        // in production; the next one re-arms the replay.
        Assert.ThrowsAsync<TimeoutException>(async () => await grain.GetAsync("k1"));
        var k1 = await grain.GetAsync("k1");

        Assert.Multiple(() =>
        {
            Assert.That(k1, Is.EqualTo(Encoding.UTF8.GetBytes("v1")),
                $"The re-armed replay resumed warm from the persisted checkpoint over a partial cache "
                + $"[{string.Join(", ", grain.EntriesForTest.Keys)}].");
            Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "k0", "k1", "k2" }));
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2L));
        });
    }
}
