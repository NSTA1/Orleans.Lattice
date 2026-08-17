using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for resumable cold WAL replay (issue #1513). A leaf
/// that must cold-replay a large un-snapshotted WAL prefix historically made
/// no durable progress until the whole two-pass replay completed: the
/// projection checkpoint was advanced only by the post-pass-2 reconciliation,
/// so a mid-replay deactivation (Orleans <c>RuntimeRequested</c> ~30 s into
/// activation) discarded every applied entry and the next activation restarted
/// from offset 0. When the prefix could not be drained inside one activation
/// window the leaf never converged and the coverage-gated WAL GC (correctly)
/// refused to trim the un-snapshotted prefix, wedging the tree.
/// <para>
/// The fix flushes the projection checkpoint incrementally over the strictly
/// contiguous, fully-applied prefix at each replay slice boundary, which also
/// drives the existing periodic snapshot capture. A teardown then loses at
/// most one flush interval, and the next activation rehydrates from the
/// incremental snapshot and resumes from the last durable offset. These tests
/// verify: (1) the checkpoint advances incrementally across slices; (2) a
/// mid-replay teardown leaves partial progress AND a covering snapshot durable;
/// (3) the next activation rehydrates from that snapshot and resumes from the
/// durable offset instead of replaying from zero; (4) the incremental advance
/// never passes a deferred terminal / DeleteRange; and (5) it never passes an
/// unresolved saga prepare.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ResumableTreeId = "tree-resumable-replay";

    /// <summary>Stable Guid leaf key so the snapshot storage grain resolves via <c>GetGuidKey()</c>.</summary>
    private static readonly Guid ResumableLeafKey = Guid.Parse("11111111-1111-1111-1111-111111111111");

    /// <summary>
    /// Minimal in-memory <see cref="ILeafSnapshotStorageGrain"/> double that
    /// captures every saved blob and serves the latest one back, so a
    /// reactivation over the same store observes the snapshot the prior
    /// activation captured.
    /// </summary>
    private sealed class InMemorySnapshotStore
    {
        public ILeafSnapshotStorageGrain Stub { get; }
        public LeafSnapshotBlob? Latest { get; private set; }
        public int SaveCount { get; private set; }
        public List<long> SavedOffsets { get; } = new();

        public InMemorySnapshotStore()
        {
            Stub = Substitute.For<ILeafSnapshotStorageGrain>();
            Stub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    Latest = call.ArgAt<LeafSnapshotBlob>(0);
                    SaveCount++;
                    SavedOffsets.Add(Latest.SnapshotOffset);
                    return Task.CompletedTask;
                });
            Stub.LoadAsync(Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(Latest));
        }
    }

    /// <summary>
    /// Builds a coordinator that serves <paramref name="entries"/> in slices of
    /// at most <paramref name="sliceSize"/> records (regardless of the caller's
    /// budget) so a small WAL still exercises the multi-slice incremental-flush
    /// path deterministically.
    /// </summary>
    private static ILeafReplayCoordinatorGrain BuildChunkingCoordinator(
        long head,
        int sliceSize,
        long tail,
        params CommitLogSliceEntry[] entries)
    {
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(tail));
        coord.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var fromExclusive = call.ArgAt<long>(0);
                var toInclusive = call.ArgAt<long>(1);
                var budget = call.ArgAt<int>(2);
                var cap = Math.Min(sliceSize, budget);
                var slice = new List<CommitLogSliceEntry>();
                foreach (var e in entries)
                {
                    if (e.Offset <= fromExclusive)
                        continue;
                    if (e.Offset > toInclusive)
                        break;
                    slice.Add(e);
                    if (slice.Count >= cap)
                        break;
                }
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
            });
        return coord;
    }

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) BuildResumableLeaf(
        FakePersistentState<LeafNodeState> state,
        ILeafReplayCoordinatorGrain coordinator,
        ILeafSnapshotStorageGrain snapshotStub,
        int reclassifyEveryN)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(Substitute.For<ILeafCursorReporter>());
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        // A Guid-formatted key so CaptureSnapshotAsync / TryRehydrateFromSnapshotAsync
        // can resolve the snapshot storage grain via GrainId.GetGuidKey().
        context.GrainId.Returns(GrainId.Create("leaf", ResumableLeafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var baseOptions = new LatticeOptions
        {
            // Every-entry flush so each incremental SetCheckpointOffsetAsync
            // persists and the observable checkpoint tracks replay progress.
            MaterialiserCheckpointInterval = TimeSpan.Zero,
            // Capture a snapshot on every checkpoint persist so a bounded test
            // WAL still produces the eventual covering snapshot the resume path
            // rehydrates from.
            LeafSnapshotReClassifyEveryNCheckpoints = reclassifyEveryN,
            // Single WAL partition keeps the offset space and slice arithmetic
            // deterministic; the multi-partition safety of the incremental
            // clamp is covered by the deferred-terminal and prepare tests.
            WalPartitions = 1,
        };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    private static FakePersistentState<LeafNodeState> NewResumableState(long persistedCheckpoint = 0)
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ResumableTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        return state;
    }

    private static CommitLogSliceEntry Set(long offset, string key) =>
        new(offset, BuildCommittedSet(key, Encoding.UTF8.GetBytes($"v-{key}"), treeId: ResumableTreeId));

    [Test]
    public async Task Cold_replay_flushes_checkpoint_incrementally_across_slices()
    {
        // 12 plain committed Sets served four-per-slice. The cold-cache
        // override drives replay from the start; the checkpoint must advance
        // at each slice boundary (4, 8, 12) rather than only at completion.
        var entries = Enumerable.Range(1, 12)
            .Select(i => Set(i, $"k{i:D2}"))
            .ToArray();
        var coord = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var persistedOffsets = new List<long>();
        state.OnWriteState = s => persistedOffsets.Add(s.ProjectionCheckpointOffset);

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 1);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(persistedOffsets, Is.EqualTo(new long[] { 4, 8, 12 }),
            "Checkpoint must be flushed incrementally at each slice boundary, not once at completion.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
        for (var i = 1; i <= 12; i++)
            Assert.That(await grain.GetAsync($"k{i:D2}"), Is.Not.Null, $"key k{i:D2} missing after replay");
    }

    [Test]
    public async Task Mid_replay_teardown_persists_partial_progress_and_snapshot()
    {
        // Deactivate deterministically right after the first slice's flush by
        // cancelling the activation token from the first state persist. The
        // partial progress (checkpoint = 4) and a covering snapshot at that
        // offset must both survive the teardown so the next activation can
        // resume instead of restarting from zero.
        var entries = Enumerable.Range(1, 12)
            .Select(i => Set(i, $"k{i:D2}"))
            .ToArray();
        var coord = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        using var cts = new CancellationTokenSource();
        var persists = 0;
        state.OnWriteState = _ =>
        {
            if (++persists == 1)
                cts.Cancel();
        };

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 1);

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(cts.Token));

        // Partial progress is durable: exactly the first slice's contiguous
        // prefix was checkpointed.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L),
            "Mid-replay teardown must leave the last flushed contiguous prefix durable.");
        // A covering snapshot at the flushed offset was captured before the
        // teardown, so the next activation has a rehydration anchor.
        Assert.That(store.SaveCount, Is.GreaterThanOrEqualTo(1));
        Assert.That(store.Latest, Is.Not.Null);
        Assert.That(store.Latest!.SnapshotOffset, Is.EqualTo(4L));
        // The still-un-replayed suffix made no progress.
        Assert.That(await grain.GetAsync("k12"), Is.Null);
    }

    [Test]
    public async Task Resumed_activation_rehydrates_from_incremental_snapshot_and_resumes()
    {
        // First activation: cold replay, deactivated after the first slice's
        // flush (checkpoint = 4, snapshot @ 4 captured).
        var fullEntries = Enumerable.Range(1, 12)
            .Select(i => Set(i, $"k{i:D2}"))
            .ToArray();
        var coord1 = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 0, fullEntries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        using (var cts = new CancellationTokenSource())
        {
            var persists = 0;
            state.OnWriteState = _ =>
            {
                if (++persists == 1)
                    cts.Cancel();
            };
            var (grain1, _) = BuildResumableLeaf(state, coord1, store.Stub, reclassifyEveryN: 1);
            Assert.ThrowsAsync<OperationCanceledException>(
                async () => await ((IGrainBase)grain1).OnActivateAsync(cts.Token));
        }

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L));
        Assert.That(store.Latest!.SnapshotOffset, Is.EqualTo(4L));

        // Second activation over the SAME persisted state and snapshot store.
        // The coordinator now only exposes offsets (4, 12] and reports a
        // trimmed tail (5), modelling the coverage-gated WAL GC having trimmed
        // the snapshot-covered prefix [0, 4]. Keys k01..k04 can therefore ONLY
        // come from the rehydrated snapshot - if the resume path restarted from
        // zero they would be permanently lost.
        state.OnWriteState = null;
        var suffixEntries = Enumerable.Range(5, 8)
            .Select(i => Set(i, $"k{i:D2}"))
            .ToArray();
        var coord2 = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 5, suffixEntries);
        var (grain2, _) = BuildResumableLeaf(state, coord2, store.Stub, reclassifyEveryN: 1);

        await ((IGrainBase)grain2).OnActivateAsync(CancellationToken.None);

        // Resumed strictly past the durable offset, never from zero.
        await coord2.Received().ReadSliceAsync(4L, 12L, Arg.Any<int>(), Arg.Any<CancellationToken>());
        await coord2.DidNotReceive().ReadSliceAsync(-1L, Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L));
        for (var i = 1; i <= 12; i++)
            Assert.That(await grain2.GetAsync($"k{i:D2}"), Is.Not.Null,
                $"key k{i:D2} missing after resumed replay (k01..k04 prove snapshot rehydrate; k05..k12 prove suffix replay)");
    }

    [Test]
    public async Task Incremental_flush_never_advances_past_a_deferred_delete_range()
    {
        // Sets at 1,2; a DeleteRange terminal at offset 3 (deferred to pass 2);
        // Sets at 4,5. The incremental flush must clamp the checkpoint at 2
        // (below the deferred terminal) throughout pass 1, and only the
        // post-pass-2 reconciliation may advance it to 5. The persist sequence
        // must therefore never touch offsets 3 or 4.
        var entries = new[]
        {
            Set(1, "a1"),
            Set(2, "a2"),
            new CommitLogSliceEntry(3, BuildDeleteRange("m0", "m9", hlcPhysical: 500, treeId: ResumableTreeId)),
            Set(4, "a4"),
            Set(5, "a5"),
        };
        var coord = BuildChunkingCoordinator(head: 5, sliceSize: 2, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var persistedOffsets = new List<long>();
        state.OnWriteState = s => persistedOffsets.Add(s.ProjectionCheckpointOffset);

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 0);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(persistedOffsets, Does.Not.Contain(3L),
            "Checkpoint must never be flushed at the deferred DeleteRange offset.");
        Assert.That(persistedOffsets, Does.Not.Contain(4L),
            "Checkpoint must not advance past the deferred DeleteRange before pass 2 applies it.");
        Assert.That(persistedOffsets, Does.Contain(2L),
            "The contiguous prefix below the deferred terminal must still be flushed incrementally.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(5L),
            "Post-pass-2 reconciliation advances the checkpoint to the full applied frontier.");
    }

    [Test]
    public async Task Incremental_flush_clamps_below_an_unresolved_prepare()
    {
        // Set at 1; an unresolved saga prepare at offset 2 (no terminal in the
        // replayed window); Sets at 3,4. The incremental flush and the final
        // reconciliation must both stay clamped at 1 (below the open prepare),
        // because a resumed replay must re-read the prepare to rebuild the
        // pending-tx bucket.
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            Set(1, "p1"),
            new CommitLogSliceEntry(2, BuildPreparedSet(txId, "p2", Encoding.UTF8.GetBytes("v2"), treeId: ResumableTreeId)),
            Set(3, "p3"),
            Set(4, "p4"),
        };
        var coord = BuildChunkingCoordinator(head: 4, sliceSize: 2, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var persistedOffsets = new List<long>();
        state.OnWriteState = s => persistedOffsets.Add(s.ProjectionCheckpointOffset);

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 0);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        Assert.That(persistedOffsets, Is.All.LessThanOrEqualTo(1L),
            "No checkpoint persist may advance past the unresolved prepare at offset 2.");
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1L),
            "The checkpoint stays clamped one below the open prepare offset.");
    }
}
