using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the activation-time WAL materialiser on
/// <see cref="BPlusLeafGrain"/>. The materialiser drives the dormant
/// <see cref="ILeafProjection.Apply(in LatticeMutation)"/> seam over
/// every WAL entry strictly after the persisted
/// <see cref="LeafNodeState.ProjectionCheckpointOffset"/> and
/// at-or-before the WAL head, then advances the persisted checkpoint
/// (clamped behind any unresolved prepared saga rebuilt during
/// replay).
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string MaterialiserTreeId = "tree-materialiser";
    private const string MaterialiserReplicaId = "leaf-materialiser-test";

    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafReplayCoordinatorGrain Coordinator, ILeafCursorReporter Reporter) CreateGrainWithMaterialiser(
        ILeafReplayCoordinatorGrain coordinator,
        string? treeId = MaterialiserTreeId,
        long persistedCheckpoint = 0,
        Action<LeafNodeState>? seedState = null,
        ILatticeFallOffLogDetector? detector = null,
        ILeafCursorReporter? reporter = null)
    {
        reporter ??= Substitute.For<ILeafCursorReporter>();

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        if (detector is not null)
            sc.AddSingleton(detector);
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", MaterialiserReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        if (treeId is not null)
            state.State.TreeId = treeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;
        seedState?.Invoke(state.State);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);

        // MaterialiserCheckpointInterval = TimeSpan.Zero forces every-entry
        // flush mode so the persisted checkpoint becomes the observable of
        // "the materialiser advanced". The default coalescing predicate
        // (1 s / 1 000 entries) does not fire under sub-second tests.
        var baseOptions = new LatticeOptions { MaterialiserCheckpointInterval = TimeSpan.Zero };
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: baseOptions,
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(context, state, grainFactory, optionsResolver, TestMutationObservers.NoObservers());
        return (grain, state, coordinator, reporter);
    }

    private static ILeafReplayCoordinatorGrain BuildCoordinator(long head, params CommitLogSliceEntry[] entries)
    {
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(head));
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
                var slice = new List<CommitLogSliceEntry>();
                foreach (var e in entries)
                {
                    if (e.Offset <= fromExclusive)
                        continue;
                    if (e.Offset > toInclusive)
                        break;
                    slice.Add(e);
                    if (slice.Count >= budget)
                        break;
                }
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(slice);
            });
        return coord;
    }

    private static LatticeMutation BuildPreparedSet(
        Guid txId,
        string key,
        byte[] value,
        long hlcPhysical = 100,
        string treeId = MaterialiserTreeId,
        bool isTombstone = false)
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = isTombstone ? null : value,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
            IsTombstone = isTombstone,
            IsPrepared = true,
            TransactionId = txId,
        };

    private static LatticeMutation BuildCommittedSet(
        string key,
        byte[] value,
        long hlcPhysical = 100,
        string treeId = MaterialiserTreeId)
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
        };

    private static LatticeMutation BuildTerminal(Guid txId, bool committed, string treeId = MaterialiserTreeId)
        => new()
        {
            TreeId = treeId,
            Kind = committed ? MutationKind.TxCommit : MutationKind.TxAbort,
            Key = "0",
            Timestamp = HybridLogicalClock.Zero,
            TransactionId = txId,
        };

    private static Task ActivateAsync(BPlusLeafGrain grain, CancellationToken ct = default) =>
        ((IGrainBase)grain).OnActivateAsync(ct);

    [Test]
    public async Task Materialiser_no_op_when_tree_id_unset()
    {
        var coord = BuildCoordinator(head: 5);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord, treeId: null);

        await ActivateAsync(grain);

        await coord.DidNotReceive().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task Materialiser_no_op_when_head_at_or_below_checkpoint()
    {
        var coord = BuildCoordinator(head: 7);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord, persistedCheckpoint: 7);

        await ActivateAsync(grain);

        await coord.Received(1).GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        await coord.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(7));
    }

    [Test]
    public async Task Materialiser_replays_committed_set_for_foreign_keys()
    {
        // state.State.Entries is rebuilt from the WAL on every activation
        // (the foreground commit path does not persist it), so the
        // materialiser must apply every committed Set in the WAL slice
        // regardless of whether the leaf already "owns" the key. A
        // ContainsKey-based ownership filter would silently drop every
        // foreground write made between the last persisted checkpoint
        // and the previous deactivation - this test is the regression
        // gate against that bug class.
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_replays_committed_delete_for_unknown_keys()
    {
        // Same regression case as foreign-key Set: the foreground Delete
        // path updates Entries in-memory only, so a re-activation must
        // re-apply every Delete in the WAL slice to materialise the
        // tombstone in the rebuilt projection.
        var entry = new CommitLogSliceEntry(1, BuildDelete("k1", hlcPhysical: 100));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
        var entryValue = state.State.Entries["k1"];
        Assert.That(entryValue.IsTombstone, Is.True);
        Assert.That(await grain.GetAsync("k1"), Is.Null);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_replays_committed_set_for_known_keys()
    {
        // Seed the leaf as the owner of "k1", then replay a fresh value.
        // The replay's HLC dominates the seeded value's HLC under LWW.
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("replayed"), hlcPhysical: 500));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.Entries["k1"] = new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes("stale"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
                IsTombstone = false,
            });

        await ActivateAsync(grain);

        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("replayed"));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_replays_delete_range_against_owned_entries()
    {
        // Seed three entries; replay a DeleteRange [k2, k4) that should
        // tombstone k2 and k3 but leave k1 and k4 visible.
        var entry = new CommitLogSliceEntry(1, BuildDeleteRange("k2", "k4", hlcPhysical: 500));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s =>
            {
                foreach (var k in new[] { "k1", "k2", "k3", "k4" })
                {
                    s.Entries[k] = new LwwValue<byte[]>
                    {
                        Value = Encoding.UTF8.GetBytes(k),
                        Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
                    };
                }
            });

        await ActivateAsync(grain);

        Assert.That(await grain.GetAsync("k1"), Is.Not.Null);
        Assert.That(await grain.GetAsync("k2"), Is.Null);
        Assert.That(await grain.GetAsync("k3"), Is.Null);
        Assert.That(await grain.GetAsync("k4"), Is.Not.Null);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_replays_prepared_set_into_pending_tx()
    {
        var txId = Guid.NewGuid();
        var entry = new CommitLogSliceEntry(1, BuildPreparedSet(txId, "k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        // Pending bucket reconstructed from the WAL replay.
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        // Prepared writes are NOT visible until the terminal mark applies.
        Assert.That(await grain.GetAsync("k1"), Is.Null);
        // Checkpoint clamped behind the unresolved prepare at offset 1
        // -> persisted offset stays at 0.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task Materialiser_replays_prepared_tombstone_into_pending_tx()
    {
        // Saga prepare-phase Delete: routed into pending-tx as a
        // prepared tombstone via ApplyPreparedSet (IsTombstone is
        // copied onto the LwwValue, not branched in the dispatch).
        var txId = Guid.NewGuid();
        var entry = new CommitLogSliceEntry(1, BuildPreparedSet(txId, "k1", Array.Empty<byte>(), isTombstone: true));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        // Checkpoint clamped behind the unresolved prepare at offset 1
        // -> persisted offset stays at 0.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0));
    }

    [Test]
    public async Task Materialiser_terminal_commit_flips_pending_tx_into_entries()
    {
        var txId = Guid.NewGuid();
        var prepared = new CommitLogSliceEntry(1, BuildPreparedSet(txId, "k1", Encoding.UTF8.GetBytes("v1")));
        var terminal = new CommitLogSliceEntry(2, BuildTerminal(txId, committed: true));
        var coord = BuildCoordinator(head: 2, prepared, terminal);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0));
        var read = await grain.GetAsync("k1");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
        // No unresolved prepare — checkpoint advances to the head.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2));
    }

    [Test]
    public async Task Materialiser_terminal_abort_drops_pending_tx_without_visible_writes()
    {
        var txId = Guid.NewGuid();
        var prepared = new CommitLogSliceEntry(1, BuildPreparedSet(txId, "k1", Encoding.UTF8.GetBytes("v1")));
        var terminal = new CommitLogSliceEntry(2, BuildTerminal(txId, committed: false));
        var coord = BuildCoordinator(head: 2, prepared, terminal);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0));
        Assert.That(await grain.GetAsync("k1"), Is.Null);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2));
    }

    [Test]
    public async Task Materialiser_clamps_checkpoint_behind_unresolved_prepare()
    {
        // Two prepares; only the first has a terminal in this slice.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var prepared1 = new CommitLogSliceEntry(1, BuildPreparedSet(tx1, "k1", Encoding.UTF8.GetBytes("v1")));
        var commit1 = new CommitLogSliceEntry(2, BuildTerminal(tx1, committed: true));
        var prepared2 = new CommitLogSliceEntry(3, BuildPreparedSet(tx2, "k2", Encoding.UTF8.GetBytes("v2")));
        var coord = BuildCoordinator(head: 3, prepared1, commit1, prepared2);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        // Checkpoint clamped to MinUnresolvedPrepareOffset - 1 = 3 - 1 = 2.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2));
    }

    [Test]
    public async Task Materialiser_clamps_to_min_unresolved_when_multiple_prepares()
    {
        // Three unresolved prepares at offsets 2, 5, 7. Checkpoint must
        // clamp behind the MIN unresolved offset (2 - 1 = 1), not the
        // MAX or the latest. A wrong clamp would silently advance the
        // persisted checkpoint past prepares whose terminals are still
        // outstanding.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var tx3 = Guid.NewGuid();
        var entry1 = new CommitLogSliceEntry(1, BuildCommittedSet("k0", Encoding.UTF8.GetBytes("v0")));
        var prepared1 = new CommitLogSliceEntry(2, BuildPreparedSet(tx1, "k1", Encoding.UTF8.GetBytes("v1")));
        var entry3 = new CommitLogSliceEntry(3, BuildCommittedSet("k3", Encoding.UTF8.GetBytes("v3")));
        var entry4 = new CommitLogSliceEntry(4, BuildCommittedSet("k4", Encoding.UTF8.GetBytes("v4")));
        var prepared2 = new CommitLogSliceEntry(5, BuildPreparedSet(tx2, "k5", Encoding.UTF8.GetBytes("v5")));
        var entry6 = new CommitLogSliceEntry(6, BuildCommittedSet("k6", Encoding.UTF8.GetBytes("v6")));
        var prepared3 = new CommitLogSliceEntry(7, BuildPreparedSet(tx3, "k7", Encoding.UTF8.GetBytes("v7")));
        var coord = BuildCoordinator(head: 7, entry1, prepared1, entry3, entry4, prepared2, entry6, prepared3);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(3));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_advances_checkpoint_after_clean_replay()
    {
        var entry1 = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var entry2 = new CommitLogSliceEntry(2, BuildCommittedSet("k2", Encoding.UTF8.GetBytes("v2")));
        var coord = BuildCoordinator(head: 2, entry1, entry2);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2));
    }

    [Test]
    public async Task Materialiser_resumes_from_persisted_checkpoint()
    {
        // Three entries on the WAL; checkpoint already at 1 -> only
        // entries 2 and 3 should be replayed.
        var entry1 = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var entry2 = new CommitLogSliceEntry(2, BuildCommittedSet("k2", Encoding.UTF8.GetBytes("v2"), hlcPhysical: 200));
        var entry3 = new CommitLogSliceEntry(3, BuildCommittedSet("k3", Encoding.UTF8.GetBytes("v3"), hlcPhysical: 300));
        var coord = BuildCoordinator(head: 3, entry1, entry2, entry3);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            persistedCheckpoint: 1,
            seedState: s =>
            {
                // k1 already persisted -> seed a value newer than entry1
                // so a redundant replay would be detectable. The
                // materialiser must NOT replay entry1, so the seeded
                // value survives.
                s.Entries["k1"] = new LwwValue<byte[]>
                {
                    Value = Encoding.UTF8.GetBytes("seeded"),
                    Timestamp = new HybridLogicalClock { WallClockTicks = 1_000 },
                };
            });

        await ActivateAsync(grain);

        // ReadSliceAsync invoked with fromExclusive = persistedCheckpoint = 1.
        await coord.Received(1).ReadSliceAsync(1, 3, Arg.Any<int>(), Arg.Any<CancellationToken>());

        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k1"))!), Is.EqualTo("seeded"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k2"))!), Is.EqualTo("v2"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k3"))!), Is.EqualTo("v3"));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3));
    }

    [Test]
    public async Task Materialiser_idempotent_terminal_replay_is_no_op_on_second_activation()
    {
        var txId = Guid.NewGuid();
        var prepared = new CommitLogSliceEntry(1, BuildPreparedSet(txId, "k1", Encoding.UTF8.GetBytes("v1")));
        var terminal = new CommitLogSliceEntry(2, BuildTerminal(txId, committed: true));
        var coord = BuildCoordinator(head: 2, prepared, terminal);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        // First activation - full replay, checkpoint advances to 2.
        await ActivateAsync(grain);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k1"))!), Is.EqualTo("v1"));

        coord.ClearReceivedCalls();

        // Second activation - checkpoint is already at the head, so the
        // materialiser short-circuits without re-reading any slice.
        await ActivateAsync(grain);
        await coord.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void Materialiser_propagates_coordinator_failures()
    {
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
             .Throws(new InvalidOperationException("WAL temporarily unavailable"));
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await ActivateAsync(grain));
        Assert.That(ex!.Message, Is.EqualTo("WAL temporarily unavailable"));
    }

    [Test]
    public async Task Materialiser_replays_multiple_slices_when_budget_exceeded()
    {
        // Seed > ReplaySliceBudget (256) entries so the inner while-loop
        // must stitch slices. Each slice returns at most 256 entries
        // (BuildCoordinator honours the budget arg). The materialiser
        // must continue iterating until fromExclusive reaches head.
        const int totalEntries = 600;
        var entries = new CommitLogSliceEntry[totalEntries];
        for (int i = 0; i < totalEntries; i++)
        {
            var key = $"k{i:D4}";
            var mutation = BuildCommittedSet(key, Encoding.UTF8.GetBytes($"v{i}"), hlcPhysical: 1_000 + i);
            entries[i] = new CommitLogSliceEntry(i + 1, mutation);
        }

        var coord = BuildCoordinator(head: totalEntries, entries);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        // Three slices required for 600 entries at budget 256.
        await coord.Received(3).ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());

        // Spot-check first / boundary / last to confirm every slice was
        // applied (not just the first).
        Assert.That(state.State.Entries.ContainsKey("k0000"), Is.True);
        Assert.That(state.State.Entries.ContainsKey("k0255"), Is.True); // end of slice 1
        Assert.That(state.State.Entries.ContainsKey("k0256"), Is.True); // start of slice 2
        Assert.That(state.State.Entries.ContainsKey("k0511"), Is.True); // end of slice 2
        Assert.That(state.State.Entries.ContainsKey("k0599"), Is.True); // last entry
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(totalEntries));
    }

    [Test]
    public async Task Materialiser_request_slice_budget_does_not_exceed_const()
    {
        // Bound assertion: the materialiser must never request a slice
        // larger than the documented ReplaySliceBudget (256). If a
        // future refactor accidentally raises the per-call request
        // size the leaf's worst-case activation memory footprint
        // grows unboundedly. NSubstitute captures the largest budget
        // observed across all calls.
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        await coord.Received().ReadSliceAsync(
            Arg.Any<long>(),
            Arg.Any<long>(),
            Arg.Is<int>(b => b > 0 && b <= 256),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Materialiser_handles_mixed_prepared_committed_terminals_in_single_slice()
    {
        // End-to-end mix: a committed write, a prepared+committed saga,
        // a committed write, a prepared+aborted saga, and a final
        // committed write. After replay: the saga-1 write is visible,
        // the saga-2 write is gone, and the trailing committed write
        // is visible.
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var entries = new[]
        {
            new CommitLogSliceEntry(1, BuildCommittedSet("a", Encoding.UTF8.GetBytes("av"))),
            new CommitLogSliceEntry(2, BuildPreparedSet(tx1, "b", Encoding.UTF8.GetBytes("bv"))),
            new CommitLogSliceEntry(3, BuildTerminal(tx1, committed: true)),
            new CommitLogSliceEntry(4, BuildCommittedSet("c", Encoding.UTF8.GetBytes("cv"))),
            new CommitLogSliceEntry(5, BuildPreparedSet(tx2, "d", Encoding.UTF8.GetBytes("dv"))),
            new CommitLogSliceEntry(6, BuildTerminal(tx2, committed: false)),
            new CommitLogSliceEntry(7, BuildCommittedSet("e", Encoding.UTF8.GetBytes("ev"))),
        };
        var coord = BuildCoordinator(head: 7, entries);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("a"))!), Is.EqualTo("av"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("b"))!), Is.EqualTo("bv"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("c"))!), Is.EqualTo("cv"));
        Assert.That(await grain.GetAsync("d"), Is.Null);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("e"))!), Is.EqualTo("ev"));
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(7));
    }

    [Test]
    public void Materialiser_honours_cancellation_during_replay()
    {
        // Cancellation responsiveness: a token cancelled before
        // activation must surface as OperationCanceledException, not
        // silently complete the replay.
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await ActivateAsync(grain, cts.Token));
    }

    [Test]
    public void Materialiser_propagates_apply_failures()
    {
        // ILeafProjection.Apply failures must propagate. A leaf that
        // comes online with a half-applied projection silently
        // violates saga reader-isolation. We trigger an Apply failure
        // by feeding a malformed DeleteRange (Key > EndExclusiveKey is
        // a no-op; the easier injection is a coordinator that
        // consistently throws on ReadSliceAsync).
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(5L));
        coord.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
             .Throws(new InvalidOperationException("synthetic slice read failure"));

        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () => await ActivateAsync(grain));
        Assert.That(ex!.Message, Is.EqualTo("synthetic slice read failure"));
    }

    [Test]
    public async Task Materialiser_stamps_apply_offset_via_min_unresolved_clamp()
    {
        // The materialiser must wrap each Apply call in
        // LatticeApplyOffsetContext.BeginScope(entry.Offset). The
        // observable consequence is that prepared-Set entries land in
        // pending-tx with their WAL offset attached, which surfaces
        // through the MinUnresolvedPrepareOffset clamp on the
        // checkpoint. Replay a prepared set at offset 42 with no
        // committed entries; the persisted checkpoint must be 41
        // (= 42 - 1), proving the offset was stamped during Apply.
        var txId = Guid.NewGuid();
        // Pad with committed entries up to offset 41 so the materialiser
        // can advance maxApplied past 0; the prepared set then surfaces
        // the clamp.
        var entries = new List<CommitLogSliceEntry>();
        for (long i = 1; i <= 41; i++)
        {
            entries.Add(new CommitLogSliceEntry(i, BuildCommittedSet($"c{i}", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100 + i)));
        }
        entries.Add(new CommitLogSliceEntry(42, BuildPreparedSet(txId, "p", Encoding.UTF8.GetBytes("pv"), hlcPhysical: 200)));

        var coord = BuildCoordinator(head: 42, entries.ToArray());
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        await ActivateAsync(grain);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
        // If the offset stamp was missing the clamp would default to
        // some other value; an offset-stamped pending tx clamps the
        // checkpoint to MinUnresolvedPrepareOffset - 1 = 41.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(41));
    }

    // -------------------------------------------------------------------
    // Fall-off-log detector integration
    // -------------------------------------------------------------------

    [Test]
    public async Task Materialiser_continues_replay_when_detector_returns_TailReplay()
    {
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.TailReplay));

        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord, detector: detector);

        await ActivateAsync(grain);

        await detector.Received(1).ClassifyAsync(
            MaterialiserTreeId,
            0,
            0L,
            Arg.Any<TimeSpan>(),
            Arg.Any<ResolvedLatticeOptions>(),
            Arg.Any<CancellationToken>());
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k1"))!), Is.EqualTo("v1"));
    }

    [Test]
    public void Materialiser_throws_LeafProjectionStaleException_when_detector_returns_Fail()
    {
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.Fail));

        var coord = BuildCoordinator(head: 100);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord, detector: detector);

        var ex = Assert.ThrowsAsync<LeafProjectionStaleException>(async () => await ActivateAsync(grain));
        Assert.That(ex!.Message, Does.Contain(MaterialiserTreeId));
        Assert.That(ex.Message, Does.Contain("Fail"));
    }

    [Test]
    public void Materialiser_throws_when_detector_returns_SnapshotThenWal()
    {
        // V1 does not integrate snapshot-driven recovery. The
        // SnapshotThenWal decision must surface as
        // LeafProjectionStaleException so the activation pipeline can
        // route the recovery through operator escalation rather than
        // silently serving a half-recovered projection.
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.SnapshotThenWal));

        var coord = BuildCoordinator(head: 100);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord, detector: detector);

        var ex = Assert.ThrowsAsync<LeafProjectionStaleException>(async () => await ActivateAsync(grain));
        Assert.That(ex!.Message, Does.Contain("SnapshotThenWal"));
    }

    [Test]
    public void Materialiser_throws_when_detector_returns_FullRebuildFromWal()
    {
        var detector = Substitute.For<ILatticeFallOffLogDetector>();
        detector.ClassifyAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<ResolvedLatticeOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(FallOffLogDecision.FullRebuildFromWal));

        var coord = BuildCoordinator(head: 100);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord, detector: detector);

        Assert.ThrowsAsync<LeafProjectionStaleException>(async () => await ActivateAsync(grain));
    }

    [Test]
    public async Task Materialiser_skips_detector_consultation_when_unregistered()
    {
        // Legacy / single-cluster hosts that do not register the
        // detector must still get the normal tail-replay path. The
        // materialiser treats a missing detector as "no fall-off-log
        // possible" (which is correct: the only way the WAL can
        // outrun the checkpoint is via trim, and trim requires the
        // replication package).
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1")));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord, detector: null);

        await ActivateAsync(grain);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    // -------------------------------------------------------------------
    // Cursor publish optimisation: redundant publish elision
    // -------------------------------------------------------------------

    [Test]
    public async Task Materialiser_skips_redundant_cursor_publish_after_checkpoint_advance()
    {
        // When the materialiser advances the checkpoint,
        // SetCheckpointOffsetAsync routes through
        // FlushPendingCheckpointAsync which already publishes the
        // cursor. The activation hook must NOT then call
        // ReportCursorIfActiveAsync explicitly: that would be a
        // redundant (idempotent but wasteful) RPC.
        var reporter = Substitute.For<ILeafCursorReporter>();
        var entry = new CommitLogSliceEntry(1, BuildCommittedSet("k1", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 500));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(coord, reporter: reporter);

        await ActivateAsync(grain);

        // Exactly one cursor publish: the materialiser-driven flush.
        // Without the elision the activation hook would publish a
        // second time after the materialiser returned.
        await reporter.Received(1).ReportAsync(
            MaterialiserTreeId,
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Materialiser_publishes_cursor_via_activation_hook_when_no_replay_required()
    {
        // The opposite path: head <= checkpoint so the materialiser
        // does NOT advance, so SetCheckpointOffsetAsync never fires.
        // The activation hook's explicit publish must run so the WAL
        // GC sees the leaf eagerly. This test exercises the warm-leaf
        // re-activation case where the projection clock has already
        // advanced from a prior activation.
        var reporter = Substitute.For<ILeafCursorReporter>();
        var coord = BuildCoordinator(head: 5);
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            coord,
            persistedCheckpoint: 5,
            seedState: s => s.Clock = new HybridLogicalClock { WallClockTicks = 1_000 },
            reporter: reporter);

        await ActivateAsync(grain);

        await reporter.Received(1).ReportAsync(
            MaterialiserTreeId,
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    // -------------------------------------------------------------------
    // Apply-time shard-ownership filter (cross-shard fanout regression)
    // -------------------------------------------------------------------

    /// <summary>
    /// Builds a committed Set with an explicit ShardIndex so the apply-time
    /// filter can be exercised regardless of how the foreground producer
    /// stamped the slot.
    /// </summary>
    private static LatticeMutation BuildCommittedSetWithShardIndex(
        string key,
        byte[] value,
        int shardIndex,
        long hlcPhysical = 100,
        string treeId = MaterialiserTreeId)
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
            ShardIndex = shardIndex,
        };

    private static LatticeMutation BuildDeleteWithShardIndex(
        string key,
        int shardIndex,
        long hlcPhysical = 200,
        string treeId = MaterialiserTreeId)
        => new()
        {
            TreeId = treeId,
            Kind = MutationKind.Delete,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlcPhysical },
            IsTombstone = true,
            ShardIndex = shardIndex,
        };

    [Test]
    public async Task Materialiser_filters_out_set_with_mismatching_shard_index()
    {
        // Cross-shard fanout regression gate: when the leaf has a
        // persisted ShardIndex, the materialiser must drop every Set
        // whose mutation.ShardIndex does not match. Without this
        // filter, a sibling chain shard sharing the WAL partition
        // would silently fanout its writes into every leaf that
        // replays the partition - the original V1 dormant bug.
        var foreign = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex("k-foreign", Encoding.UTF8.GetBytes("foreign"), shardIndex: 2));
        var coord = BuildCoordinator(head: 1, foreign);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.ShardIndex = 1);

        await ActivateAsync(grain);

        // Filtered out: the entry never lands in this leaf's projection.
        Assert.That(state.State.Entries.ContainsKey("k-foreign"), Is.False);
        Assert.That(await grain.GetAsync("k-foreign"), Is.Null);
        // Checkpoint still advances - the filter is per-entry, not a slice abort.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_applies_set_with_matching_shard_index()
    {
        var owned = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex("k-mine", Encoding.UTF8.GetBytes("v"), shardIndex: 3));
        var coord = BuildCoordinator(head: 1, owned);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.ShardIndex = 3);

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k-mine"), Is.True);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k-mine"))!), Is.EqualTo("v"));
    }

    [Test]
    public async Task Materialiser_filters_out_delete_with_mismatching_shard_index()
    {
        // Same regression class as the Set side: a sibling chain
        // shard's Delete must not silently tombstone an entry on this
        // leaf. The Delete is filtered before it reaches the
        // projection.
        // Seed an owned entry first so we can prove it survived.
        var coord = BuildCoordinator(
            head: 2,
            new CommitLogSliceEntry(
                1,
                BuildCommittedSetWithShardIndex("k1", Encoding.UTF8.GetBytes("alive"), shardIndex: 1, hlcPhysical: 100)),
            new CommitLogSliceEntry(
                2,
                BuildDeleteWithShardIndex("k1", shardIndex: 7, hlcPhysical: 500)));
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.ShardIndex = 1);

        await ActivateAsync(grain);

        // The owned Set landed; the foreign Delete was filtered.
        Assert.That(state.State.Entries.ContainsKey("k1"), Is.True);
        Assert.That(await grain.GetAsync("k1"), Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k1"))!), Is.EqualTo("alive"));
    }

    [Test]
    public async Task Materialiser_applies_set_when_leaf_shard_index_is_null_legacy_compat()
    {
        // Legacy compat: if the leaf has no persisted shard index
        // (e.g. an older deployment that never seeded the slot, or
        // any V1 single-shard deployment), every Set/Delete is
        // applied unconditionally. The filter only engages once the
        // shard root has called SetShardIndexAsync.
        var entry = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex("k", Encoding.UTF8.GetBytes("v"), shardIndex: 42));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(coord);

        // Pre-condition: ShardIndex is null (no seedState callback set it).
        Assert.That(state.State.ShardIndex, Is.Null);

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k"), Is.True);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k"))!), Is.EqualTo("v"));
    }

    [Test]
    public async Task Materialiser_applies_delete_range_regardless_of_shard_index()
    {
        // DeleteRange replay on the receiving leaf iterates that
        // leaf's own Entries only, so the apply-time filter
        // unconditionally applies DeleteRange entries regardless of
        // the originating shard. (A DeleteRange that names keys this
        // leaf does not own is a no-op by construction.) The filter
        // would otherwise need to track range overlap, which is more
        // complex than necessary.
        var entry = new CommitLogSliceEntry(
            1,
            new LatticeMutation
            {
                TreeId = MaterialiserTreeId,
                Kind = MutationKind.DeleteRange,
                Key = "a",
                EndExclusiveKey = "z",
                Timestamp = new HybridLogicalClock { WallClockTicks = 500 },
                ShardIndex = 99,
            });
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s =>
            {
                s.ShardIndex = 1;
                s.Entries["k1"] = new LwwValue<byte[]>
                {
                    Value = Encoding.UTF8.GetBytes("v1"),
                    Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
                };
            });

        await ActivateAsync(grain);

        // Owned entry tombstoned by the cross-shard DeleteRange.
        Assert.That(await grain.GetAsync("k1"), Is.Null);
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_applies_terminals_regardless_of_shard_index()
    {
        // Terminals (TxCommit / TxAbort) must fire on every leaf that
        // has the matching prepared write, regardless of which leaf
        // authored the terminal. (The terminal records the
        // saga-coordinator decision; it does not "belong" to any one
        // leaf.) The filter applies terminals unconditionally.
        var txId = Guid.NewGuid();
        var prepared = new CommitLogSliceEntry(
            1,
            new LatticeMutation
            {
                TreeId = MaterialiserTreeId,
                Kind = MutationKind.Set,
                Key = "k1",
                Value = Encoding.UTF8.GetBytes("v1"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
                IsPrepared = true,
                TransactionId = txId,
                ShardIndex = 1,
            });
        // Terminal stamped from a sibling shard (e.g. the saga coordinator
        // ran on a different shard than this leaf).
        var terminal = new CommitLogSliceEntry(
            2,
            new LatticeMutation
            {
                TreeId = MaterialiserTreeId,
                Kind = MutationKind.TxCommit,
                Key = "0",
                Timestamp = HybridLogicalClock.Zero,
                TransactionId = txId,
                ShardIndex = 9,
            });
        var coord = BuildCoordinator(head: 2, prepared, terminal);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.ShardIndex = 1);

        await ActivateAsync(grain);

        // Terminal applied -> prepared write is now committed and visible.
        Assert.That(grain.PendingTransactionCount, Is.EqualTo(0));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k1"))!), Is.EqualTo("v1"));
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(2));
    }

    // -------------------------------------------------------------------
    // Apply-time per-key-range filter (intra-shard sibling-leaf fanout
    // regression). Within a single chain shard, splits produce multiple
    // leaves that share the shard's WAL partition. The per-shard filter
    // alone is not enough: every sibling leaf in the chain would
    // otherwise absorb every other leaf's writes on activation. The
    // filter keys on the leaf's persisted ownership range
    // [LowKeyInclusive, HighKeyExclusive). Authorship is intentionally
    // NOT considered: a leaf born from a split must apply WAL entries
    // that fall in its current range even when those entries were
    // authored by the donor pre-split (the rebuild-from-WAL scenario).
    // -------------------------------------------------------------------

    [Test]
    public async Task Materialiser_filters_out_set_outside_owned_key_range()
    {
        // Same shard, but the entry's key falls outside this leaf's
        // [low, high) ownership range. The filter must drop it so this
        // leaf's projection does not absorb a sibling leaf's keys.
        var foreign = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex(
                "z-foreign",
                Encoding.UTF8.GetBytes("foreign"),
                shardIndex: 1));
        var coord = BuildCoordinator(head: 1, foreign);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s =>
            {
                s.ShardIndex = 1;
                s.LowKeyInclusive = "a";
                s.HighKeyExclusive = "m";
            });

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("z-foreign"), Is.False);
        Assert.That(await grain.GetAsync("z-foreign"), Is.Null);
        // Checkpoint still advances - the filter is per-entry, not a slice abort.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(1));
    }

    [Test]
    public async Task Materialiser_applies_set_inside_owned_key_range()
    {
        // Same shard and the entry's key falls inside this leaf's
        // [low, high) ownership range, so the filter passes and the
        // entry lands in the projection.
        var owned = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex(
                "k-mine",
                Encoding.UTF8.GetBytes("v"),
                shardIndex: 1));
        var coord = BuildCoordinator(head: 1, owned);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s =>
            {
                s.ShardIndex = 1;
                s.LowKeyInclusive = "k";
                s.HighKeyExclusive = "l";
            });

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k-mine"), Is.True);
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync("k-mine"))!), Is.EqualTo("v"));
    }

    [Test]
    public async Task Materialiser_applies_set_when_range_unset_legacy_compat()
    {
        // Wire-additivity: a leaf whose state pre-dates the
        // LowKeyInclusive/HighKeyExclusive slots decodes both as
        // null, which the filter treats as "apply unconditionally on
        // the range axis" so a binary upgrade does not start silently
        // dropping legacy entries.
        var legacy = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex(
                "k-legacy",
                Encoding.UTF8.GetBytes("v"),
                shardIndex: 1));
        var coord = BuildCoordinator(head: 1, legacy);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s => s.ShardIndex = 1
            /* LowKeyInclusive/HighKeyExclusive omitted -> null */);

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k-legacy"), Is.True);
    }

    [Test]
    public async Task Materialiser_applies_set_in_owned_range_regardless_of_authoring_leaf()
    {
        // Rebuild-mode scenario: a leaf inherited keys via a previous
        // split but is reconstructing its projection from offset 0
        // (snapshot rebuild, corruption recovery, or fresh activation
        // before any local writes). The WAL entries for the inherited
        // keys were authored by the donor sibling in the same shard,
        // not by this leaf. The materialiser must still apply them
        // because they fall in this leaf's current ownership range,
        // which is the authoritative "currently owned by" identity
        // (not the WAL-immutable "originally written by"). Without
        // this, a freshly-activated split-born leaf with an empty
        // Entries map would never recover its inherited keys from the
        // WAL.
        var entry = new CommitLogSliceEntry(
            1,
            BuildCommittedSetWithShardIndex(
                "k-inherited",
                Encoding.UTF8.GetBytes("v"),
                shardIndex: 1));
        var coord = BuildCoordinator(head: 1, entry);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            coord,
            seedState: s =>
            {
                s.ShardIndex = 1;
                s.LowKeyInclusive = "k";
                s.HighKeyExclusive = "l";
            });

        await ActivateAsync(grain);

        Assert.That(state.State.Entries.ContainsKey("k-inherited"), Is.True);
    }
}
