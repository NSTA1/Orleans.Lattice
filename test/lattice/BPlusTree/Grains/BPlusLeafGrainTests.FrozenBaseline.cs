using System.Globalization;
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
/// Unit tests for the per-shard snapshot-baseline capture seam on
/// <see cref="BPlusLeafGrain"/> - <c>FreezeProjectionAsync</c> and
/// <c>FoldTailOntoFrozenAsync</c>, the two read-only calls
/// <c>ShardRootGrain.CaptureSnapshotBaselineAsync</c> drives across a shard's
/// leaf chain.
/// <para>
/// The freeze copies the committed cache, the per-partition WAL frontier, and
/// the in-flight prepared sagas into a serializable
/// <see cref="LeafBaselineFreeze"/>; the fold replays only this leaf's own
/// <c>(frontier_p, capturedHead_p]</c> tail on top of that frozen state and
/// returns the materialised rows. The cluster fixtures exercise the happy
/// path end to end but cannot pin the individual branches - the prepared-saga
/// flatten, the per-partition slice pump, the ownership filter, the deferred
/// saga-terminal drain, and the two defensive loop exits - which is what these
/// direct-construction tests do.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string FrozenTreeId = "tree-frozen-baseline";
    private const string FrozenReplicaId = "leaf-frozen-baseline-test";

    /// <summary>
    /// Builds a leaf wired to one stub <see cref="ILeafReplayCoordinatorGrain"/>
    /// per WAL partition, resolved by parsing the partition number off the
    /// <c>{treeId}/{partition}</c> grain key. Deliberately leaves
    /// <see cref="LeafNodeState.ShardIndex"/> unset so the replay shard map
    /// resolves to <see langword="null"/> and the ownership filter reduces to
    /// the leaf's own key boundary.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateFrozenBaselineGrain(
        Func<int, (long Head, CommitLogSliceEntry[] Entries)>? sliceFactory = null,
        int walPartitions = 1,
        string? treeId = FrozenTreeId,
        Action<LeafNodeState>? seedState = null,
        ILeafReplayCoordinatorGrain? explicitCoordinator = null)
    {
        var services = new ServiceCollection().BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", FrozenReplicaId));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        if (treeId is not null)
            state.State.TreeId = treeId;
        seedState?.Invoke(state.State);

        var coordinators = new Dictionary<int, ILeafReplayCoordinatorGrain>(walPartitions);
        for (var p = 0; p < walPartitions; p++)
        {
            coordinators[p] = explicitCoordinator
                ?? BuildCoordinator(sliceFactory?.Invoke(p) is { } s ? s.Head : 0,
                                    sliceFactory?.Invoke(p).Entries ?? []);
        }

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(call =>
        {
            var key = call.ArgAt<string>(0);
            var slash = key.LastIndexOf('/');
            var token = slash >= 0 ? key[(slash + 1)..] : key;
            return coordinators[int.Parse(token, CultureInfo.InvariantCulture)];
        });

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = walPartitions },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context,
            state,
            grainFactory,
            optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        return (grain, state);
    }

    private static LatticeMutation FrozenPreparedSet(
        Guid txId,
        string key,
        byte[]? value,
        long hlc = 100,
        byte[]? delta = null,
        LatticeMergeMode mode = LatticeMergeMode.LwwRegister)
        => new()
        {
            TreeId = FrozenTreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlc },
            IsPrepared = true,
            TransactionId = txId,
            Delta = delta,
            Mode = mode,
        };

    private static LatticeMutation FrozenCommittedSet(string key, byte[] value, long hlc = 100)
        => new()
        {
            TreeId = FrozenTreeId,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = hlc },
        };

    private static LatticeMutation FrozenTerminal(Guid txId, bool committed)
        => new()
        {
            TreeId = FrozenTreeId,
            Kind = committed ? MutationKind.TxCommit : MutationKind.TxAbort,
            Key = "0",
            Timestamp = HybridLogicalClock.Zero,
            TransactionId = txId,
        };

    private static void SeedPrepared(BPlusLeafGrain grain, LatticeMutation prepared) =>
        ((ILeafProjection)grain).Apply(prepared);

    // --- FreezeProjectionAsync ---

    [Test]
    public async Task Freeze_copies_committed_rows_and_per_partition_frontier()
    {
        var (grain, _) = CreateFrozenBaselineGrain(
            sliceFactory: _ => (Head: 4, Entries: []));
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var freeze = await grain.FreezeProjectionAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(freeze.Rows.Select(r => r.Key), Does.Contain("k1"));
            // The head is exclusive, so the cache spans [0, head) and the
            // fold's exclusive lower bound is head - 1.
            Assert.That(freeze.FrontierPerPartition, Is.EqualTo(new long[] { 3 }));
            Assert.That(freeze.Pending, Is.Empty);
        });
    }

    [Test]
    public async Task Freeze_reports_empty_partition_as_the_from_the_beginning_sentinel()
    {
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 0, Entries: []));

        var freeze = await grain.FreezeProjectionAsync(CancellationToken.None);

        Assert.That(freeze.FrontierPerPartition, Is.EqualTo(new long[] { -1 }),
            "An empty partition must yield the -1 'from the beginning' sentinel.");
    }

    [Test]
    public async Task Freeze_flattens_prepared_saga_buckets()
    {
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 2, Entries: []));
        var tx = Guid.NewGuid();
        SeedPrepared(grain, FrozenPreparedSet(tx, "p1", Encoding.UTF8.GetBytes("staged")));
        SeedPrepared(grain, FrozenPreparedSet(tx, "p2", Encoding.UTF8.GetBytes("staged-2")));

        var freeze = await grain.FreezeProjectionAsync(CancellationToken.None);

        Assert.That(freeze.Pending, Has.Count.EqualTo(2),
            "Every prepared key in the bucket must be flattened into the freeze.");
        var p1 = freeze.Pending.Single(p => p.Key == "p1");
        Assert.Multiple(() =>
        {
            Assert.That(p1.TransactionId, Is.EqualTo(tx));
            Assert.That(p1.Delta, Is.Null, "A plain LWW prepared write carries no CRDT delta.");
            Assert.That(p1.Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(p1.Value.Value, Is.EqualTo(Encoding.UTF8.GetBytes("staged")));
        });
    }

    [Test]
    public async Task Freeze_carries_crdt_delta_and_merge_mode_for_prepared_entries()
    {
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 2, Entries: []));
        var tx = Guid.NewGuid();
        var delta = new byte[] { 7, 8, 9 };
        SeedPrepared(grain, FrozenPreparedSet(
            tx, "c1", Encoding.UTF8.GetBytes("merged"), delta: delta, mode: LatticeMergeMode.GCounter));

        var freeze = await grain.FreezeProjectionAsync(CancellationToken.None);

        var entry = freeze.Pending.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.Delta, Is.EqualTo(delta),
                "The prepared CRDT delta side-map must ride along with the freeze, or the tail's "
                + "terminal would drain the merged state last-writer-wins instead of folding the delta.");
            Assert.That(entry.Mode, Is.EqualTo(LatticeMergeMode.GCounter));
        });
    }

    [Test]
    public async Task Freeze_keeps_pending_entries_without_a_delta_side_map_entry()
    {
        // Two keys in the same saga bucket where only one carries a typed
        // delta: the flatten must look each key up individually rather than
        // stamping the bucket's mode onto every key.
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 2, Entries: []));
        var tx = Guid.NewGuid();
        SeedPrepared(grain, FrozenPreparedSet(
            tx, "crdt", Encoding.UTF8.GetBytes("m"), delta: [1], mode: LatticeMergeMode.OrSet));
        SeedPrepared(grain, FrozenPreparedSet(tx, "lww", Encoding.UTF8.GetBytes("v")));

        var freeze = await grain.FreezeProjectionAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(freeze.Pending.Single(p => p.Key == "crdt").Mode, Is.EqualTo(LatticeMergeMode.OrSet));
            Assert.That(freeze.Pending.Single(p => p.Key == "lww").Mode, Is.EqualTo(LatticeMergeMode.LwwRegister));
            Assert.That(freeze.Pending.Single(p => p.Key == "lww").Delta, Is.Null);
        });
    }

    [Test]
    public void Freeze_throws_when_tree_id_is_unset()
    {
        var (grain, _) = CreateFrozenBaselineGrain(treeId: null);

        Assert.That(
            async () => await grain.FreezeProjectionAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>().And.Message.Contains("tree id is unset"));
    }

    [Test]
    public void Freeze_honours_a_pre_cancelled_token()
    {
        var (grain, _) = CreateFrozenBaselineGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.FreezeProjectionAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // --- FoldTailOntoFrozenAsync ---

    private static LeafBaselineFreeze Freeze(
        IEnumerable<LeafSnapshotRow>? rows = null,
        long[]? frontier = null,
        IEnumerable<LeafBaselinePendingEntry>? pending = null) => new()
        {
            Rows = rows?.ToArray() ?? [],
            FrontierPerPartition = frontier ?? [-1],
            Pending = pending?.ToArray() ?? [],
        };

    private static LeafSnapshotRow Row(string key, string value, long hlc = 10) =>
        new(key, new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes(value),
            Timestamp = new HybridLogicalClock { WallClockTicks = hlc },
        });

    [Test]
    public async Task FoldTail_replays_the_partition_tail_on_top_of_the_frozen_rows()
    {
        var entries = new[]
        {
            new CommitLogSliceEntry(0, FrozenCommittedSet("tail", Encoding.UTF8.GetBytes("from-wal"), hlc: 50)),
        };
        var (grain, _) = CreateFrozenBaselineGrain(sliceFactory: _ => (Head: 1, Entries: entries));

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(rows: [Row("frozen", "baseline")]),
            capturedHead: [1],
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(r => r.Key), Does.Contain("frozen"),
                "The frozen cache must be re-seeded before the tail fold.");
            Assert.That(rows.Single(r => r.Key == "tail").Value.Value,
                Is.EqualTo(Encoding.UTF8.GetBytes("from-wal")));
        });
    }

    [Test]
    public async Task FoldTail_skips_a_partition_whose_tail_is_empty()
    {
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        var (grain, _) = CreateFrozenBaselineGrain(explicitCoordinator: coordinator);

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(rows: [Row("only", "frozen")], frontier: [5]),
            capturedHead: [4],
            CancellationToken.None);

        await coordinator.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "only" }));
    }

    [Test]
    public async Task FoldTail_stops_pumping_when_the_coordinator_returns_an_empty_slice()
    {
        // A non-empty (frontier, capturedHead] window whose coordinator serves
        // nothing must terminate rather than spin.
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]));
        var (grain, _) = CreateFrozenBaselineGrain(explicitCoordinator: coordinator);

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(rows: [Row("only", "frozen")], frontier: [-1]),
            capturedHead: [10],
            CancellationToken.None);

        await coordinator.Received(1).ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "only" }));
    }

    [Test]
    public async Task FoldTail_breaks_when_a_slice_fails_to_advance_the_cursor()
    {
        // Defensive exit: a coordinator that ignores the requested window and
        // keeps serving an offset at or below the cursor must not spin forever.
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
                [new CommitLogSliceEntry(2, FrozenCommittedSet("stuck", Encoding.UTF8.GetBytes("v")))]));
        var (grain, _) = CreateFrozenBaselineGrain(explicitCoordinator: coordinator);

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(frontier: [5]),
            capturedHead: [20],
            CancellationToken.None);

        await coordinator.Received(1).ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "stuck" }),
            "The non-advancing slice is still folded once; only the pump stops.");
    }

    [Test]
    public async Task FoldTail_skips_entries_outside_the_leaf_key_range()
    {
        var entries = new[]
        {
            new CommitLogSliceEntry(0, FrozenCommittedSet("b-owned", Encoding.UTF8.GetBytes("mine"))),
            new CommitLogSliceEntry(1, FrozenCommittedSet("z-foreign", Encoding.UTF8.GetBytes("theirs"))),
        };
        var (grain, _) = CreateFrozenBaselineGrain(
            sliceFactory: _ => (Head: 2, Entries: entries),
            seedState: s =>
            {
                s.LowKeyInclusive = "b";
                s.HighKeyExclusive = "d";
            });

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(),
            capturedHead: [2],
            CancellationToken.None);

        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "b-owned" }),
            "A WAL entry outside this leaf's key range must not be folded into its baseline.");
    }

    [Test]
    public async Task FoldTail_defers_a_saga_terminal_until_every_partition_prepare_is_absorbed()
    {
        // The terminal sits on partition 0 while its prepare sits on
        // partition 1. Only a deferred pass-2 drain resolves it.
        var tx = Guid.NewGuid();
        var (grain, _) = CreateFrozenBaselineGrain(
            walPartitions: 2,
            sliceFactory: p => p == 0
                ? (Head: 1, Entries: [new CommitLogSliceEntry(0, FrozenTerminal(tx, committed: true))])
                : (Head: 1, Entries: [new CommitLogSliceEntry(0, FrozenPreparedSet(tx, "saga", Encoding.UTF8.GetBytes("committed")))]));

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(frontier: [-1, -1]),
            capturedHead: [1, 1],
            CancellationToken.None);

        Assert.That(rows.Single(r => r.Key == "saga").Value.Value,
            Is.EqualTo(Encoding.UTF8.GetBytes("committed")),
            "A terminal read before its prepare must be deferred to pass 2, or the commit is lost.");
    }

    [Test]
    public async Task FoldTail_reseeds_frozen_pending_sagas_so_a_tail_terminal_resolves()
    {
        // The prepare was already in the leaf's in-memory bucket at freeze
        // time, so only the terminal lands in the tail. Without the pending
        // re-seed the terminal would drain an empty bucket and silently lose
        // the committed write.
        var tx = Guid.NewGuid();
        var (grain, _) = CreateFrozenBaselineGrain(
            sliceFactory: _ => (Head: 1, Entries: [new CommitLogSliceEntry(0, FrozenTerminal(tx, committed: true))]));

        var pending = new LeafBaselinePendingEntry(
            tx,
            "staged",
            new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes("survives"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 90 },
            },
            null,
            LatticeMergeMode.LwwRegister);

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(pending: [pending]),
            capturedHead: [1],
            CancellationToken.None);

        Assert.That(rows.Single(r => r.Key == "staged").Value.Value,
            Is.EqualTo(Encoding.UTF8.GetBytes("survives")));
    }

    [Test]
    public async Task FoldTail_drops_an_aborted_saga_reseeded_from_the_freeze()
    {
        var tx = Guid.NewGuid();
        var (grain, _) = CreateFrozenBaselineGrain(
            sliceFactory: _ => (Head: 1, Entries: [new CommitLogSliceEntry(0, FrozenTerminal(tx, committed: false))]));

        var pending = new LeafBaselinePendingEntry(
            tx,
            "staged",
            new LwwValue<byte[]>
            {
                Value = Encoding.UTF8.GetBytes("rolled-back"),
                Timestamp = new HybridLogicalClock { WallClockTicks = 90 },
            },
            null,
            LatticeMergeMode.LwwRegister);

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(pending: [pending]),
            capturedHead: [1],
            CancellationToken.None);

        Assert.That(rows.Select(r => r.Key), Does.Not.Contain("staged"));
    }

    [Test]
    public async Task FoldTail_only_reads_the_partitions_both_sides_agree_on()
    {
        // The freeze carries two partitions but the shard root captured only
        // one head; the fold must clamp to the shorter of the two.
        var reads = new List<string>();
        var (grain, _) = CreateFrozenBaselineGrain(
            walPartitions: 2,
            sliceFactory: _ => (Head: 1, Entries: [new CommitLogSliceEntry(0, FrozenCommittedSet("k", Encoding.UTF8.GetBytes("v")))]));

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(frontier: [-1, -1]),
            capturedHead: [1],
            CancellationToken.None);

        Assert.That(reads, Is.Empty);
        Assert.That(rows.Select(r => r.Key), Is.EqualTo(new[] { "k" }));
    }

    [Test]
    public async Task FoldTail_pumps_multiple_slices_until_the_captured_head_is_reached()
    {
        // ReplaySliceBudget is large, so force multiple pumps with a
        // coordinator that serves one entry per call.
        var served = 0;
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var fromExclusive = call.ArgAt<long>(0);
                var toInclusive = call.ArgAt<long>(1);
                var next = fromExclusive + 1;
                served++;
                if (next > toInclusive)
                    return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]);
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
                    [new CommitLogSliceEntry(next, FrozenCommittedSet($"k{next}", Encoding.UTF8.GetBytes($"v{next}")))]);
            });
        var (grain, _) = CreateFrozenBaselineGrain(explicitCoordinator: coordinator);

        var rows = await grain.FoldTailOntoFrozenAsync(
            Freeze(frontier: [-1]),
            capturedHead: [3],
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(served, Is.GreaterThan(1), "The pump must issue one read per served slice.");
            Assert.That(rows.Select(r => r.Key), Is.EquivalentTo(new[] { "k0", "k1", "k2" }));
        });
    }

    [Test]
    public void FoldTail_rejects_a_null_freeze()
    {
        var (grain, _) = CreateFrozenBaselineGrain();

        Assert.That(
            async () => await grain.FoldTailOntoFrozenAsync(null!, [1], CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void FoldTail_rejects_a_null_captured_head()
    {
        var (grain, _) = CreateFrozenBaselineGrain();

        Assert.That(
            async () => await grain.FoldTailOntoFrozenAsync(Freeze(), null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void FoldTail_throws_when_tree_id_is_unset()
    {
        var (grain, _) = CreateFrozenBaselineGrain(treeId: null);

        Assert.That(
            async () => await grain.FoldTailOntoFrozenAsync(Freeze(), [1], CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>().And.Message.Contains("tree id is unset"));
    }

    [Test]
    public void FoldTail_honours_a_pre_cancelled_token()
    {
        var (grain, _) = CreateFrozenBaselineGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.FoldTailOntoFrozenAsync(Freeze(), [1], cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void FoldTail_honours_cancellation_raised_mid_pump()
    {
        using var cts = new CancellationTokenSource();
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        coordinator.ReadSliceAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>(
                    [new CommitLogSliceEntry(0, FrozenCommittedSet("k", Encoding.UTF8.GetBytes("v")))]);
            });
        var (grain, _) = CreateFrozenBaselineGrain(explicitCoordinator: coordinator);

        Assert.That(
            async () => await grain.FoldTailOntoFrozenAsync(Freeze(frontier: [-1]), [5], cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
