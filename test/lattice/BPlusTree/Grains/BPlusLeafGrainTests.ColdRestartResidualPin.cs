using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the RESIDUAL cold-restart prefix-loss defect that
/// survived PR #1490. #1490 fixed the UNBOUNDED-growth class (an un-checkpointed
/// data partition whose block pin was overwritten by a real frontier). The
/// residual is a distinct, BOUNDED loss: a partition that HAS durably
/// checkpointed its prefix <c>[0, checkpoint]</c> but for which NO durable
/// snapshot covers that prefix. The pre-fix
/// <c>ResolveDurablePinForPartition</c> reported <c>(clock, checkpoint)</c> for
/// such a partition, which authorises the shared-shard WAL GC to trim
/// <c>[0, checkpoint]</c>. Because the leaf's per-activation projection cache is
/// NOT persisted (it is rebuilt from the WAL on every activation), a cold
/// restart then replays from offset 0 over a WAL whose prefix has been trimmed -
/// silently losing exactly the checkpointed prefix. This reproduced in the
/// repocontext store as a stable ~580-node loss per <c>docker compose restart</c>.
/// <para>
/// The fix couples three parts: (1) a coverage-gated trim floor - the durable
/// pin authorises trimming only up to <c>min(checkpoint, snapshotCoveredOffset)</c>
/// and retains a Zero block pin when the checkpointed prefix is not
/// snapshot-covered; (2) a guaranteed cadence snapshot capture so every block
/// pin has a bounded path to coverage and trim (invariant (b): WAL stays
/// bounded); (3) per-partition snapshot capture + a tail-aware rehydrate that
/// accepts a snapshot that is the sole durable coverage of a trimmed prefix.
/// </para>
/// <para>
/// These tests isolate the residual mechanism at the durable-pin seam - a seam
/// the #1490 block-pin overload does NOT touch (the #1490 case is
/// <c>checkpoint &lt; 0</c>; the residual is <c>checkpoint &gt;= 0 &amp;&amp;
/// !covered</c>). The RED failure below is provably the residual and not #1490
/// because the partition under test IS durably checkpointed (offset &gt;= 0).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ResidualTreeId = "tree-cold-restart-residual";

    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        Func<IReadOnlyList<MaterialiserPinReport>?> LastFlush,
        ILeafSnapshotStorageGrain SnapshotStub)
        CreateResidualLeaf(int walPartitions, long coordinatorTail = 0, int reclassifyEveryN = 0)
    {
        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(coordinatorTail));

        var leafKey = Guid.NewGuid();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ResidualTreeId;
        state.State.ProjectionCheckpointOffset = 0;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                LeafSnapshotReClassifyEveryNCheckpoints = reclassifyEveryN,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state, () => captured, snapshotStub);
    }

    /// <summary>
    /// Picks the lowest test key that hashes to a WAL partition other than 0
    /// under <paramref name="walPartitions"/>. Multi-partition regression tests
    /// deliberately drive a NON-ZERO partition so a bug that gates capture or
    /// coverage on partition 0's scalar checkpoint is exposed rather than hidden.
    /// </summary>
    private static (string Key, int Partition) FirstKeyInNonZeroPartition(int walPartitions)
    {
        var key = Enumerable.Range(0, 65536)
            .Select(i => $"k{i}")
            .First(k => WalPartitionHash.Compute(k, walPartitions) != 0);
        return (key, WalPartitionHash.Compute(key, walPartitions));
    }

    [Test]
    public async Task Residual_cold_restart_loses_checkpointed_prefix_when_trimmed_to_checkpoint_without_snapshot()
    {
        // INVARIANT (a) - NO cold-restart loss. A partition that holds live
        // data AND has durably checkpointed its prefix, but for which no
        // durable snapshot exists, must retain a Zero BLOCK pin so the WAL GC
        // cannot trim the checkpointed prefix. Reporting (clock, checkpoint)
        // here - the PRE-FIX behaviour - authorises the GC to trim [0, 1],
        // after which a cold rebuild (empty per-activation cache -> replay from
        // 0) silently loses the prefix. This is the residual ~580-node loss.
        var (grain, _, lastFlush, _) = CreateResidualLeaf(walPartitions: 1);
        var projection = AsProjection(grain);

        // Apply a foreground write (advances the leaf clock past Zero, stores a
        // row in partition 0's cache) and DURABLY checkpoint partition 0 at
        // offset 1. This is the residual precondition: checkpoint >= 0 with
        // live data, no snapshot. It is provably NOT the #1490 case, which
        // requires checkpoint < 0.
        projection.Apply(BuildSet("k0", Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ResidualTreeId));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null, "the first checkpoint flush must fire the durable-frontier barrier");
        Assert.That(reports!.Count, Is.EqualTo(1));

        // RED (pre-fix): reports[0] == (clock > Zero, 1) -> GC trims [0, 1].
        // GREEN (post-fix): reports[0] == (Zero, -1) block -> WAL retained.
        Assert.That(reports[0].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "a checkpointed, data-bearing, snapshot-UNCOVERED partition must retain its Zero block pin");
        Assert.That(reports[0].CheckpointOffset, Is.EqualTo(-1L),
            "the block pin's offset must be the -1 sentinel so the GC's offset floor cannot advance");
    }

    [Test]
    public async Task Block_pin_lifts_to_covered_offset_after_cadence_snapshot_capture()
    {
        // INVARIANT (b) - WAL BOUNDED. The Zero block pin from invariant (a) is
        // not permanent: once a durable snapshot covers the checkpointed
        // prefix, the pin advances to min(checkpoint, coveredOffset) and the
        // WAL GC can trim the now-recoverable prefix. Part 2 (the guaranteed
        // cadence capture) makes that coverage inevitable, so a block pin
        // always has a bounded path to coverage-and-trim - this is what
        // prevents the fix from reintroducing the #1489/#1490 unbounded-growth
        // class. Here we capture a snapshot explicitly and prove the very next
        // durable-frontier flush lifts the block.
        var (grain, _, lastFlush, _) = CreateResidualLeaf(walPartitions: 1);
        var projection = AsProjection(grain);

        projection.Apply(BuildSet("k0", Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ResidualTreeId));
        await projection.SetCheckpointOffsetAsync(1, default);
        await projection.FlushCheckpointAsync(default);

        // Before coverage: the pin is a Zero block (invariant (a)).
        var blocked = lastFlush();
        Assert.That(blocked![0].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "precondition: uncovered checkpointed prefix reports a Zero block pin");

        // Capture a durable snapshot covering the checkpointed prefix [0, 1].
        // This stamps per-partition coverage at offset 1.
        await grain.CaptureSnapshotAsync();

        // Re-flush the durable frontier (the deactivation path always flushes).
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        var lifted = lastFlush();
        Assert.That(lifted![0].Frontier, Is.GreaterThan(HybridLogicalClock.Zero),
            "once a durable snapshot covers the prefix, the block pin lifts to the real frontier");
        Assert.That(lifted[0].CheckpointOffset, Is.EqualTo(1L),
            "the lifted pin authorises trimming up to the snapshot-covered offset (min(checkpoint, covered) = 1)");
    }

    [Test]
    public async Task Capture_covers_a_non_zero_partition_when_partition_zero_is_idle()
    {
        // MULTI-PARTITION regression for capture-starvation issue 1(a). Under
        // the default WalPartitions = 8 a leaf's live keys can hash entirely to
        // a NON-ZERO partition while partition 0 stays idle (checkpoint == -1).
        // The coverage-gated durable pin BLOCK-pins such a partition (it holds
        // committed, un-trimmable WAL), so its bounded-WAL guarantee depends on
        // the cadence capture eventually covering it. The pre-fix
        // CaptureSnapshotAsync early-returned on `ProjectionCheckpointOffset < 0`
        // - partition 0's SCALAR - so when partition 0 idled the capture NEVER
        // ran, coverage for the busy partition never advanced, its block pin was
        // held forever and its WAL grew unbounded (reopening #1489/#1490). The
        // fix proceeds with capture when ANY partition is checkpointed. A
        // WalPartitions = 1 test cannot see this because partition 0 is the only
        // partition. RED (pre-fix): coverage stays -1 (capture skipped).
        // GREEN (post-fix): coverage advances to the busy partition's checkpoint.
        const int partitions = 8;
        var (grain, state, lastFlush, snapshotStub) = CreateResidualLeaf(partitions);
        var projection = AsProjection(grain);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // Partition 0 is idle: never checkpointed. The only live data is in a
        // non-zero partition, durably checkpointed at offset 3.
        state.State.ProjectionCheckpointOffset = -1L;
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 3))
        {
            await projection.SetCheckpointOffsetAsync(3, default);
        }
        await projection.FlushCheckpointAsync(default);

        Assert.That(grain.GetCurrentCheckpointForPartition(dataPartition), Is.EqualTo(3L),
            "precondition: the non-zero partition is durably checkpointed at 3");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L),
            "precondition: partition 0 is idle (never checkpointed)");

        await grain.CaptureSnapshotAsync();

        // GREEN: capture ran despite partition 0 being idle, so the busy
        // partition's checkpointed prefix is now durably covered.
        await snapshotStub.Received().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(3L),
            "capture MUST cover a checkpointed non-zero partition even when partition 0 is idle; " +
            "gating on partition 0's scalar starves it and its block-pinned WAL grows unbounded");

        // Bounded (invariant b): the now-covered partition's block pin lifts to
        // the covered offset on the next durable-frontier flush, so the GC can
        // trim the recoverable prefix.
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        var reports = lastFlush();
        Assert.That(reports![dataPartition].Frontier, Is.GreaterThan(HybridLogicalClock.Zero),
            "the covered partition's block pin must lift so its WAL becomes trimmable");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(3L));
    }

    [Test]
    public async Task Periodic_recheck_covers_a_busy_non_zero_partition_while_partition_zero_idles()
    {
        // MULTI-PARTITION regression for capture-starvation issue 1(b): FROZEN
        // COVERAGE. Once partition 0 has captured once, the pre-fix periodic
        // recheck short-circuited on `ProjectionCheckpointOffset ==
        // _lastCapturedCheckpointOffset` - both partition 0's SCALAR. So if
        // partition 0 then idled while another partition took heavy writes past
        // the cadence threshold, the recheck kept short-circuiting, that
        // partition's coverage froze at its stale value, its durable pin froze
        // at min(checkpoint, frozenCovered) and its retained WAL grew unbounded.
        // The fix debounces PER PARTITION (capture when ANY partition's current
        // checkpoint has advanced beyond its recorded coverage). A
        // WalPartitions = 1 test cannot see this. RED (pre-fix): coverage frozen
        // at -1 for the busy partition. GREEN (post-fix): coverage advances.
        const int partitions = 8;
        var (grain, _, lastFlush, _) = CreateResidualLeaf(partitions, reclassifyEveryN: 1);
        var projection = AsProjection(grain);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // Partition 0 captures once (created with checkpoint 0), establishing
        // the frozen scalar the pre-fix debounce keys on.
        await grain.CaptureSnapshotAsync();
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(0L),
            "precondition: partition 0 is covered at 0");

        // Partition 0 now idles. A non-zero partition takes a write and
        // checkpoints past it. The cadence recheck (threshold 1) fires on the
        // flush; it MUST notice the busy partition advanced beyond coverage.
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 700, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 5))
        {
            await projection.SetCheckpointOffsetAsync(5, default);
        }
        await projection.FlushCheckpointAsync(default);

        // GREEN: the busy partition is now covered at its checkpoint. RED
        // (pre-fix): the partition-0 scalar debounce short-circuited the
        // recheck, leaving coverage frozen at -1.
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(5L),
            "the periodic recheck MUST cover a busy non-zero partition even while partition 0 idles; " +
            "the partition-0 scalar debounce freezes its coverage and its WAL grows unbounded");

        // Bounded (invariant b): the newly-covered partition's pin advances to
        // the covered offset, so its WAL is trimmable rather than pinned forever.
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        var reports = lastFlush();
        Assert.That(reports![dataPartition].Frontier, Is.GreaterThan(HybridLogicalClock.Zero),
            "the covered busy partition's pin must lift so its retained WAL stays bounded");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(5L));
    }

    [Test]
    public async Task Rehydrate_resets_uncovered_partition_checkpoint_so_retained_prefix_is_not_skipped()
    {
        // MULTI-PARTITION regression for rehydrate issue 2 (projection loss).
        // On the rehydrate ACCEPT path, after Cache.Clear() reloads ONLY the
        // snapshot rows, every partition's checkpoint must equal what the
        // reloaded cache actually contains. The pre-fix per-partition loop
        // SKIPPED partitions whose snapshot offset was -1 (`if (perPartition[p]
        // >= 0)`), leaving a partition's checkpoint AHEAD of the reloaded cache
        // whenever the loaded blob predated that partition's checkpoint. The
        // tail replay then resumed at (checkpoint_p, head] and SKIPPED
        // [0, checkpoint_p] - dropping those entries, which are in neither the
        // stale snapshot rows nor the replay window. Resetting to -1 is
        // loss-free: an uncovered partition (perPartition[p] == -1 on the latest
        // blob) was never snapshot-covered, so the coverage gate held its block
        // pin and its full WAL survives - the from-zero replay rebuilds it
        // intact. A WalPartitions = 1 test cannot see this (partition 0 is
        // special-cased). RED (pre-fix): checkpoint left at 7. GREEN: reset to -1.
        const int partitions = 8;
        // coordinatorTail = 1 makes AnyPartitionWalPrefixTrimmedAsync report a
        // trimmed prefix, so the rehydrate takes the ACCEPT path it is built for.
        var (grain, state, _, snapshotStub) = CreateResidualLeaf(partitions, coordinatorTail: 1);
        var (_, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // Persisted state: partition 0 checkpointed at 0; the non-zero partition
        // holds data checkpointed at offset 7 (its uncovered, block-pinned WAL
        // [0, 7] is retained). No pending.
        state.State.ProjectionCheckpointOffset = 0L;
        var persisted = new long[partitions];
        Array.Fill(persisted, -1L);
        persisted[0] = 0L;
        persisted[dataPartition] = 7L;
        state.State.ProjectionCheckpointOffsetsByPartition = persisted;

        // The loaded (latest durable) blob predates the non-zero partition's
        // checkpoint: it covers partition 0 only, so its per-partition offset
        // for the data partition is the -1 sentinel.
        var blobOffsets = new long[partitions];
        Array.Fill(blobOffsets, -1L);
        blobOffsets[0] = 0L;
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 0L,
            Rows = new List<LeafSnapshotRow>(),
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = blobOffsets,
        };
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(blob));

        var accepted = await grain.TryRehydrateFromSnapshotAsync(default);
        Assert.That(accepted, Is.True, "the snapshot is the sole durable coverage of a trimmed prefix, so rehydrate must accept");

        // GREEN: the uncovered partition's checkpoint is reset to -1 so the tail
        // replay covers its retained [0, 7] prefix. RED (pre-fix): the loop
        // skipped it and left the checkpoint at 7, so replay would resume at
        // (7, head] and silently drop [0, 7].
        Assert.That(grain.GetCurrentCheckpointForPartition(dataPartition), Is.EqualTo(-1L),
            "an uncovered partition's checkpoint MUST be reset to -1 on rehydrate so its retained WAL prefix is replayed, not skipped");
        // The safety coupling that makes the reset loss-free: the partition is
        // uncovered, so the coverage gate never authorised trimming its prefix.
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
            "the reset is loss-free precisely because the partition is uncovered and its full WAL is retained");
    }

    /// <summary>
    /// Variant of <see cref="CreateResidualLeaf"/> that wires the leaf to a
    /// caller-supplied <see cref="ILeafSnapshotStorageGrain"/> (typically the
    /// REAL <see cref="LeafSnapshotStorageGrain"/> backed by an in-memory
    /// persistent state) so a test can round-trip a captured blob through the
    /// production load/save seam - including the scalar-sentinel load guard that
    /// a stubbed <c>LoadAsync</c> bypasses.
    /// </summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State)
        CreateResidualLeafWithSnapshotStore(
            int walPartitions,
            ILeafSnapshotStorageGrain snapshotStore,
            long coordinatorTail = 0,
            int reclassifyEveryN = 0)
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(coordinatorTail));

        var leafKey = Guid.NewGuid();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStore);
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coord);

        var sc = new ServiceCollection();
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", leafKey.ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ResidualTreeId;
        state.State.ProjectionCheckpointOffset = 0;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                LeafSnapshotReClassifyEveryNCheckpoints = reclassifyEveryN,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state);
    }

    [Test]
    public async Task Partition_zero_idle_capture_writes_a_loadable_snapshot_through_real_storage()
    {
        // MULTI-PARTITION regression for the capture-writes-an-UNLOADABLE-snapshot
        // defect the issue-1(a) fix introduced. Proceeding with capture when ANY
        // partition is checkpointed (correct) stamped the blob's SCALAR
        // SnapshotOffset from partition 0's checkpoint, which is the -1 "idle"
        // sentinel when a leaf's live data hashes only to a non-zero partition.
        // The REAL LeafSnapshotStorageGrain.LoadAsync treated SnapshotOffset < 0
        // as "nothing captured" and returned null, so on cold restart the SOLE
        // durable copy of the busy partition's checkpointed prefix was discarded -
        // and the coverage-gated WAL GC had already trimmed [0, checkpoint], so
        // the prefix was unrecoverable: SILENT LOSS. The three stubbed-LoadAsync
        // multi-partition tests missed this because their stub never exercises
        // the scalar-sentinel load guard. This test round-trips through the real
        // storage grain. The fix (LeafSnapshotStorageGrain.HasCapturedPrefix)
        // recognises a per-partition-only blob (any SnapshotOffsetsByPartition
        // slot >= 0 is loadable) while keeping legacy scalar-only semantics.
        // RED (pre-fix): LoadAsync returns null. GREEN: the blob loads and a
        // cold leaf rehydrates the busy partition's coverage.
        const int partitions = 8;
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // Real storage grain backed by an in-memory persistent state.
        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var snapshotContext = Substitute.For<IGrainContext>();
        var realStore = new LeafSnapshotStorageGrain(snapshotContext, snapshotState);

        // A "warm" leaf whose live data is in a non-zero partition, checkpointed
        // at 3, while partition 0 stays idle at -1. coordinatorTail = 1 makes the
        // cold leaf below take the rehydrate ACCEPT path (a prefix was trimmed).
        var (warm, warmState) = CreateResidualLeafWithSnapshotStore(partitions, realStore, coordinatorTail: 1);
        var projection = AsProjection(warm);

        warmState.State.ProjectionCheckpointOffset = -1L;
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 3))
        {
            await projection.SetCheckpointOffsetAsync(3, default);
        }
        await projection.FlushCheckpointAsync(default);

        await warm.CaptureSnapshotAsync();

        // The durably-written blob carries the partition-0-idle scalar sentinel
        // yet DOES cover the busy partition per-partition. This is the exact
        // shape the load guard must not discard.
        Assert.That(snapshotState.State.SnapshotOffset, Is.EqualTo(-1L),
            "precondition: partition 0 is idle so the blob's scalar offset is the -1 sentinel");
        Assert.That(snapshotState.State.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(snapshotState.State.SnapshotOffsetsByPartition![dataPartition], Is.EqualTo(3L),
            "precondition: the blob DOES cover the busy non-zero partition at its checkpoint");

        // RED (pre-fix): the real load guard sees SnapshotOffset < 0 and returns
        // null, discarding the only durable copy. GREEN (post-fix): a
        // per-partition-covered blob is loadable.
        var loaded = await realStore.LoadAsync(default);
        Assert.That(loaded, Is.Not.Null,
            "a per-partition-covered blob MUST be loadable even when partition 0's scalar offset is the -1 " +
            "sentinel; otherwise cold restart discards the sole durable copy and the coverage-gated GC has " +
            "already trimmed the prefix -> silent loss");

        // End-to-end no-loss: a COLD leaf sharing the same durable store
        // rehydrates the blob and restores the busy partition's coverage, so its
        // trimmed prefix is recoverable (accept path requires LoadAsync != null).
        var (cold, coldState) = CreateResidualLeafWithSnapshotStore(partitions, realStore, coordinatorTail: 1);
        coldState.State.ProjectionCheckpointOffset = -1L;
        var coldPartitionOffsets = new long[partitions];
        Array.Fill(coldPartitionOffsets, -1L);
        coldPartitionOffsets[dataPartition] = 3L;
        coldState.State.ProjectionCheckpointOffsetsByPartition = coldPartitionOffsets;

        var accepted = await cold.TryRehydrateFromSnapshotAsync(default);
        Assert.That(accepted, Is.True,
            "the cold leaf must accept the sole durable snapshot; pre-fix LoadAsync returned null so rehydrate " +
            "returned false and the cache was left empty over a trimmed WAL");
        Assert.That(cold.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(3L),
            "rehydrate must restore the busy partition's durable coverage from the loaded blob");
    }
}
