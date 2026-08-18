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
/// Regression coverage for the "fall off the log" recurrence that survived the
/// 9.0.1/9.0.3/9.0.4 fixes: the coverage-gated durable pin
/// (<c>ResolveDurablePinForPartition</c>) decided a partition was "empty" - and
/// therefore safe to RELEASE its Zero block pin and authorise WAL trim up to the
/// checkpoint - purely from the per-activation in-memory cache
/// (<c>ComputePartitionsWithLiveData</c> -&gt; <c>Cache.EnumerateRows</c>). That
/// cache does NOT reflect a leaf's durable data in the window between activation
/// and cache hydration:
/// <list type="bullet">
/// <item><description>a leaf reactivates cold and its snapshot rehydrate finds
/// nothing (in the incident, ZERO snapshots existed anywhere), so the cache is
/// empty even though the persisted projection checkpoint says the prefix
/// <c>[0, checkpoint]</c> was durably applied;</description></item>
/// <item><description>tombstone reaping / compaction can empty a checkpointed
/// partition's cache while its WAL prefix still must replay.</description></item>
/// </list>
/// Pre-fix, such a partition took the empty branch and reported
/// <c>(clock, checkpoint)</c>, releasing the block and licensing the shared-shard
/// WAL GC to trim a checkpointed, un-snapshotted prefix. The next cold rebuild
/// then replays from offset 0 over a WAL whose prefix is gone, coming up with its
/// checkpoint BELOW the WAL trim floor - the exact
/// <see cref="LeafProjectionStaleException"/> the incident wedged on (tree
/// <c>repo-context-vector-payload</c>, shard 0, partition 0: checkpoint 5459
/// below oldest-readable 6093, no snapshot).
/// <para>
/// The fix narrows the empty branch to <c>checkpoint &lt; 0</c> (genuinely
/// nothing applied): a partition with a durable checkpoint is coverage-gated
/// exactly like a cache-populated one, so with no snapshot it retains its Zero
/// block pin. The first test proves the pin at the exact defect seam; the second
/// drives the REAL <see cref="LeafCursorReporter"/> / <see cref="WalMaterialiserPinGrain"/>
/// / <see cref="LatticeWalGc"/> over an <see cref="InMemoryWalStorageProvider"/>
/// and proves the checkpointed prefix survives the GC (so a cold rebuild can
/// still replay it) instead of being trimmed away.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Durable_pin_blocks_when_checkpointed_partition_cache_is_empty_without_snapshot()
    {
        // The exact defect at the pin seam. Model a leaf that has reactivated
        // COLD: the durable projection checkpoint and the applied clock survived
        // in persisted state, but the per-activation cache is empty (no snapshot
        // rehydrated it - none was ever captured) and NO durable snapshot exists.
        var (grain, state, lastFlush, _) = CreateResidualLeaf(walPartitions: 1);

        state.State.ProjectionCheckpointOffset = 5;
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 500 };

        // The graceful-deactivation retention barrier flushes the durable
        // frontier (the same path that runs when a dormant leaf goes to sleep).
        // With an empty cache + a durable checkpoint + no snapshot, the pin MUST
        // be the Zero block pin so the WAL GC cannot trim the unrecoverable
        // checkpointed prefix.
        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null, "the deactivation retention barrier must flush a durable pin");
        Assert.That(reports!.Count, Is.EqualTo(1));

        // RED (pre-fix): reports[0] == (HLC(500:0), 5) -> block released, the GC
        //   is authorised to trim [0, 5] with no covering snapshot -> the next
        //   cold rebuild falls off the log.
        // GREEN (post-fix): reports[0] == (Zero, -1) -> the WAL prefix is retained.
        Assert.That(reports[0].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "a durably-checkpointed partition with an empty (unhydrated) cache and no covering " +
            "snapshot must retain its Zero block pin; trusting the transient in-memory cache to " +
            "release the block is the fall-off-the-log hole");
        Assert.That(reports[0].CheckpointOffset, Is.EqualTo(-1L),
            "the block pin's offset must be the -1 sentinel so the GC's offset floor cannot advance " +
            "past the un-snapshotted checkpointed prefix");
    }

    [Test]
    public async Task Empty_cache_checkpointed_leaf_retains_whole_wal_through_real_gc_without_snapshot()
    {
        // END-TO-END through the real reporter / pin store / WAL GC. A leaf that
        // reactivated cold (empty cache) but has a durable checkpoint and no
        // snapshot must, on its retention-barrier flush, leave a durable pin that
        // makes the REAL LatticeWalGc retain the entire WAL - including the
        // checkpointed prefix a cold rebuild will replay from offset 0. Pre-fix
        // the empty branch released the block, so the GC trimmed the prefix.
        var registry = new InMemoryWalCursorRegistry();

        var pinContext = Substitute.For<IGrainContext>();
        pinContext.GrainId.Returns(GrainId.Create("wal-materialiser-pin", PinSeamTreeId));
        var pinGrain = new WalMaterialiserPinGrain(
            pinContext, new FakePersistentState<WalMaterialiserPinState>(), PinOptionsMonitor());

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);

        var reporter = new LeafCursorReporter(registry, factory);
        var services = new ServiceCollection();
        services.AddSingleton<ILeafCursorReporter>(reporter);
        var activationServices = services.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "empty-cache-cold-leaf"));
        context.ActivationServices.Returns(activationServices);

        // Persisted state of a leaf that durably checkpointed partition 0 at
        // offset 1 (HLC 20) and then reactivated cold: the per-activation cache
        // is empty (no snapshot store is wired, so nothing rehydrates it).
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = PinSeamTreeId;
        state.State.ProjectionCheckpointOffset = 1;
        state.State.Clock = PinHlc(20);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions { WalPartitions = 1 },
            maxLeafKeys: 128, shardCount: 1, factory: factory);
        var leaf = new BPlusLeafGrain(
            context, state, factory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        // Retention-barrier flush of the durable frontier with an empty cache.
        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // Simulate the restart: the process-local registry is wiped, so only the
        // durable pin remains to protect the WAL.
        var freshRegistry = new InMemoryWalCursorRegistry();

        // WAL holds four entries. Offsets 0 and 1 are the checkpointed prefix a
        // cold rebuild must replay; a forward shipper at HLC 40 would otherwise
        // drag the HLC trim floor over them.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            PinSeamTreeId,
            0,
            new[]
            {
                PinWalEntry(0, PinHlc(10)),
                PinWalEntry(1, PinHlc(20)),
                PinWalEntry(2, PinHlc(30)),
                PinWalEntry(3, PinHlc(40)),
            },
            CancellationToken.None);
        await freshRegistry.ReportCursorAsync(PinSeamTreeId, "shipper", PinHlc(40));

        var gcServices = new ServiceCollection();
        gcServices.AddSingleton<IWalStorageProvider>(provider);
        gcServices.AddSingleton(factory);
        var gc = new LatticeWalGc(gcServices.BuildServiceProvider(), freshRegistry, PinOptionsMonitor());

        var report = await gc.RunOnceAsync(PinSeamTreeId);

        // GREEN (post-fix): the empty-cache checkpointed leaf's Zero block pin
        // DISABLES the cursor trim (MinCursor null), so nothing is trimmed.
        // RED (pre-fix): the released (HLC 20, offset 1) pin leaves MinCursor at
        // HLC 20 and the offset floor at 1, so offsets 0 and 1 are trimmed.
        Assert.That(report.MinCursor, Is.Null,
            "the empty-cache, durably-checkpointed, un-snapshotted leaf's block pin must disable the " +
            "cursor trim entirely; a released real-frontier pin would let the shipper cursor trim the " +
            "checkpointed prefix");

        var survivors = await SurvivingPinOffsetsAsync(provider);
        Assert.That(survivors, Does.Contain(0L).And.Contain(1L).And.Contain(2L).And.Contain(3L),
            "with no snapshot covering the prefix, EVERY WAL entry - including the checkpointed prefix " +
            "[0, 1] the cold rebuild replays from offset 0 - must survive the GC pass, otherwise the " +
            "leaf reactivates with its checkpoint below the WAL trim floor (LeafProjectionStaleException)");
    }

    [Test]
    public async Task Durable_pin_stays_live_for_genuinely_empty_partition_that_never_checkpointed()
    {
        // Liveness guard: the fix must NOT over-block. A partition that has
        // applied nothing durably (checkpoint == -1) AND holds no live cache row
        // is genuinely empty - there is no committed prefix to lose - so it must
        // keep reporting the real frontier (clock, -1) and let WAL trim proceed.
        // This is the ubiquitous empty-partition pin a multi-partition leaf
        // emits; blocking it would wedge WAL trim for the whole tree. This test
        // passes both before and after the fix (checkpoint < 0 behaviour is
        // unchanged); it locks in that the narrowing did not regress liveness.
        const int partitions = 2;
        var (grain, state, lastFlush, _) = CreateResidualLeaf(partitions);
        var projection = AsProjection(grain);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // Partition 0 is idle (never checkpointed); the only live data hashes to
        // a non-zero partition and is durably checkpointed at offset 3.
        state.State.ProjectionCheckpointOffset = -1L;
        projection.Apply(BuildSet(dataKey, System.Text.Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 3))
        {
            await projection.SetCheckpointOffsetAsync(3, default);
        }
        await projection.FlushCheckpointAsync(default);

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null);
        Assert.That(reports![0].Frontier, Is.GreaterThan(HybridLogicalClock.Zero),
            "a genuinely empty partition (nothing applied, no live cache row) must keep reporting the " +
            "real frontier so WAL trim stays live - the fix must not block it");
        Assert.That(reports[0].CheckpointOffset, Is.EqualTo(-1L),
            "the genuinely-empty partition reports the -1 sentinel offset, not a block");
    }

    // ---- Workload / crash-timing reproduction axes -------------------------
    //
    // The durability invariant (never trim past a data-bearing partition's
    // un-snapshotted checkpointed prefix) is UNCONDITIONAL: it must hold for
    // every workload and every crash schedule. These two tests encode the two
    // reproduction conditions that produce the wedge state observed in the
    // incident (checkpoint below trim-floor, ZERO snapshots) - a bursty
    // short-lived activation that deactivates before the snapshot cadence ever
    // fires, and a crash between a checkpoint flush and a snapshot capture -
    // and prove the coverage gate holds the block on the cold restart both
    // produce. Pre-fix, the empty branch released the block on the cold
    // restart's empty cache (fall off the log); post-fix it holds.

    private static long CheckpointForPartition(long[]? perPartition, long scalar, int partition)
        => partition == 0
            ? scalar
            : (perPartition is not null && partition < perPartition.Length ? perPartition[partition] : -1L);

    /// <summary>
    /// Builds a fresh leaf activation over a given persisted checkpoint state
    /// with an EMPTY projection cache and NO durable snapshot - the exact
    /// cold-restart shape (the cache is never persisted; with no snapshot
    /// nothing rehydrates it). Captures the durable-frontier pin flush so a
    /// test can assert what the restarted leaf reports for each partition.
    /// </summary>
    private static (BPlusLeafGrain Grain, Func<IReadOnlyList<MaterialiserPinReport>?> LastFlush)
        CreateColdRestartedLeaf(int walPartitions, HybridLogicalClock clock, long scalarCheckpoint, long[]? perPartitionOffsets)
    {
        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        // No durable snapshot exists for this leaf (LoadAsync -> null), so the
        // cold cache cannot be rehydrated and coverage stays -1.
        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var sc = new ServiceCollection();
        sc.AddSingleton<ILeafCursorReporter>(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ResidualTreeId;
        state.State.Clock = clock;
        state.State.ProjectionCheckpointOffset = scalarCheckpoint;
        if (perPartitionOffsets is not null)
            state.State.ProjectionCheckpointOffsetsByPartition = perPartitionOffsets;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = walPartitions,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);
        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        return (grain, () => captured);
    }

    [Test]
    public async Task Bursty_workload_that_never_snapshots_keeps_block_pin_on_cold_restart()
    {
        // AXIS (a): bursty short-lived activation. A leaf activates, takes a
        // handful of writes, advances and flushes its checkpoint a few times,
        // then deactivates - all well under LeafSnapshotReClassifyEveryNCheckpoints
        // (64), so the per-activation periodic recheck never captures, and the
        // margin-gated activation-time advisory never ran (no OnActivateAsync).
        // Result: NO snapshot is ever captured (the incident's "zero snapshots"
        // census). The invariant must still hold on the next cold restart.
        const int partitions = 2;
        var (warm, warmState, _, snapshotStub) = CreateResidualLeaf(partitions, reclassifyEveryN: 64);
        var projection = AsProjection(warm);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        for (long off = 0; off < 3; off++)
        {
            projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes($"v{off}"), hlcPhysical: 100 + off, treeId: ResidualTreeId));
            using (LatticeApplyOffsetContext.BeginScope(dataPartition, off))
            {
                await projection.SetCheckpointOffsetAsync(off, default);
            }
            await projection.FlushCheckpointAsync(default);
        }

        // H2 precondition, asserted: the bursty cadence captured NOTHING.
        await snapshotStub.DidNotReceive().SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>());

        var scalar = warmState.State.ProjectionCheckpointOffset;
        var perPartition = warmState.State.ProjectionCheckpointOffsetsByPartition;
        var clock = warmState.State.Clock;
        Assert.That(CheckpointForPartition(perPartition, scalar, dataPartition), Is.GreaterThanOrEqualTo(0L),
            "precondition: the data partition durably advanced its checkpoint during the bursty activation");

        // Cold restart over the SAME persisted checkpoint state, empty cache,
        // no snapshot. Pre-fix the empty branch releases (clock, checkpoint) and
        // authorises the trim that falls off the log; post-fix the coverage gate
        // holds the Zero block.
        var (cold, coldFlush) = CreateColdRestartedLeaf(partitions, clock, scalar, perPartition);
        await ((IGrainBase)cold).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        var reports = coldFlush();
        Assert.That(reports, Is.Not.Null);
        Assert.That(reports![dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "under a bursty workload that never captured a snapshot, the cold-restarted leaf's " +
            "durably-checkpointed data partition must retain its Zero block pin - the empty cold cache " +
            "must not be trusted to release it");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L));
    }

    [Test]
    public async Task Crash_between_checkpoint_flush_and_snapshot_capture_keeps_block_pin_on_cold_restart()
    {
        // AXIS (b): a crash between a checkpoint flush and a snapshot capture.
        // The leaf flushes a durable checkpoint, then the snapshot capture the
        // cadence triggers FAILS to durably land (modelled by the snapshot store
        // throwing - equivalent to a SIGKILL after the checkpoint write but
        // before SaveAsync commits). Coverage never advances (it is recorded
        // only after a confirmed SaveAsync), so on restart there is a durable
        // checkpoint but no covering snapshot - exactly the incident shape.
        const int partitions = 2;
        var (warm, warmState, _, snapshotStub) = CreateResidualLeaf(partitions, reclassifyEveryN: 1);
        var projection = AsProjection(warm);
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // The capture attempt at the checkpoint boundary crashes before it lands.
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns<Task>(_ => throw new InvalidOperationException("crash before the snapshot durably lands"));

        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 0))
        {
            await projection.SetCheckpointOffsetAsync(0, default);
        }
        await projection.FlushCheckpointAsync(default);
        // No OnDeactivateAsync: the process was killed. No durable snapshot exists.

        var scalar = warmState.State.ProjectionCheckpointOffset;
        var perPartition = warmState.State.ProjectionCheckpointOffsetsByPartition;
        var clock = warmState.State.Clock;
        Assert.That(CheckpointForPartition(perPartition, scalar, dataPartition), Is.GreaterThanOrEqualTo(0L),
            "precondition: the checkpoint flushed durably before the crash");

        var (cold, coldFlush) = CreateColdRestartedLeaf(partitions, clock, scalar, perPartition);
        await ((IGrainBase)cold).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        var reports = coldFlush();
        Assert.That(reports, Is.Not.Null);
        Assert.That(reports![dataPartition].Frontier, Is.EqualTo(HybridLogicalClock.Zero),
            "after a crash between the checkpoint flush and the snapshot capture, the cold-restarted " +
            "leaf's checkpointed data partition has no covering snapshot and must retain its Zero block pin");
        Assert.That(reports[dataPartition].CheckpointOffset, Is.EqualTo(-1L));
    }
}
