using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #1542 - the residual snapshot-cadence liveness
/// gap #1537 leaves open. #1537 captures a durable snapshot on graceful
/// deactivation so a bursty leaf that <b>advanced a checkpoint this activation</b>
/// lifts its <see cref="HybridLogicalClock.Zero"/> block pin before going
/// dormant. The residual is an <b>already-converged</b> leaf: its persisted
/// checkpoint is already at the WAL head and no durable snapshot covers it, so it
/// holds a Zero block pin. On a cold reactivation it fully rebuilds its cache
/// from the start of the readable WAL (the <c>checkpointOverride = -1</c> path),
/// but because the checkpoint does not move,
/// <c>_checkpointAdvancedThisActivation</c> stays <c>false</c> and #1537's
/// deactivation capture is (conservatively) suppressed. All three capture
/// triggers then fail and the block pin - and the shared-shard WAL it retains -
/// is held forever.
/// <para>
/// The fix recognises the full cold rebuild as an equally-safe capture
/// precondition: the cache holds the entire readable window, a superset of the
/// checkpointed prefix, so stamping each partition's checkpoint records coverage
/// truthfully. The one shape that could make a <c>-1</c> rebuild unfaithful - a
/// trimmed WAL prefix with no covering snapshot - throws
/// <see cref="LeafProjectionStaleException"/> at activation (the #945
/// durable-frontier fall-off guard) before the capture is ever reached, so the
/// broadened capture can never authorise trimming a lost prefix (the #1535
/// no-loss invariant).
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string ColdRebuildTreeId = "tree-cold-rebuild-liveness";

    private static CommitLogSliceEntry ColdRebuildSet(long offset, string key) =>
        new(offset, BuildCommittedSet(key, System.Text.Encoding.UTF8.GetBytes($"v-{key}"), treeId: ColdRebuildTreeId));

    /// <summary>
    /// Builds a leaf wired for the cold-rebuild scenario: a chunking replay
    /// coordinator (WAL head/tail plus sliceable entries), an in-memory snapshot
    /// store, and a frontier-capturing <see cref="ILeafCursorReporter"/> so the
    /// resolved durable pin from the deactivation flush is observable.
    /// </summary>
    private static (BPlusLeafGrain Grain,
        FakePersistentState<LeafNodeState> State,
        Func<IReadOnlyList<MaterialiserPinReport>?> LastFlush)
        BuildColdRebuildLeaf(
            ILeafReplayCoordinatorGrain coordinator,
            ILeafSnapshotStorageGrain snapshotStub,
            long persistedCheckpoint)
    {
        IReadOnlyList<MaterialiserPinReport>? captured = null;
        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Do<IReadOnlyList<MaterialiserPinReport>>(r => captured = r),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = ColdRebuildTreeId;
        state.State.ProjectionCheckpointOffset = persistedCheckpoint;

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                WalPartitions = 1,
                MaterialiserCheckpointInterval = TimeSpan.Zero,
                // Periodic recheck disabled so the ONLY capture driver under test
                // is the graceful-deactivation hook - the residual-gap seam.
                LeafSnapshotReClassifyEveryNCheckpoints = 0,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        var grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());

        return (grain, state, () => captured);
    }

    [Test]
    public async Task Converged_cold_rebuilt_leaf_captures_on_deactivation_and_lifts_block_pin()
    {
        // The residual #1542 scenario. A data-bearing leaf durably checkpointed
        // its whole prefix [1, 3] (checkpoint == head == 3) but no snapshot
        // covers it, so it holds a Zero block pin. It reactivates cold: the
        // per-activation cache starts empty and no snapshot rehydrates it, so the
        // replay runs from the -1 sentinel and rebuilds the full cache. The
        // checkpoint does NOT advance (already at head), so this is provably the
        // residual case and not the #1537 checkpoint-advance case.
        var store = new InMemorySnapshotStore();
        var coord = BuildChunkingCoordinator(
            head: 3, sliceSize: 8, tail: 0,
            ColdRebuildSet(1, "k1"), ColdRebuildSet(2, "k2"), ColdRebuildSet(3, "k3"));
        var (grain, state, lastFlush) = BuildColdRebuildLeaf(coord, store.Stub, persistedCheckpoint: 3);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        // The cache was rebuilt from the WAL start (all three keys present) but
        // nothing advanced the checkpoint and no capture fired during activation.
        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
            "converged leaf: checkpoint must stay at head - no forward advance this activation");
        Assert.That(store.SaveCount, Is.EqualTo(0),
            "no capture during activation: neither the advisory nor the periodic recheck fires for a converged block-pinned leaf");
        foreach (var k in new[] { "k1", "k2", "k3" })
            Assert.That(await grain.GetAsync(k), Is.Not.Null, $"key {k} must be present after the full cold rebuild");

        await ((IGrainBase)grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);

        // GREEN (post-fix): the cold-rebuild signal authorises the deactivation
        // capture, so a durable snapshot covers the checkpointed prefix and the
        // durable-frontier flush lifts the Zero block pin to the real frontier.
        // RED (pre-fix): SaveCount stays 0 and the flush reports (Zero, -1).
        Assert.That(store.SaveCount, Is.EqualTo(1),
            "the converged cold-rebuilt leaf must capture a snapshot on graceful deactivation (#1542)");
        Assert.That(store.Latest!.SnapshotOffset, Is.EqualTo(3L),
            "the deactivation snapshot must cover the full checkpointed prefix");

        var reports = lastFlush();
        Assert.That(reports, Is.Not.Null.And.Count.EqualTo(1),
            "the deactivation path must flush the durable frontier");
        Assert.That(reports![0].Frontier, Is.GreaterThan(HybridLogicalClock.Zero),
            "once the deactivation snapshot covers the prefix, the block pin lifts to the real frontier");
        Assert.That(reports[0].CheckpointOffset, Is.EqualTo(3L),
            "the lifted pin authorises trimming up to the now-covered checkpoint offset");
    }

    [Test]
    public async Task Cold_rebuild_over_a_lost_prefix_throws_and_never_captures()
    {
        // The invariant that keeps the broadened capture safe. A converged,
        // snapshot-less leaf whose WAL prefix has fallen off the log (the oldest
        // readable offset 5 is past the first offset this leaf still needs,
        // checkpoint + 1 == 4) cannot be faithfully rebuilt from the WAL alone.
        // The #945 durable-frontier fall-off guard must throw at activation - so
        // the cold-rebuild capture signal is never latched and no snapshot
        // claiming coverage of the lost prefix is ever written.
        var store = new InMemorySnapshotStore();
        var coord = BuildChunkingCoordinator(
            head: 10, sliceSize: 8, tail: 5,
            ColdRebuildSet(6, "k6"), ColdRebuildSet(7, "k7"), ColdRebuildSet(8, "k8"),
            ColdRebuildSet(9, "k9"), ColdRebuildSet(10, "k10"));
        var (grain, _, _) = BuildColdRebuildLeaf(coord, store.Stub, persistedCheckpoint: 3);

        Assert.ThrowsAsync<LeafProjectionStaleException>(
            async () => await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None),
            "a cold rebuild over a trimmed, un-snapshotted prefix must fail rather than silently rebuild over lost data");
        Assert.That(store.SaveCount, Is.EqualTo(0),
            "no snapshot may be captured when the cache cannot faithfully hold the checkpointed prefix (the #1535 no-loss invariant)");
    }
}
