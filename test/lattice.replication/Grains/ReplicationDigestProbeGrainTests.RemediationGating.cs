using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the gated repair-stage orchestration in <c>TryRemediateAsync</c>
/// and the two repair helpers it drives. Every automatic-remediation action is
/// opt-in and further guarded by a per-(tree, peer) circuit breaker and traffic
/// budget; detection is never gated. These tests prove each gate's skip arm
/// (opt-out master flag, open circuit, spent budget), the failure branch that
/// opens the breaker when a re-ship is rejected, and the disabled/faulted arms of
/// the targeted leaf re-replay and the scoped bootstrap-snapshot fallback. The
/// circuit and budget skips need two shards in one pass because the guard state
/// is per-activation: the first shard trips the guard, the second observes it.
/// </summary>
public partial class ReplicationDigestProbeGrainTests
{
    private static (
        ReplicationDigestProbeGrain Grain,
        IReplicationTransport ReplicationTransport) CreateGatingGrain(
            int shardCount = 1,
            bool autoRemediate = true,
            bool leafReReplayEnabled = true,
            bool bootstrapFallbackEnabled = true,
            bool sendAccepted = true,
            HybridLogicalClock? peerCursor = null,
            ISnapshotProvider? snapshotProvider = null,
            Exception? highWaterMarkFault = null,
            int shipBatchSize = 256,
            double trafficBudgetFraction = 0.01,
            int failureThreshold = 3)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("digest-probe-grain", Tree));
        var reminders = Substitute.For<IReminderRegistry>();

        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var replicationOptions = new LatticeReplicationOptions
        {
            ClusterId = Origin,
            DigestProbeEnabled = true,
            DigestProbeInterval = TimeSpan.FromMinutes(5),
            DigestProbeJitter = 0.0,
            MerkleWalkEnabled = true,
            MerkleWalkMaxDepth = 8,
            MerkleWalkMaxBytes = 1 << 20,
            LeafReReplayEnabled = leafReReplayEnabled,
            LeafReReplayMaxEntries = 4096,
            LeafReReplayMaxBytes = 1 << 20,
            BootstrapFallbackEnabled = bootstrapFallbackEnabled,
            BootstrapFallbackMaxEntries = 4096,
            BootstrapFallbackMaxBytes = 1 << 20,
            AutoRemediateOnDigestMismatch = autoRemediate,
            ReplogPartitions = 1,
            ShipPartitionPageSize = 256,
            ShipBatchSize = shipBatchSize,
            RemediationTrafficBudgetFraction = trafficBudgetFraction,
            RemediationFailureThreshold = failureThreshold,
            RemediationTrafficWindow = TimeSpan.FromMinutes(10),
            RemediationCircuitResetInterval = TimeSpan.FromMinutes(30),
        };
        replicationMonitor.CurrentValue.Returns(replicationOptions);
        replicationMonitor.Get(Arg.Any<string>()).Returns(replicationOptions);

        var latticeMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var latticeOptions = new LatticeOptions { MaintainProjectionDigest = true };
        latticeMonitor.CurrentValue.Returns(latticeOptions);
        latticeMonitor.Get(Arg.Any<string>()).Returns(latticeOptions);

        var topology = new FakeReplicationTopology(new[] { Peer });
        var transport = Substitute.For<IReplicationDigestProbeTransport>();
        var replicationTransport = Substitute.For<IReplicationTransport>();
        replicationTransport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ReplicationAck { Accepted = sendAccepted }));
        var batchEncoder = Substitute.For<IReplicationBatchEncoder>();
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(shardCount));

        var lattice = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(lattice);
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("phys", ShardMap.CreateDefault(1, 1))));

        // Every shard's local digest mismatches the peer, so each shard localises
        // a divergent range and enters the gated repair stage.
        lattice.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));
        transport.ProbeMerkleWalkAsync(Peer, Arg.Any<MerkleWalkProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));

        if (highWaterMarkFault is null)
        {
            transport.GetPeerHighWaterMarkAsync(
                    Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(peerCursor ?? Hlc(50)));
        }
        else
        {
            transport.GetPeerHighWaterMarkAsync(
                    Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(Task.FromException<HybridLogicalClock>(highWaterMarkFault));
        }

        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var shardRoot = Substitute.For<IShardRootGrain>();
        shardRoot.GetRootNodeRefAsync()
            .Returns(Task.FromResult<ShardRootNodeRef?>(new ShardRootNodeRef { NodeId = leafId, IsLeaf = true }));
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetProjectionDigestAsync().Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);

        // The WAL retains a single orphan at ticks 100; whether re-replay selects
        // it depends on the peer cursor the test supplies.
        var walPage = new WalShardPage
        {
            Entries = new[] { new WalShardSequencedEntry { Sequence = 0, Entry = OrphanWal("fruit:apple", ticks: 100) } },
            NextSequence = 1,
        };
        var walGrain = Substitute.For<IWalShardGrain>();
        walGrain.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(walPage));
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(walGrain);

        var state = new FakePersistentState<ReplicationDigestProbeState>();
        var provider = snapshotProvider
            ?? new OrphanSnapshotProvider(new[] { Committed("fruit:apple", ticks: 100) });

        var grain = new ReplicationDigestProbeGrain(
            context, reminders, NullLogger<ReplicationDigestProbeGrain>.Instance,
            replicationMonitor, latticeMonitor, topology, transport,
            replicationTransport, batchEncoder, shardCounts, grainFactory, provider,
            Substitute.For<ILatticeMergeModeResolver>(), state);

        return (grain, replicationTransport);
    }

    private static string? ReasonTag(RecordedMeasurement<long> measurement)
    {
        foreach (var tag in measurement.Tags)
        {
            if (tag.Key == LatticeReplicationMetrics.TagReason)
            {
                return tag.Value as string;
            }
        }
        return null;
    }

    private static string? ReasonOf(MeterCollector<long> collector) =>
        ReasonTag(collector.Measurements.Single());

    [Test]
    public async Task Remediation_is_skipped_with_opt_out_when_auto_remediate_is_off()
    {
        // The master opt-in flag is off, so detection still fires but the repair
        // is suppressed before any circuit/budget accounting.
        var (grain, replicationTransport) = CreateGatingGrain(autoRemediate: false);
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestRemediationSkippedName);

        await grain.ProcessNextPhaseAsync();

        Assert.That(ReasonOf(skipped), Is.EqualTo(LatticeReplicationMetrics.DigestRemediationReasonOptOut));
        await replicationTransport.DidNotReceive().SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Second_shard_is_skipped_with_circuit_open_after_the_first_shards_reship_is_rejected()
    {
        // Threshold 1 + a rejecting sink: the first shard's re-ship is refused
        // (shipped zero despite selecting a candidate), which is a failure that
        // opens the breaker; the second shard then sees the open circuit and
        // skips. Only the first shard reached the transport.
        var (grain, replicationTransport) = CreateGatingGrain(
            shardCount: 2, sendAccepted: false, failureThreshold: 1, peerCursor: Hlc(50));
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestRemediationSkippedName);

        await grain.ProcessNextPhaseAsync();

        Assert.That(
            skipped.Measurements.Any(m => ReasonTag(m) == LatticeReplicationMetrics.DigestRemediationReasonCircuitOpen),
            Is.True);
        await replicationTransport.Received(1).SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Second_shard_is_skipped_with_budget_exhausted_after_the_first_shard_spends_the_window()
    {
        // A one-entry per-window budget: the first shard ships its single orphan
        // and spends the budget, so the second shard's remediation is skipped
        // with budget-exhausted before any re-ship.
        var (grain, replicationTransport) = CreateGatingGrain(
            shardCount: 2, sendAccepted: true, shipBatchSize: 1, trafficBudgetFraction: 0.5, peerCursor: Hlc(50));
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestRemediationSkippedName);

        await grain.ProcessNextPhaseAsync();

        Assert.That(
            skipped.Measurements.Any(m => ReasonTag(m) == LatticeReplicationMetrics.DigestRemediationReasonBudgetExhausted),
            Is.True);
        await replicationTransport.Received(1).SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Leaf_re_replay_records_disabled_skip_when_the_stage_is_off()
    {
        // With re-replay disabled (and the fallback off so it cannot mask the
        // signal), a localised divergence records a single disabled skip and
        // ships nothing.
        var (grain, replicationTransport) = CreateGatingGrain(
            leafReReplayEnabled: false, bootstrapFallbackEnabled: false);
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplaySkippedName);

        await grain.ProcessNextPhaseAsync();

        Assert.That(ReasonOf(skipped), Is.EqualTo(LatticeReplicationMetrics.LeafReReplaySkipDisabled));
        await replicationTransport.DidNotReceive().SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Leaf_re_replay_swallows_a_fault_and_ships_nothing()
    {
        // The peer high-water-mark read faults inside re-replay; the best-effort
        // helper logs and returns not-attempted, so no repair reaches the
        // transport and the pass completes.
        var (grain, replicationTransport) = CreateGatingGrain(
            highWaterMarkFault: new TimeoutException("high-water-mark unavailable"), peerCursor: Hlc(50));

        await grain.ProcessNextPhaseAsync();

        await replicationTransport.DidNotReceive().SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Bootstrap_fallback_records_disabled_skip_when_the_stage_is_off()
    {
        // The orphan sits at or below the peer cursor, so re-replay yields
        // RangeEmpty and escalates to the fallback - which is disabled, so it
        // records a single disabled skip.
        var (grain, _) = CreateGatingGrain(bootstrapFallbackEnabled: false, peerCursor: Hlc(500));
        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackSkippedName);

        await grain.ProcessNextPhaseAsync();

        Assert.That(ReasonOf(skipped), Is.EqualTo(LatticeReplicationMetrics.BootstrapFallbackSkipDisabled));
    }

    [Test]
    public async Task Bootstrap_fallback_swallows_a_snapshot_export_fault_after_recording_triggered()
    {
        // The below-cursor orphan escalates to the enabled fallback, whose scoped
        // snapshot export faults. The planner records the triggered signal before
        // the export, so triggered fires while no entries ship, and the fault is
        // swallowed.
        var (grain, _) = CreateGatingGrain(
            bootstrapFallbackEnabled: true,
            peerCursor: Hlc(500),
            snapshotProvider: new ThrowingScopedSnapshotProvider(new InvalidOperationException("snapshot export failed")));
        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var entries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(triggered.Measurements, Is.Not.Empty);
            Assert.That(entries.Measurements, Is.Empty);
        });
    }

    /// <summary>
    /// A snapshot provider whose range-scoped export always faults, so the
    /// bootstrap-fallback planner throws after it has recorded the triggered
    /// signal - exercising the fallback helper's swallow-and-log catch arm.
    /// </summary>
    private sealed class ThrowingScopedSnapshotProvider(Exception fault) : ISnapshotProvider
    {
        public Task<SnapshotStream> ExportAsync(
            string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
            => Task.FromException<SnapshotStream>(fault);

        public Task<SnapshotStream> ExportAsync(
            string treeName,
            IReadOnlyList<LeafReReplayRange> ranges,
            HybridLogicalClock asOfHlc,
            CancellationToken cancellationToken = default)
            => Task.FromException<SnapshotStream>(fault);
    }
}
