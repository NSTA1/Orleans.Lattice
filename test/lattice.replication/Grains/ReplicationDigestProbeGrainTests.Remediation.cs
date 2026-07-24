using System.Runtime.CompilerServices;
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
/// Regression coverage for the below-cursor anti-entropy blind spot
/// (issue #1330). When a later write advanced the peer's high-water-mark past a
/// gap of older, never-shipped entries, targeted leaf re-replay filters those
/// orphans out (they sit at or below the peer cursor) and reports
/// <see cref="LeafReReplaySkipReason.RangeEmpty"/>. The bootstrap-snapshot
/// fallback - which re-derives the divergent ranges from the live tree with no
/// cursor filter - must fire on that reason as well as on
/// <see cref="LeafReReplaySkipReason.WalTrimmed"/>, otherwise the orphans never
/// heal.
/// </summary>
public partial class ReplicationDigestProbeGrainTests
{
    private const string Origin = "site-a";
    private const string Peer = "site-b";

    [Test]
    public async Task Below_cursor_orphan_divergence_triggers_the_bootstrap_fallback()
    {
        // The WAL still retains the orphan write, but at a timestamp at or below
        // the peer's high-water-mark (a later write advanced the cursor past
        // it), so targeted re-replay filters it out and yields RangeEmpty.
        var walPage = new WalShardPage
        {
            Entries = new[]
            {
                new WalShardSequencedEntry { Sequence = 0, Entry = OrphanWal("fruit:apple", ticks: 100) },
            },
            NextSequence = 1,
        };

        // The live tree still projects the orphan, so the cursor-immune
        // bootstrap fallback can re-derive and re-ship it.
        var (grain, _, replicationTransport) = CreateRemediationGrain(
            walPage: walPage,
            snapshotRows: new[] { Committed("fruit:apple", ticks: 100) },
            peerCursor: Hlc(500));

        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var shipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            // The fallback fired for the below-cursor RangeEmpty and re-shipped
            // the stranded orphan. On the pre-fix code the fallback only fired
            // on WalTrimmed, so both collectors would stay empty.
            Assert.That(triggered.Measurements.Single().Value, Is.EqualTo(1L));
            Assert.That(shipped.Measurements.Single().Value, Is.EqualTo(1L));
        });
        await replicationTransport.Received().SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Re_replay_that_closes_the_gap_does_not_trigger_the_bootstrap_fallback()
    {
        // The orphan sits ABOVE the peer cursor, so targeted re-replay ships it
        // and closes the gap. The heavier bootstrap fallback must stay dormant.
        var walPage = new WalShardPage
        {
            Entries = new[]
            {
                new WalShardSequencedEntry { Sequence = 0, Entry = OrphanWal("fruit:apple", ticks: 100) },
            },
            NextSequence = 1,
        };

        var (grain, _, _) = CreateRemediationGrain(
            walPage: walPage,
            snapshotRows: new[] { Committed("fruit:apple", ticks: 100) },
            peerCursor: Hlc(50));

        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var reReplayed = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplayEntriesName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reReplayed.Measurements.Single().Value, Is.EqualTo(1L));
            Assert.That(triggered.Measurements, Is.Empty);
        });
    }

    // ---- Factory + fakes -------------------------------------------------

    private static (
        ReplicationDigestProbeGrain Grain,
        FakePersistentState<ReplicationDigestProbeState> State,
        IReplicationTransport ReplicationTransport) CreateRemediationGrain(
            WalShardPage walPage,
            IReadOnlyList<SnapshotEntry> snapshotRows,
            HybridLogicalClock peerCursor)
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
            LeafReReplayEnabled = true,
            LeafReReplayMaxEntries = 4096,
            LeafReReplayMaxBytes = 1 << 20,
            BootstrapFallbackEnabled = true,
            BootstrapFallbackMaxEntries = 4096,
            BootstrapFallbackMaxBytes = 1 << 20,
            AutoRemediateOnDigestMismatch = true,
            ReplogPartitions = 1,
            ShipPartitionPageSize = 256,
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
            .Returns(Task.FromResult(new ReplicationAck { Accepted = true }));
        var batchEncoder = Substitute.For<IReplicationBatchEncoder>();
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(1));

        var lattice = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(lattice);
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("phys", ShardMap.CreateDefault(1, 1))));

        // Detection: the shard digest mismatches the peer.
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));
        // Localise: a single-leaf root that diverges at depth 0 covers the whole
        // keyspace, so any orphan key falls inside the localised repair range.
        transport.ProbeMerkleWalkAsync(Peer, Arg.Any<MerkleWalkProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));
        transport.GetPeerHighWaterMarkAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(peerCursor));

        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var shardRoot = Substitute.For<IShardRootGrain>();
        shardRoot.GetRootNodeRefAsync()
            .Returns(Task.FromResult<ShardRootNodeRef?>(new ShardRootNodeRef { NodeId = leafId, IsLeaf = true }));
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetProjectionDigestAsync().Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);

        // WAL seam: the re-replay source reads the retained orphan from the
        // {tree}/{partition} WAL shard grain.
        var walGrain = Substitute.For<IWalShardGrain>();
        walGrain.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(walPage));
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(walGrain);

        var state = new FakePersistentState<ReplicationDigestProbeState>();
        var snapshotProvider = new OrphanSnapshotProvider(snapshotRows);

        var grain = new ReplicationDigestProbeGrain(
            context, reminders, NullLogger<ReplicationDigestProbeGrain>.Instance,
            replicationMonitor, latticeMonitor, topology, transport,
            replicationTransport, batchEncoder, shardCounts, grainFactory, snapshotProvider,
            Substitute.For<ILatticeMergeModeResolver>(), state);

        return (grain, state, replicationTransport);
    }

    private static WalRecord OrphanWal(string key, long ticks)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[8],
            Timestamp = Hlc(ticks),
            OriginClusterId = Origin,
        };

    private static SnapshotEntry Committed(string key, long ticks)
        => new() { Key = key, Value = new byte[8], Timestamp = Hlc(ticks) };

    private static HybridLogicalClock Hlc(long ticks)
        => new() { WallClockTicks = ticks, Counter = 0 };

    /// <summary>
    /// Hand-written cursor-immune snapshot provider mirroring the production
    /// live-tree export: it emits the committed projection regardless of any
    /// peer cursor, so the default range-scoped <see cref="ISnapshotProvider"/>
    /// interface method filters it to the localised repair ranges.
    /// </summary>
    private sealed class OrphanSnapshotProvider(IReadOnlyList<SnapshotEntry> rows) : ISnapshotProvider
    {
        public Task<SnapshotStream> ExportAsync(
            string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
            => Task.FromResult(new SnapshotStream(treeName, asOfHlc, new VersionVector(), Emit(rows, cancellationToken)));

        private static async IAsyncEnumerable<SnapshotEntry> Emit(
            IReadOnlyList<SnapshotEntry> rows,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var e in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return e;
            }
            await Task.CompletedTask;
        }
    }
}
