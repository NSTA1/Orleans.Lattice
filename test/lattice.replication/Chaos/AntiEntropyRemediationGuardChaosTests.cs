using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Deterministic chaos coverage of the anti-entropy chain's behavioural
/// guarantees, driven against the production localisation and repair engines
/// and the real digest-probe grain. Complements the multi-site cluster drift
/// tests by pinning the metric-level invariants the issue requires: detection
/// fires inside one probe cadence, the Merkle walk localises within the
/// fan-out depth bound, the repair engines close the localised gap bounded by
/// the configured entry budget, and both the opt-out master gate and the
/// projection-digest-disabled latch short-circuit with zero remediation
/// traffic while detection is still permitted to fire.
/// </summary>
[TestFixture]
[Category("Chaos")]
public class AntiEntropyRemediationGuardChaosTests
{
    private const string Tree = "anti-entropy-guard";
    private const string Peer = "site-b";
    private const string Origin = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0)
        => new() { WallClockTicks = ticks, Counter = counter };

    private static LeafProjectionDigest Digest(byte[] hash, int version = LeafProjectionDigest.CurrentVersion)
        => new() { Hash = hash, EntryCount = hash.Length, CheckpointOffset = 1, Version = version };

    // ---- Assertion #1: detection inside a single probe cadence -----------

    [Test]
    public async Task Skipped_write_drift_is_detected_within_one_probe_cadence()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        // Local shard holds four entries; the diverged peer is missing two of
        // them, so the shard hashes differ (the skipped-write signal).
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3, 4 })));
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 1, 2 }),
            }));

        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        // A single ProcessNextPhaseAsync is one scheduler cadence; detection
        // here is therefore well inside two probe intervals.
        await grain.ProcessNextPhaseAsync();

        Assert.That(mismatch.Measurements, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Corrupted_apply_drift_is_detected_within_one_probe_cadence()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        // Same entry count on both sides but a different value was applied on
        // the peer, so the content hashes still differ.
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 1, 9, 3 }),
            }));

        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        Assert.That(mismatch.Measurements, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Detection_clears_after_the_peer_digest_re_matches_on_heal()
    {
        var (grain, state, lattice, transport, _) = CreateProbeGrain();
        var local = new byte[] { 1, 2, 3 };
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(local)));

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);
        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        // Partitioned: the peer digest differs, so the first cadence mismatches.
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));
        await grain.ProcessNextPhaseAsync();

        // Healed: the peer catches up to the same content. Reset the cadence
        // marker so the next cadence runs, then probe again.
        state.State.LastProbeTicks = 0;
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(local) }));
        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            // Two cadences compared, only the partitioned one mismatched.
            Assert.That(compared.Measurements, Has.Count.EqualTo(2));
            Assert.That(mismatch.Measurements, Has.Count.EqualTo(1));
        });
    }

    // ---- Assertion #2: localisation within the log_k(N) depth bound ------

    [Test]
    public async Task Merkle_walk_localises_the_diverging_leaf_within_the_log_k_depth_bound()
    {
        const int fanOut = 4;
        const int leafCount = 16; // depth bound = ceil(log_4 16) = 2.
        var expectedDepth = (int)Math.Ceiling(Math.Log(leafCount) / Math.Log(fanOut));

        // Build a balanced fan-out-4, depth-2 tree whose leftmost leaf is the
        // only diverging node. The leftmost descent path always carries a null
        // range-start; every other range start is non-null.
        var d1 = Enumerable.Range(0, fanOut).Select(i => GrainId.Create("n", $"d1-{i}")).ToArray();
        var leaves0 = Enumerable.Range(0, fanOut).Select(i => GrainId.Create("n", $"l0-{i}")).ToArray();

        var root = new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = Digest(new byte[] { 0, 0 }),
            Children = new[]
            {
                new MerkleWalkLocalChild { SeparatorKey = null, NodeId = d1[0], ChildIsLeaf = false },
                new MerkleWalkLocalChild { SeparatorKey = "m", NodeId = d1[1], ChildIsLeaf = false },
                new MerkleWalkLocalChild { SeparatorKey = "t", NodeId = d1[2], ChildIsLeaf = false },
                new MerkleWalkLocalChild { SeparatorKey = "w", NodeId = d1[3], ChildIsLeaf = false },
            },
        };

        var tree = new FakeTree(root);
        // Leftmost depth-1 internal node diverges and is descended.
        tree.Add(d1[0], new MerkleWalkLocalNode
        {
            IsLeaf = false,
            Digest = Digest(new byte[] { 1, 1 }),
            Children = new[]
            {
                new MerkleWalkLocalChild { SeparatorKey = null, NodeId = leaves0[0], ChildIsLeaf = true },
                new MerkleWalkLocalChild { SeparatorKey = "d", NodeId = leaves0[1], ChildIsLeaf = true },
                new MerkleWalkLocalChild { SeparatorKey = "g", NodeId = leaves0[2], ChildIsLeaf = true },
                new MerkleWalkLocalChild { SeparatorKey = "j", NodeId = leaves0[3], ChildIsLeaf = true },
            },
        });
        // The other depth-1 internals are resolved (to read their local digest)
        // but match remotely and are pruned without descent.
        for (var i = 1; i < fanOut; i++)
        {
            tree.Add(d1[i], new MerkleWalkLocalNode
            {
                IsLeaf = false,
                Digest = Digest(new byte[] { 7, 7 }),
                Children = Array.Empty<MerkleWalkLocalChild>(),
            });
        }
        // Leftmost leaf diverges; its siblings match.
        tree.Add(leaves0[0], new MerkleWalkLocalNode
        {
            IsLeaf = true,
            Digest = Digest(new byte[] { 2, 2 }),
            Children = Array.Empty<MerkleWalkLocalChild>(),
        });
        for (var i = 1; i < fanOut; i++)
        {
            tree.Add(leaves0[i], new MerkleWalkLocalNode
            {
                IsLeaf = true,
                Digest = Digest(new byte[] { 7, 7 }),
                Children = Array.Empty<MerkleWalkLocalChild>(),
            });
        }

        // Remote responder: the leftmost descent path (null range start)
        // diverges at every depth; all sibling ranges (non-null start) match
        // the matching-sibling local hash and prune.
        var transport = new StubProbeTransport((_, req) =>
            new MerkleWalkProbeResponse
            {
                Available = true,
                Digest = req.RangeStartKey is null
                    ? Digest(new byte[] { 9, 9 })
                    : Digest(new byte[] { 7, 7 }),
            });

        using var localised = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.MerkleWalkLocalisedName);

        var outcome = await MerkleWalkLocaliser.WalkAsync(
            Tree, 0, Peer, tree, transport, maxDepth: 8, maxBytes: 4096, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Localised, Is.True);
            Assert.That(outcome.LeavesLocalised, Is.EqualTo(1));
            Assert.That(outcome.DepthReached, Is.LessThanOrEqualTo(expectedDepth));
            Assert.That(outcome.DepthReached, Is.EqualTo(expectedDepth));
        });
        Assert.That(localised.Measurements.Single().Value, Is.EqualTo(1L));
    }

    // ---- Assertion #3: repair closes the localised gap within budget -----

    [Test]
    public async Task Targeted_leaf_re_replay_closes_the_localised_gap_within_the_entry_budget()
    {
        using var entries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplayEntriesName);

        // Five in-range entries above the peer cursor; a generous budget ships
        // every one, closing the gap fully.
        var read = new WalReReplayReadResult
        {
            Entries = Enumerable.Range(0, 5)
                .Select(i => Entry($"k{i}", ticks: 200 + i))
                .ToArray(),
        };
        var source = new StubReReplaySource(read);
        var sink = new RecordingSink(ackAccepted: true);

        var full = await LeafReReplayer.ReplayAsync(
            Tree, Peer, Origin, new[] { Range(null, null) }, Hlc(100), source, sink,
            maxEntries: 4096, maxBytes: 1024 * 1024, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(full.Attempted, Is.True);
            Assert.That(full.EntriesReReplayed, Is.EqualTo(5));
        });
        Assert.That(entries.Measurements.Single().Value, Is.EqualTo(5L));

        // A tight budget caps the re-ship to a bounded prefix - remediation is
        // never unbounded.
        var capped = await LeafReReplayer.ReplayAsync(
            Tree, Peer, Origin, new[] { Range(null, null) }, Hlc(100),
            new StubReReplaySource(read), new RecordingSink(ackAccepted: true),
            maxEntries: 2, maxBytes: 1024 * 1024, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(capped.EntriesReReplayed, Is.GreaterThan(0));
            Assert.That(capped.EntriesReReplayed, Is.LessThanOrEqualTo(2));
        });
    }

    [Test]
    public async Task Wal_trimmed_divergence_falls_back_to_the_bootstrap_snapshot_within_budget()
    {
        using var triggered = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackTriggeredName);
        using var shipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        var rows = new[]
        {
            Committed("a", ticks: 100),
            Committed("b", ticks: 110),
            Committed("c", ticks: 120),
        };
        var provider = new StubSnapshotProvider(rows);
        var sink = new RecordingSink(ackAccepted: true);

        var outcome = await BootstrapFallbackPlanner.PlanAsync(
            Tree, Peer, Origin, new[] { Range(null, null) }, provider, sink,
            maxEntries: 4096, maxBytes: 1024 * 1024, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.EntriesShipped, Is.EqualTo(3));
        });
        Assert.That(triggered.Measurements.Single().Value, Is.EqualTo(1L));
        Assert.That(shipped.Measurements.Single().Value, Is.EqualTo(3L));

        // The entry budget bounds the snapshot re-ship to a prefix.
        var capped = await BootstrapFallbackPlanner.PlanAsync(
            Tree, Peer, Origin, new[] { Range(null, null) },
            new StubSnapshotProvider(rows), new RecordingSink(ackAccepted: true),
            maxEntries: 2, maxBytes: 1024 * 1024, CancellationToken.None);

        Assert.That(capped.EntriesShipped, Is.LessThanOrEqualTo(2));
    }

    // ---- Assertion #4: zero remediation traffic on the negative paths ----

    [Test]
    public async Task Opt_out_localises_drift_but_emits_zero_remediation_traffic()
    {
        // A single-leaf root that diverges from the peer at depth 0. The
        // Merkle walk localises it, then the master opt-out gate
        // (AutoRemediateOnDigestMismatch=false) suppresses every repair action.
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString("N"));
        var (grain, _, lattice, transport, replicationTransport) = CreateProbeGrain(
            merkleWalkEnabled: true,
            autoRemediate: false,
            leafRoot: leafId,
            localLeafHash: new byte[] { 1, 2, 3 });

        // Detection: the shard digest mismatches.
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync(Peer, Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));
        // Localise: the leaf-root range probe also diverges, same digest version.
        transport.ProbeMerkleWalkAsync(Peer, Arg.Any<MerkleWalkProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new MerkleWalkProbeResponse { Available = true, Digest = Digest(new byte[] { 9, 9, 9 }) }));

        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestRemediationSkippedName);
        using var reReplayEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplayEntriesName);
        using var bootstrapEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            // Detection localised the drift, then the gate short-circuited:
            // a single opt-out skip, no re-replay, no bootstrap, no transport.
            Assert.That(reReplayEntries.Measurements, Is.Empty);
            Assert.That(bootstrapEntries.Measurements, Is.Empty);
            Assert.That(skipped.Measurements, Has.Count.GreaterThanOrEqualTo(1));
            Assert.That(skipped.Measurements.First().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == LatticeReplicationMetrics.TagReason
                && (string?)t.Value == LatticeReplicationMetrics.DigestRemediationReasonOptOut));
        });
        await replicationTransport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Projection_digest_disabled_latch_short_circuits_with_zero_probe_or_repair_traffic()
    {
        // The system-tree default-off / latched path: projection-digest
        // maintenance is off for the tree, so the pass never reads a digest,
        // never probes a peer, and never emits repair traffic.
        var (grain, _, lattice, transport, replicationTransport) =
            CreateProbeGrain(maintainProjectionDigest: false);

        using var skipped = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestRemediationSkippedName);
        using var reReplayEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.LeafReReplayEntriesName);
        using var bootstrapEntries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.BootstrapFallbackEntriesName);

        await grain.ProcessNextPhaseAsync();

        await lattice.DidNotReceive().GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
        await replicationTransport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(reReplayEntries.Measurements, Is.Empty);
            Assert.That(bootstrapEntries.Measurements, Is.Empty);
            Assert.That(skipped.Measurements, Is.Empty);
        });
    }

    // ---- Factory + fakes -------------------------------------------------

    private static (
        ReplicationDigestProbeGrain Grain,
        FakePersistentState<ReplicationDigestProbeState> State,
        ILattice Lattice,
        IReplicationDigestProbeTransport Transport,
        IReplicationTransport ReplicationTransport) CreateProbeGrain(
            bool maintainProjectionDigest = true,
            bool merkleWalkEnabled = false,
            bool autoRemediate = false,
            GrainId? leafRoot = null,
            byte[]? localLeafHash = null)
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
            MerkleWalkEnabled = merkleWalkEnabled,
            MerkleWalkMaxDepth = 8,
            MerkleWalkMaxBytes = 1 << 20,
            LeafReReplayEnabled = true,
            LeafReReplayMaxEntries = 4096,
            LeafReReplayMaxBytes = 1 << 20,
            BootstrapFallbackEnabled = true,
            BootstrapFallbackMaxEntries = 4096,
            BootstrapFallbackMaxBytes = 1 << 20,
            AutoRemediateOnDigestMismatch = autoRemediate,
        };
        replicationMonitor.CurrentValue.Returns(replicationOptions);
        replicationMonitor.Get(Arg.Any<string>()).Returns(replicationOptions);

        var latticeMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var latticeOptions = new LatticeOptions { MaintainProjectionDigest = maintainProjectionDigest };
        latticeMonitor.CurrentValue.Returns(latticeOptions);
        latticeMonitor.Get(Arg.Any<string>()).Returns(latticeOptions);

        var topology = new FakeReplicationTopology(new[] { Peer });
        var transport = Substitute.For<IReplicationDigestProbeTransport>();
        var replicationTransport = Substitute.For<IReplicationTransport>();
        var batchEncoder = Substitute.For<IReplicationBatchEncoder>();
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(1));

        var lattice = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(lattice);
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("phys", ShardMap.CreateDefault(1, 1))));

        var shardRoot = Substitute.For<IShardRootGrain>();
        if (leafRoot is { } leafId)
        {
            shardRoot.GetRootNodeRefAsync()
                .Returns(Task.FromResult<ShardRootNodeRef?>(new ShardRootNodeRef { NodeId = leafId, IsLeaf = true }));
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.GetProjectionDigestAsync()
                .Returns(Task.FromResult(Digest(localLeafHash ?? new byte[] { 1, 2, 3 })));
            grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        }
        else
        {
            shardRoot.GetRootNodeRefAsync().Returns(Task.FromResult<ShardRootNodeRef?>(null));
        }
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);

        var state = new FakePersistentState<ReplicationDigestProbeState>();
        var snapshotProvider = Substitute.For<ISnapshotProvider>();

        var grain = new ReplicationDigestProbeGrain(
            context, reminders, NullLogger<ReplicationDigestProbeGrain>.Instance,
            replicationMonitor, latticeMonitor, topology, transport,
            replicationTransport, batchEncoder, shardCounts, grainFactory, snapshotProvider, state);

        return (grain, state, lattice, transport, replicationTransport);
    }

    private static WalRecord Entry(string key, long ticks)
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

    private static LeafReReplayRange Range(string? start, string? end)
        => new() { StartKey = start, EndKey = end };

    private sealed class FakeTree(MerkleWalkLocalNode? root) : IMerkleWalkLocalTree
    {
        private readonly Dictionary<GrainId, MerkleWalkLocalNode> _nodes = new();

        public void Add(GrainId id, MerkleWalkLocalNode node) => _nodes[id] = node;

        public ValueTask<MerkleWalkLocalNode?> GetRootAsync(CancellationToken cancellationToken) =>
            new(root);

        public ValueTask<MerkleWalkLocalNode> ResolveAsync(GrainId nodeId, bool isLeaf, CancellationToken cancellationToken) =>
            new(_nodes[nodeId]);
    }

    private sealed class StubProbeTransport(Func<string, MerkleWalkProbeRequest, MerkleWalkProbeResponse> responder)
        : IReplicationDigestProbeTransport
    {
        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new DigestProbeResponse { DigestAvailable = false });

        public Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync(
            string targetClusterId, MerkleWalkProbeRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(responder(targetClusterId, request));
    }

    private sealed class StubReReplaySource(WalReReplayReadResult result) : IWalReReplaySource
    {
        public ValueTask<WalReReplayReadResult> ReadAsync(CancellationToken cancellationToken) =>
            new(result);
    }

    private sealed class StubSnapshotProvider(IReadOnlyList<SnapshotEntry> entries) : ISnapshotProvider
    {
        public Task<SnapshotStream> ExportAsync(
            string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
            => Task.FromResult(new SnapshotStream(treeName, asOfHlc, new VersionVector(), Emit(entries, cancellationToken)));

        private static async IAsyncEnumerable<SnapshotEntry> Emit(
            IReadOnlyList<SnapshotEntry> entries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var e in entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return e;
            }
            await Task.CompletedTask;
        }
    }

    private sealed class RecordingSink(bool ackAccepted) : ILeafReReplaySink
    {
        public int Calls { get; private set; }

        public IReadOnlyList<WalRecord> LastEntries { get; private set; } = Array.Empty<WalRecord>();

        public ValueTask<int> ReplayAsync(
            string peer, string treeName, IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
        {
            Calls++;
            LastEntries = entries;
            return new ValueTask<int>(ackAccepted ? entries.Count : 0);
        }
    }
}
