using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the anti-entropy peer digest-probe scheduler grain.
/// Tests construct the grain directly and drive
/// <c>ProcessNextPhaseAsync</c>, asserting against the
/// <see cref="LatticeReplicationMetrics.DigestProbeCompared"/> and
/// <see cref="LatticeReplicationMetrics.DigestProbeMismatch"/> counters
/// captured by a <see cref="MeterCollector{T}"/>.
/// </summary>
[TestFixture]
public class ReplicationDigestProbeGrainTests
{
    private const string Tree = "probe-tree";

    private static LeafProjectionDigest Digest(byte[] hash, int version = LeafProjectionDigest.CurrentVersion)
        => new() { Hash = hash, EntryCount = hash.Length, CheckpointOffset = 1, Version = version };

    private static (
        ReplicationDigestProbeGrain Grain,
        FakePersistentState<ReplicationDigestProbeState> State,
        ILattice Lattice,
        IReplicationDigestProbeTransport Transport,
        IShardCountProvider ShardCounts) CreateProbeGrain(
            bool enabled = true,
            bool maintainProjectionDigest = true,
            int shardCount = 1,
            IEnumerable<string>? peers = null,
            ReplicationDigestProbeState? seed = null,
            bool merkleWalkEnabled = false)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("digest-probe-grain", Tree));
        var reminders = Substitute.For<IReminderRegistry>();

        var replicationMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var replicationOptions = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DigestProbeEnabled = enabled,
            DigestProbeInterval = TimeSpan.FromMinutes(5),
            DigestProbeJitter = 0.0,
            MerkleWalkEnabled = merkleWalkEnabled,
        };
        replicationMonitor.CurrentValue.Returns(replicationOptions);
        replicationMonitor.Get(Arg.Any<string>()).Returns(replicationOptions);

        var latticeMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var latticeOptions = new LatticeOptions { MaintainProjectionDigest = maintainProjectionDigest };
        latticeMonitor.CurrentValue.Returns(latticeOptions);
        latticeMonitor.Get(Arg.Any<string>()).Returns(latticeOptions);

        var topology = new FakeReplicationTopology(peers ?? new[] { "site-b" });
        var transport = Substitute.For<IReplicationDigestProbeTransport>();
        var replicationTransport = Substitute.For<IReplicationTransport>();
        var batchEncoder = Substitute.For<IReplicationBatchEncoder>();
        var shardCounts = Substitute.For<IShardCountProvider>();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(shardCount));

        var lattice = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Tree).Returns(lattice);

        // Wire the routing snapshot and an empty-shard root so the
        // localise-stage walk (when enabled) resolves a physical tree id
        // and terminates cleanly without emitting localise/abort metrics.
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(new RoutingInfo("phys", ShardMap.CreateDefault(1, 1))));
        var shardRoot = Substitute.For<IShardRootGrain>();
        shardRoot.GetRootNodeRefAsync().Returns(Task.FromResult<ShardRootNodeRef?>(null));
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shardRoot);

        var state = new FakePersistentState<ReplicationDigestProbeState>();
        if (seed is not null)
        {
            state.State = seed;
        }

        var snapshotProvider = Substitute.For<ISnapshotProvider>();

        var grain = new ReplicationDigestProbeGrain(
            context, reminders, NullLogger<ReplicationDigestProbeGrain>.Instance,
            replicationMonitor, latticeMonitor, topology, transport,
            replicationTransport, batchEncoder, shardCounts, grainFactory, snapshotProvider, state);

        return (grain, state, lattice, transport, shardCounts);
    }

    [Test]
    public async Task ProcessNextPhaseAsync_does_nothing_when_disabled_by_default()
    {
        var (grain, state, lattice, transport, _) = CreateProbeGrain(enabled: false);

        await grain.ProcessNextPhaseAsync();

        await lattice.DidNotReceive().GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.LastProbeTicks, Is.EqualTo(0L));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_when_maintain_projection_digest_is_false()
    {
        var (grain, state, lattice, transport, _) =
            CreateProbeGrain(maintainProjectionDigest: false);

        await grain.ProcessNextPhaseAsync();

        await lattice.DidNotReceive().GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
        // Cadence advances so the next interval re-checks (the option can be flipped back).
        Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_advances_cadence_when_no_peers()
    {
        var (grain, state, lattice, transport, _) =
            CreateProbeGrain(peers: Array.Empty<string>());

        await grain.ProcessNextPhaseAsync();

        await lattice.DidNotReceive().GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_permanently_skips_on_latched_registry()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        lattice.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("digest maintenance disabled"));

        // First pass detects the latch and disables further passes.
        await grain.ProcessNextPhaseAsync();
        // Second pass must short-circuit before any further digest read.
        await grain.ProcessNextPhaseAsync();

        await lattice.Received(1).GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_records_match_when_digests_equal()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        var hash = new byte[] { 1, 2, 3 };
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(hash)));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(hash) }));

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);
        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(compared.Measurements, Has.Count.EqualTo(1));
            Assert.That(OutcomeOf(compared), Is.EqualTo(LatticeReplicationMetrics.DigestProbeOutcomeMatch));
            Assert.That(mismatch.Measurements, Is.Empty);
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_reads_local_digest_under_a_system_origin_scope()
    {
        // Regression for the anti-entropy self-block on deny-by-default trees:
        // the probe's own local projection-digest read funnels through the
        // fail-closed data-plane access gate, so without a system-origin scope it
        // resolves to the anonymous subject and a secured tree refuses it - which
        // silently disabled detection and remediation on exactly the estates that
        // need them. The read must run under a system-origin scope, and the scope
        // must be restored afterwards.
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        var hash = new byte[] { 1, 2, 3 };
        bool? systemOriginDuringRead = null;
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                systemOriginDuringRead = LatticeAccessGateContext.IsSystemOrigin;
                return Task.FromResult(Digest(hash));
            });
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(hash) }));

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(systemOriginDuringRead, Is.True, "the local digest read must run under a system-origin scope");
            Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False, "the scope must be restored after the pass");
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_records_mismatch_when_digests_differ_same_version()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 9, 9, 9 }),
            }));

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);
        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(compared.Measurements, Has.Count.EqualTo(1));
            Assert.That(OutcomeOf(compared), Is.EqualTo(LatticeReplicationMetrics.DigestProbeOutcomeMismatch));
            Assert.That(mismatch.Measurements, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_does_not_walk_on_mismatch_when_merkle_walk_disabled()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain(merkleWalkEnabled: false);
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 9, 9, 9 }),
            }));

        await grain.ProcessNextPhaseAsync();

        // The localise stage is dark by default: a mismatch must not resolve
        // routing or issue a key-range probe when MerkleWalkEnabled is off.
        await lattice.DidNotReceive().GetRoutingAsync(Arg.Any<CancellationToken>());
        await transport.DidNotReceive().ProbeMerkleWalkAsync(
            Arg.Any<string>(), Arg.Any<MerkleWalkProbeRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_walks_on_mismatch_when_merkle_walk_enabled()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain(merkleWalkEnabled: true);
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 9, 9, 9 }),
            }));

        await grain.ProcessNextPhaseAsync();

        // With the flag on, a mismatch triggers the read-only localise stage,
        // which resolves the physical tree id before descending the tree.
        await lattice.Received().GetRoutingAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ProcessNextPhaseAsync_records_version_skew_and_no_mismatch()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 }, version: 0)));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 1, 2, 3 }, version: 1),
            }));

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);
        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(compared.Measurements, Has.Count.EqualTo(1));
            Assert.That(OutcomeOf(compared), Is.EqualTo(LatticeReplicationMetrics.DigestProbeOutcomeVersionSkew));
            Assert.That(mismatch.Measurements, Is.Empty);
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_records_remote_unavailable_and_no_mismatch()
    {
        var (grain, _, lattice, transport, _) = CreateProbeGrain();
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = false }));

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);
        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(compared.Measurements, Has.Count.EqualTo(1));
            Assert.That(OutcomeOf(compared), Is.EqualTo(LatticeReplicationMetrics.DigestProbeOutcomeRemoteUnavailable));
            Assert.That(mismatch.Measurements, Is.Empty);
        });
    }

    [Test]
    public void EnsureActiveAsync_throws_when_grain_key_is_empty()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(default(GrainId));
        var grain = new ReplicationDigestProbeGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationDigestProbeGrain>.Instance,
            Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
            Substitute.For<IOptionsMonitor<LatticeOptions>>(),
            new FakeReplicationTopology(),
            Substitute.For<IReplicationDigestProbeTransport>(),
            Substitute.For<IReplicationTransport>(),
            Substitute.For<IReplicationBatchEncoder>(),
            Substitute.For<IShardCountProvider>(),
            Substitute.For<IGrainFactory>(),
            Substitute.For<ISnapshotProvider>(),
            new FakePersistentState<ReplicationDigestProbeState>());

        Assert.That(
            async () => await grain.EnsureActiveAsync(CancellationToken.None),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Constructor_throws_when_probe_transport_is_null()
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("digest-probe-grain", Tree));
        Assert.That(
            () => new ReplicationDigestProbeGrain(
                ctx, Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationDigestProbeGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<IOptionsMonitor<LatticeOptions>>(),
                new FakeReplicationTopology(),
                null!,
                Substitute.For<IReplicationTransport>(),
                Substitute.For<IReplicationBatchEncoder>(),
                Substitute.For<IShardCountProvider>(),
                Substitute.For<IGrainFactory>(),
                Substitute.For<ISnapshotProvider>(),
                new FakePersistentState<ReplicationDigestProbeState>()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    private static string? OutcomeOf(MeterCollector<long> collector)
    {
        var m = collector.Measurements.First();
        foreach (var tag in m.Tags)
        {
            if (tag.Key == LatticeReplicationMetrics.TagOutcome)
            {
                return tag.Value as string;
            }
        }
        return null;
    }
}
