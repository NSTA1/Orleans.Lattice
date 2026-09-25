using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;
using System.Net.Http;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Test-only fixture that brings up N Orleans <see cref="TestCluster"/>
/// instances (one per "site") with the production replication shipper
/// grain + receiver-side applier wiring, routed across sites by a
/// shared in-process loopback <see cref="IReplicationTransport"/>
/// (<see cref="LoopbackReplicationTransport"/>). Replaces the
/// in-process <see cref="ChaosDeliveryPump"/> for chaos tests that
/// need real WAL-cursor / shipper-grain behaviour - in particular
/// chaos tests that assert on per-peer cursor visibility into
/// <see cref="IWalCursorRegistry"/> (production-shipper-fixture prerequisite tracked on GitHub Issues) or that
/// drive WAL GC against an actively-shipping pipeline.
/// </summary>
/// <remarks>
/// <para>
/// Each site is a single-silo <c>TestCluster</c> with
/// <c>AddLattice</c> + <c>AddLatticeReplication</c>. The
/// <see cref="LoopbackReplicationTransport"/> is registered as
/// <c>IReplicationTransport</c> on every silo and resolves the peer
/// site's <c>IClusterClient</c> (and through it,
/// <c>IReplicationApplier</c>) via the shared
/// <see cref="LoopbackTransportRegistry"/> singleton. The registry is
/// process-static, keyed on local cluster id, so a per-test fixture
/// instance lives for the duration of the test and tears down cleanly
/// in <see cref="DisposeAsync"/>.
/// </para>
/// <para>
/// <see cref="LoopbackReplicationTransport.IsolateSite"/> /
/// <see cref="LoopbackReplicationTransport.HealSite"/> drop / restore
/// outbound delivery to a specific peer (one-way - to drop both
/// directions of an edge, call <c>IsolateSite</c> on both sites'
/// transports). Drop manifests as <see cref="ReplicationAck.Accepted"/>
/// = <see langword="false"/> so the shipper's per-peer cursor stays
/// stationary and the local WAL keeps growing - the production
/// behaviour under a real partition.
/// </para>
/// </remarks>
internal sealed class ProductionShipperFixture : IAsyncDisposable
{
    public int SiteCount { get; }
    public string TreeName { get; }
    public IReadOnlyList<string> ClusterIds => _clusterIds;

    private readonly string[] _clusterIds;
    private readonly TestCluster[] _clusters;
    private readonly FaultInjectingReplicationApplier[] _appliers;
    private readonly ReplicationPeerStats[] _peerStats;
    private readonly LoopbackTransportRegistry _registry;
    private readonly TimeSpan _livenessProbeInterval;
    private readonly TimeSpan? _sourceIdentityBackstopInterval;
    private readonly TimeSpan _interSiteDeployDelay;

    /// <summary>
    /// Upper bound on how long <see cref="InitializeAsync"/> waits for every
    /// directed edge to clear its startup backoff. The shipper's backoff is
    /// capped at <see cref="LatticeReplicationOptions.DefaultShipBackoffMax"/>
    /// (30 s, plus up to 20 % jitter), so a healthy edge always clears well
    /// inside this.
    /// </summary>
    internal static readonly TimeSpan EdgeReadinessTimeout = TimeSpan.FromSeconds(90);

    public static string ClusterIdFor(int siteIndex) => $"shipper-site-{siteIndex}";

    private readonly string _instanceTag = Guid.NewGuid().ToString("N").Substring(0, 8);
    private string ClusterIdForInstance(int i) => $"shipper-{_instanceTag}-{i}";

    /// <param name="treeName">Tree id the fixture wires for replication.</param>
    /// <param name="siteCount">Number of sites in the chaos topology. Defaults to 2.</param>
    /// <param name="livenessProbeInterval">
    /// Per-silo <see cref="LatticeReplicationOptions.LivenessProbeInterval"/>.
    /// Defaults to 200 ms so chaos tests that drive partitions for
    /// hundreds of milliseconds can observe the empty-tick liveness
    /// probe firing inside the test window. Set to
    /// <see cref="Timeout.InfiniteTimeSpan"/> to disable.
    /// </param>
    /// <param name="sourceIdentityBackstopInterval">
    /// Per-silo <see cref="LatticeReplicationOptions.ShipSourceIdentityBackstopInterval"/>.
    /// Defaults to <see langword="null"/>, which leaves the production
    /// default (30 s) in place. Identity-swap chaos tests set this to a
    /// value far larger than their convergence window so the ONLY path
    /// that can re-resolve a swapped source identity within the test is
    /// the event-driven alias-change notification - never the slow
    /// backstop poll. That makes convergence a genuine assertion about
    /// the deterministic notify/rebind seam rather than a race the
    /// backstop timer would eventually win regardless.
    /// </param>
    /// <param name="interSiteDeployDelay">
    /// Extra pause inserted before deploying each site after the first.
    /// Defaults to zero. Models a loaded CI runner on which a site takes
    /// seconds to come up, so the fixture's startup-backoff readiness
    /// barrier can be exercised deterministically.
    /// </param>
    public ProductionShipperFixture(
        string treeName,
        int siteCount = 2,
        TimeSpan? livenessProbeInterval = null,
        TimeSpan? sourceIdentityBackstopInterval = null,
        TimeSpan? interSiteDeployDelay = null)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        if (siteCount < 2)
        {
            throw new ArgumentOutOfRangeException(nameof(siteCount), siteCount, "Fixture requires at least two sites.");
        }
        TreeName = treeName;
        SiteCount = siteCount;
        _clusterIds = Enumerable.Range(0, siteCount).Select(i => ClusterIdForInstance(i)).ToArray();
        _clusters = new TestCluster[siteCount];
        _appliers = new FaultInjectingReplicationApplier[siteCount];
        _peerStats = new ReplicationPeerStats[siteCount];
        _registry = new LoopbackTransportRegistry();
        _livenessProbeInterval = livenessProbeInterval ?? TimeSpan.FromMilliseconds(200);
        _sourceIdentityBackstopInterval = sourceIdentityBackstopInterval;
        _interSiteDeployDelay = interSiteDeployDelay ?? TimeSpan.Zero;
    }

    public TestCluster ClusterOf(int siteIndex) => _clusters[siteIndex];
    public IClusterClient ClientOf(int siteIndex) => _clusters[siteIndex].Client;
    public LoopbackReplicationTransport TransportOf(int siteIndex) => _registry.Get(_clusterIds[siteIndex]);
    public FaultInjectingReplicationApplier ApplierOf(int siteIndex) => _appliers[siteIndex];
    public ReplicationPeerStats PeerStatsOf(int siteIndex) => _peerStats[siteIndex];

    /// <summary>
    /// The silo-side <see cref="ReplicationPeerStats"/> the production
    /// shipper grains on <paramref name="siteIndex"/> record outbound
    /// contact into. Distinct from <see cref="PeerStatsOf"/>, which is the
    /// fixture-side applier's inbound recorder.
    /// </summary>
    public ReplicationPeerStats SiloPeerStatsOf(int siteIndex) =>
        ((InProcessSiloHandle)_clusters[siteIndex].Silos.First())
            .SiloHost.Services.GetRequiredService<ReplicationPeerStats>();

    /// <summary>Per-silo liveness-probe interval used at silo configure time.</summary>
    internal TimeSpan LivenessProbeInterval => _livenessProbeInterval;

    /// <summary>
    /// Optional per-silo source-identity backstop interval used at silo
    /// configure time; <see langword="null"/> leaves the production default.
    /// </summary>
    internal TimeSpan? SourceIdentityBackstopInterval => _sourceIdentityBackstopInterval;

    public async Task InitializeAsync()
    {
        // Stand up every cluster sequentially. Each silo reads its
        // per-cluster config from the static FixtureRegistry below
        // (keyed on cluster id); this side-channel is the standard
        // Orleans-test pattern for stateful silo configurators
        // (mirrors what MultiSiteClusterFixture does).
        FixtureRegistry.Register(this);
        for (var i = 0; i < SiteCount; i++)
        {
            if (i > 0 && _interSiteDeployDelay > TimeSpan.Zero)
            {
                await Task.Delay(_interSiteDeployDelay);
            }

            var localClusterId = _clusterIds[i];
            var builder = new TestClusterBuilder(initialSilosCount: 1);
            builder.Options.ClusterId = localClusterId;
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
            var cluster = builder.Build();
            await cluster.DeployAsync();
            _clusters[i] = cluster;

            var registry = cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.RegisterAsync(TreeName, new TreeRegistryEntry
            {
                MaxLeafKeys = 16,
                ShardCount = 1,
            });

            _registry.RegisterCluster(localClusterId, cluster.Client);

            // Construct a fixture-side ReplicationApplier per site,
            // mirroring MultiSiteClusterFixture's pattern. The loopback
            // transport routes inbound batches through this applier so
            // the receiver-side inbound peer-stats recording (the bidirectional `peer.last_contact_seconds` / `peer.consecutive_errors` direction tag the inbound-stats wiring shipped)
            // actually fires.
            _peerStats[i] = new ReplicationPeerStats();
            var optsMonitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
            var perSiteOptions = new LatticeReplicationOptions { ClusterId = localClusterId };
            optsMonitor.CurrentValue.Returns(perSiteOptions);
            optsMonitor.Get(Arg.Any<string>()).Returns(perSiteOptions);
            var inner = new ReplicationApplier(
                cluster.Client,
                optsMonitor,
                crdtShapes: null,
                logger: null,
                peerStats: _peerStats[i],
                replicationContext: new OverridesReplicationContext());
            _appliers[i] = new FaultInjectingReplicationApplier(inner, _peerStats[i], localClusterId);
            _registry.RegisterApplier(localClusterId, _appliers[i]);
            var siloHandle = (InProcessSiloHandle)cluster.Silos.First();
            _registry.RegisterEncoder(localClusterId,
                siloHandle.SiloHost.Services.GetRequiredService<IWalRecordEncoder>());
        }

        await WaitForStartupBackoffToClearAsync();
    }

    /// <summary>
    /// Readiness barrier: returns only once every directed edge's shipper
    /// has completed a successful round trip with no failure since, so a
    /// test starts from a clean backoff state rather than inheriting one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Sites deploy one at a time, and each site's shippers activate at
    /// silo start. Until a peer site is deployed and registered the
    /// loopback transport ack-rejects every liveness probe sent to it, and
    /// each rejection escalates that shipper's exponential backoff
    /// (<c>ShipBackoffInitial * 2^(failures - 1)</c>). Without this barrier
    /// the escalation leaks into the test. A test that then injects its own
    /// faults stacks them on top of the inherited failure count, so on a
    /// runner slow enough to reject six startup probes, three injected
    /// faults push the next retry roughly 25 s out and the test's
    /// convergence window closes first. That was issue #3337: the shipper
    /// had not stopped retrying, it was retrying on a 51 s cumulative
    /// schedule.
    /// </para>
    /// <para>
    /// The signal is the silo's outbound <see cref="ReplicationPeerStats"/>
    /// row, which the shipper resets on the same success paths that reset
    /// its backoff. A row with a recorded contact and zero consecutive
    /// errors therefore means the edge's peer-attributable backoff is
    /// clear. The barrier waits out any inherited backoff; it does not
    /// shorten it, so production backoff behaviour is untouched.
    /// </para>
    /// <para>
    /// With the liveness probe disabled nothing is sent on an idle edge, so
    /// no startup backoff can accumulate and there is nothing to wait for.
    /// </para>
    /// </remarks>
    private async Task WaitForStartupBackoffToClearAsync()
    {
        if (_livenessProbeInterval == Timeout.InfiniteTimeSpan)
        {
            return;
        }

        var deadline = DateTime.UtcNow + EdgeReadinessTimeout;
        while (true)
        {
            var pending = FindEdgeWithStartupBackoff();
            if (pending is null)
            {
                return;
            }
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"ProductionShipperFixture readiness barrier: edge {pending} did not complete a clean " +
                    $"round trip within {EdgeReadinessTimeout.TotalSeconds}s of all sites being registered.");
            }
            await Task.Delay(50);
        }
    }

    /// <summary>
    /// Returns a description of the first directed edge whose shipper has
    /// not yet recorded a clean round trip, or <see langword="null"/> when
    /// every edge is clean.
    /// </summary>
    internal string? FindEdgeWithStartupBackoff()
    {
        for (var i = 0; i < SiteCount; i++)
        {
            var snapshot = SiloPeerStatsOf(i).Snapshot();
            for (var j = 0; j < SiteCount; j++)
            {
                if (i == j) continue;
                var peer = _clusterIds[j];
                var row = snapshot.FirstOrDefault(s =>
                    s.Direction == ReplicationContactDirection.Outbound
                    && s.Tree == TreeName
                    && s.Peer == peer);
                if (row == default || double.IsNaN(row.LastContactSeconds) || row.ConsecutiveErrors != 0)
                {
                    return row == default
                        ? $"{_clusterIds[i]} -> {peer} (no outbound row yet)"
                        : $"{_clusterIds[i]} -> {peer} (consecutive errors = {row.ConsecutiveErrors}, " +
                          $"last contact = {row.LastContactSeconds}s)";
                }
            }
        }
        return null;
    }

    public async ValueTask DisposeAsync()
    {
        for (var i = 0; i < SiteCount; i++)
        {
            if (_clusters[i] is null) continue;
            try
            {
                await _clusters[i].StopAllSilosAsync();
                await _clusters[i].DisposeAsync();
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
        FixtureRegistry.Unregister(this);
        _registry.Dispose();
    }

    internal LoopbackTransportRegistry InnerRegistry => _registry;

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = 0);

            // Pull the per-cluster fixture out of the static registry
            // at configure time. The silo builder doesn't know the
            // cluster id directly; we read it from ClusterOptions via
            // ConfigureServices below to resolve the right fixture.
            siloBuilder.AddLatticeReplication((opts) =>
            {
                // Placeholder - real values are post-configured below
                // after the cluster id is bound.
                opts.ClusterId = "pending";
            });

            siloBuilder.ConfigureServices(services =>
            {
                services.AddSingleton<IPostConfigureOptions<LatticeReplicationOptions>, FixtureLatticeReplicationOptionsPostConfigure>();
                services.AddSingleton<IReplicationTransport>(sp =>
                {
                    var clusterOpts = sp.GetRequiredService<IOptions<ClusterOptions>>();
                    var localClusterId = clusterOpts.Value.ClusterId;
                    var fixture = FixtureRegistry.GetByCluster(localClusterId)
                        ?? throw new InvalidOperationException($"No fixture registered for cluster {localClusterId}.");
                    var transport = new LoopbackReplicationTransport(fixture.InnerRegistry, localClusterId);
                    fixture.InnerRegistry.RegisterTransport(localClusterId, transport);
                    return transport;
                });
            });
        }
    }

    private sealed class FixtureLatticeReplicationOptionsPostConfigure(IOptions<ClusterOptions> clusterOptions)
        : IPostConfigureOptions<LatticeReplicationOptions>
    {
        public void PostConfigure(string? name, LatticeReplicationOptions options)
        {
            var localClusterId = clusterOptions.Value.ClusterId;
            var fixture = FixtureRegistry.GetByCluster(localClusterId);
            if (fixture is null) return;
            options.ClusterId = localClusterId;
            options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                [fixture.TreeName] = LatticeMergeMode.LwwRegister,
            };
            options.ReplicationPeers = fixture._clusterIds
                .Where(id => id != localClusterId)
                .ToArray();
            options.ShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(50);
            options.MaintenanceGcInterval = TimeSpan.FromSeconds(1);
            options.LivenessProbeInterval = fixture._livenessProbeInterval;
            if (fixture._sourceIdentityBackstopInterval is { } backstop)
            {
                options.ShipSourceIdentityBackstopInterval = backstop;
            }
        }
    }

    /// <summary>
    /// Process-static fixture registry. The Orleans test silo's
    /// <see cref="ISiloConfigurator"/> is type-instantiated by the
    /// host (cannot carry per-test state), so per-fixture config
    /// flows through this static side-channel keyed on cluster id.
    /// </summary>
    private static class FixtureRegistry
    {
        private static readonly ConcurrentDictionary<string, ProductionShipperFixture> Map =
            new(StringComparer.Ordinal);

        public static void Register(ProductionShipperFixture fixture)
        {
            foreach (var id in fixture._clusterIds)
            {
                Map[id] = fixture;
            }
        }

        public static void Unregister(ProductionShipperFixture fixture)
        {
            foreach (var id in fixture._clusterIds)
            {
                Map.TryRemove(id, out _);
            }
        }

        public static ProductionShipperFixture? GetByCluster(string clusterId) =>
            Map.TryGetValue(clusterId, out var f) ? f : null;
    }
}

/// <summary>
/// Process-static registry that the
/// <see cref="LoopbackReplicationTransport"/> consults to resolve a
/// peer site's <see cref="IClusterClient"/> at SendAsync time.
/// Each <see cref="ProductionShipperFixture"/> instance owns its own
/// registry; sites register their cluster clients on
/// <see cref="ProductionShipperFixture.InitializeAsync"/> and the
/// registry is torn down via <see cref="Dispose"/> in fixture dispose.
/// </summary>
internal sealed class LoopbackTransportRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, IClusterClient> _clusters = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, LoopbackReplicationTransport> _transports = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IReplicationApplier> _appliers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IWalRecordEncoder> _encoders = new(StringComparer.Ordinal);

    public void RegisterCluster(string clusterId, IClusterClient client) => _clusters[clusterId] = client;
    public void RegisterTransport(string clusterId, LoopbackReplicationTransport transport) => _transports[clusterId] = transport;
    public void RegisterApplier(string clusterId, IReplicationApplier applier) => _appliers[clusterId] = applier;
    public void RegisterEncoder(string clusterId, IWalRecordEncoder encoder) => _encoders[clusterId] = encoder;

    public IClusterClient? GetCluster(string clusterId) =>
        _clusters.TryGetValue(clusterId, out var c) ? c : null;

    public IReplicationApplier? GetApplier(string clusterId) =>
        _appliers.TryGetValue(clusterId, out var a) ? a : null;

    public IWalRecordEncoder? GetEncoder(string clusterId) =>
        _encoders.TryGetValue(clusterId, out var e) ? e : null;

    public LoopbackReplicationTransport Get(string clusterId) =>
        _transports.TryGetValue(clusterId, out var t)
            ? t
            : throw new InvalidOperationException($"No loopback transport registered for cluster {clusterId}.");

    public void Dispose()
    {
        _clusters.Clear();
        _transports.Clear();
        _appliers.Clear();
        _encoders.Clear();
    }
}

/// <summary>
/// In-process <see cref="IReplicationTransport"/> that delivers a
/// shipped <see cref="ReplicationBatch"/> directly to the peer site's
/// <see cref="IReplicationApplier"/> via the shared
/// <see cref="LoopbackTransportRegistry"/>. Decodes the framing-only
/// encoded entries the production shipper sends, calls the peer's
/// <c>ApplyBatchAsync</c>, and returns the peer's ack verbatim.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IsolateSite"/> drops outbound delivery to the named
/// peer; subsequent <see cref="SendAsync"/> calls return
/// <see cref="ReplicationAck.Accepted"/> = <see langword="false"/> so
/// the production shipper's per-peer cursor stays stationary. The
/// local WAL continues growing during the partition; on
/// <see cref="HealSite"/> the cursor resumes from its stationary
/// value and ships the accumulated backlog.
/// </para>
/// <para>
/// Faults injected via <see cref="FaultOutboundOnce"/> throw an
/// <see cref="HttpRequestException"/> from the next outbound
/// SendAsync to the named peer, modelling a transport-layer fault
/// that the shipper's backoff path absorbs.
/// </para>
/// </remarks>
internal sealed class LoopbackReplicationTransport : IReplicationTransport
{
    private readonly LoopbackTransportRegistry _registry;
    private readonly string _localClusterId;
    private readonly ConcurrentDictionary<string, byte> _isolated = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _pendingFaults = new(StringComparer.Ordinal);
    private long _batchesShipped;
    private long _batchesAccepted;

    public LoopbackReplicationTransport(LoopbackTransportRegistry registry, string localClusterId)
    {
        _registry = registry;
        _localClusterId = localClusterId;
    }

    public long BatchesShipped => Interlocked.Read(ref _batchesShipped);
    public long BatchesAccepted => Interlocked.Read(ref _batchesAccepted);

    /// <summary>
    /// Optional inspector invoked once per accepted batch with the
    /// fully-decoded entry list. Chaos tests that want to assert on
    /// the wire-level entry shape (e.g. "no maintenance-tagged
    /// tombstone-reap envelopes must cross the producer-side
    /// ShouldShip filter") wire a callback here. Runs synchronously
    /// inside SendAsync.
    /// </summary>
    public Action<IReadOnlyList<WalRecord>>? OnBatchObserved { get; set; }

    public void IsolateSite(string peerClusterId) => _isolated[peerClusterId] = 0;
    public void HealSite(string peerClusterId) => _isolated.TryRemove(peerClusterId, out _);
    public void FaultOutboundOnce(string peerClusterId) => _pendingFaults[peerClusterId] = 0;

    public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _batchesShipped);

        if (_isolated.ContainsKey(batch.TargetClusterId))
        {
            // Partitioned: ack-reject so the shipper holds its cursor.
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero };
        }

        if (_pendingFaults.TryRemove(batch.TargetClusterId, out _))
        {
            throw new HttpRequestException("simulated loopback transport fault");
        }

        var peerApplier = _registry.GetApplier(batch.TargetClusterId);
        var peerEncoder = _registry.GetEncoder(batch.TargetClusterId);
        if (peerApplier is null || peerEncoder is null)
        {
            return new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero };
        }

        // Decode every entry in the framing-only encoded envelope.
        var encodedEntries = batch.EncodedEnvelope?.EncodedEntries ?? ReadOnlyMemory<ArraySegment<byte>>.Empty;
        var mode = batch.EncodedEnvelope?.Header.Mode ?? LatticeMergeMode.LwwRegister;
        var decoded = new List<WalRecord>(encodedEntries.Length);
        for (var i = 0; i < encodedEntries.Length; i++)
        {
            var seg = encodedEntries.Span[i];
            var record = peerEncoder.Decode(seg.AsSpan(), batch.TreeName, mode);
            decoded.Add(record);
        }

        OnBatchObserved?.Invoke(decoded);
        var result = await peerApplier.ApplyBatchAsync(decoded, cancellationToken).ConfigureAwait(false);
        Interlocked.Increment(ref _batchesAccepted);
        return new ReplicationAck
        {
            Accepted = true,
            HighestAppliedHlc = result.HighWaterMark,
        };
    }
}

/// <summary>
/// <see cref="IReplicationApplier"/> decorator that injects
/// caller-controlled failures on the receiver-side apply path so
/// chaos tests can drive the inbound-error recording path (the receiver-side complement of the outbound success counter)
/// (<see cref="ReplicationPeerStats.RecordInboundError(string, string)"/>).
/// The inner applier is the canonical <see cref="ReplicationApplier"/>
/// constructed by <see cref="ProductionShipperFixture"/>; the decorator
/// throws <see cref="InvalidOperationException"/> when a fault is due,
/// otherwise delegates.
/// </summary>
/// <remarks>
/// <para>
/// Two injection modes are offered, and they differ in whether the
/// resulting failure count depends on shipper batching:
/// </para>
/// <list type="bullet">
///   <item><description><see cref="FailEveryNthCall"/> - a one-in-N
///   rate over observed calls. The number of failures it produces is a
///   function of how many times the shipper calls the applier, which is
///   a function of how it packs the WAL into batches. A test that needs
///   <c>k</c> failures from a rate of <c>N</c> is therefore asserting on
///   batch packing it does not control, and fails if the shipper
///   coalesces. Only safe with <c>N == 1</c> ("fail everything for a
///   window"), which needs no assumption about call
///   counts.</description></item>
///   <item><description><see cref="InjectFaults"/> - a deterministic
///   fault budget drained one per entry-carrying apply call, and
///   therefore <b>independent of batching</b>. A thrown batch is retried
///   by the shipper's backoff path, and each retry is another call, so
///   the budget drains against a single coalesced batch exactly as it
///   does against many small ones. Because an entry cannot appear on the
///   receiver until some entry-carrying batch applied successfully, and
///   no such call can succeed while budget remains, <b>convergence of
///   the shipped keys entails that the budget drained in full</b> - the
///   precondition is carried by the convergence assertion rather than by
///   a wall-clock sleep. This is the mode to reach for.</description></item>
/// </list>
/// <para>
/// Empty liveness-probe batches never consume budget: they carry no
/// entries, so failing one would burn a fault that cannot stamp the
/// inbound-error counter (which is keyed on the entries' origin).
/// </para>
/// </remarks>
internal sealed class FaultInjectingReplicationApplier : IReplicationApplier
{
    private readonly IReplicationApplier _inner;
    private readonly ReplicationPeerStats _peerStats;
    private readonly string _localClusterId;
    private int _callCount;
    private int _entryCarryingCallCount;
    private int _injectedFailures;
    private int _faultBudget;

    public FaultInjectingReplicationApplier(IReplicationApplier inner, ReplicationPeerStats peerStats, string localClusterId)
    {
        _inner = inner;
        _peerStats = peerStats;
        _localClusterId = localClusterId;
    }

    /// <summary>
    /// One-in-N fault rate. <c>0</c> disables; <c>3</c> means every 3rd
    /// call throws (counts the call before deciding). Defaults to <c>0</c>.
    /// Batching-sensitive - see the remarks on the class; prefer
    /// <see cref="InjectFaults"/> unless the rate is <c>1</c>.
    /// </summary>
    public int FailEveryNthCall { get; set; }

    /// <summary>Number of receiver-side throws the decorator has injected.</summary>
    public int InjectedFailures => Volatile.Read(ref _injectedFailures);

    /// <summary>Total inbound apply-batch calls observed (including those that threw).</summary>
    public int TotalCalls => Volatile.Read(ref _callCount);

    /// <summary>
    /// Inbound apply-batch calls that carried at least one entry
    /// (i.e. excluding empty liveness-probe batches), including those
    /// that threw. This is the population the fault budget drains from.
    /// </summary>
    public int EntryCarryingCalls => Volatile.Read(ref _entryCarryingCallCount);

    /// <summary>
    /// Faults still owed by <see cref="InjectFaults"/> and not yet
    /// injected. Zero means the budget drained in full.
    /// </summary>
    public int RemainingFaultBudget => Volatile.Read(ref _faultBudget);

    /// <summary>
    /// Arms a deterministic budget of <paramref name="count"/> failures,
    /// replacing any budget still outstanding. Each subsequent
    /// entry-carrying <see cref="ApplyBatchAsync"/> call consumes one
    /// unit and throws; once drained, calls delegate to the inner
    /// applier again. Independent of how the shipper packs batches.
    /// </summary>
    public void InjectFaults(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        Interlocked.Exchange(ref _faultBudget, count);
    }

    public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        => _inner.ApplyAsync(entry, cancellationToken);

    public async Task<ApplyResult> ApplyBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
    {
        var n = Interlocked.Increment(ref _callCount);
        var carriesEntries = entries is { Count: > 0 };
        if (carriesEntries)
        {
            Interlocked.Increment(ref _entryCarryingCallCount);
        }

        var rate = FailEveryNthCall;
        var fail = (carriesEntries && TryConsumeFaultBudget()) || (rate > 0 && n % rate == 0);
        if (fail)
        {
            Interlocked.Increment(ref _injectedFailures);
            // Stamp the inbound-error counter the production applier
            // would have stamped if its inner per-origin run threw,
            // so chaos tests can assert on the failure-path peer-stats
            // recording. The inner applier is bypassed so it never
            // sees this batch - inbound success on the same origin
            // would otherwise leak through and skew the counter.
            if (carriesEntries
                && !string.IsNullOrEmpty(entries[0].OriginClusterId)
                && !string.Equals(entries[0].OriginClusterId, _localClusterId, StringComparison.Ordinal)
                && !string.IsNullOrEmpty(entries[0].TreeId))
            {
                _peerStats.RecordInboundError(entries[0].TreeId, entries[0].OriginClusterId!);
            }
            throw new InvalidOperationException("Injected fixture-side receiver fault");
        }
        return await _inner.ApplyBatchAsync(entries, cancellationToken).ConfigureAwait(false);
    }

    private bool TryConsumeFaultBudget()
    {
        while (true)
        {
            var remaining = Volatile.Read(ref _faultBudget);
            if (remaining <= 0)
            {
                return false;
            }
            if (Interlocked.CompareExchange(ref _faultBudget, remaining - 1, remaining) == remaining)
            {
                return true;
            }
        }
    }
}
