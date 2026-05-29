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
/// <see cref="IWalCursorRegistry"/> (production-shipper-fixture prerequisite tracked on the replication roadmap) or that
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
    public ProductionShipperFixture(string treeName, int siteCount = 2, TimeSpan? livenessProbeInterval = null)
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
    }

    public TestCluster ClusterOf(int siteIndex) => _clusters[siteIndex];
    public IClusterClient ClientOf(int siteIndex) => _clusters[siteIndex].Client;
    public LoopbackReplicationTransport TransportOf(int siteIndex) => _registry.Get(_clusterIds[siteIndex]);
    public FaultInjectingReplicationApplier ApplierOf(int siteIndex) => _appliers[siteIndex];
    public ReplicationPeerStats PeerStatsOf(int siteIndex) => _peerStats[siteIndex];

    /// <summary>Per-silo liveness-probe interval used at silo configure time.</summary>
    internal TimeSpan LivenessProbeInterval => _livenessProbeInterval;

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
                new LocalVectorClockCache(cluster.Client),
                crdtShapes: null,
                logger: null,
                peerStats: _peerStats[i]);
            _appliers[i] = new FaultInjectingReplicationApplier(inner, _peerStats[i], localClusterId);
            _registry.RegisterApplier(localClusterId, _appliers[i]);
            var siloHandle = (InProcessSiloHandle)cluster.Silos.First();
            _registry.RegisterEncoder(localClusterId,
                siloHandle.SiloHost.Services.GetRequiredService<IWalRecordEncoder>());
        }
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
/// <see cref="IReplicationApplier"/> decorator that injects a
/// caller-controlled failure rate on the receiver-side apply path so
/// chaos tests can drive the inbound-error recording path (the receiver-side complement of the outbound success counter)
/// (<see cref="ReplicationPeerStats.RecordInboundError(string, string)"/>).
/// The inner applier is the canonical <see cref="ReplicationApplier"/>
/// constructed by <see cref="ProductionShipperFixture"/>; the decorator
/// throws <see cref="InvalidOperationException"/> on every Nth call
/// when the fault rate is non-zero, otherwise delegates.
/// </summary>
internal sealed class FaultInjectingReplicationApplier : IReplicationApplier
{
    private readonly IReplicationApplier _inner;
    private readonly ReplicationPeerStats _peerStats;
    private readonly string _localClusterId;
    private int _callCount;
    private int _injectedFailures;

    public FaultInjectingReplicationApplier(IReplicationApplier inner, ReplicationPeerStats peerStats, string localClusterId)
    {
        _inner = inner;
        _peerStats = peerStats;
        _localClusterId = localClusterId;
    }

    /// <summary>
    /// One-in-N fault rate. <c>0</c> disables; <c>3</c> means every 3rd
    /// call throws (counts the call before deciding). Defaults to <c>0</c>.
    /// </summary>
    public int FailEveryNthCall { get; set; }

    /// <summary>Number of receiver-side throws the decorator has injected.</summary>
    public int InjectedFailures => Volatile.Read(ref _injectedFailures);

    /// <summary>Total inbound apply-batch calls observed (including those that threw).</summary>
    public int TotalCalls => Volatile.Read(ref _callCount);

    public Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
        => _inner.ApplyAsync(entry, cancellationToken);

    public async Task<ApplyResult> ApplyBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
    {
        var n = Interlocked.Increment(ref _callCount);
        var rate = FailEveryNthCall;
        if (rate > 0 && n % rate == 0)
        {
            Interlocked.Increment(ref _injectedFailures);
            // Stamp the inbound-error counter the production applier
            // would have stamped if its inner per-origin run threw,
            // so chaos tests can assert on the failure-path peer-stats
            // recording. The inner applier is bypassed so it never
            // sees this batch - inbound success on the same origin
            // would otherwise leak through and skew the counter.
            if (entries is { Count: > 0 }
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
}
