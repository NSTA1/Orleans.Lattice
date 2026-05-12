using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Runtime;
using Orleans.Streams;

namespace MultiSiteManufacturing.Host.Dashboard;

/// <summary>
/// In-process pub/sub hub that feeds the Blazor dashboard.
/// Subscribes to <see cref="FederationRouter.FactRouted"/>, 
/// <see cref="FederationRouter.FactReplicated"/>, and
/// <see cref="FederationRouter.ChaosConfigChanged"/>, derives
/// <see cref="PartSummaryUpdate"/> / <see cref="ChaosOverview"/> /
/// <see cref="SiteActivityIndexEntry"/> records, and broadcasts them
/// to every active local subscriber via per-subscriber
/// <see cref="Channel{T}"/> instances.
/// </summary>
/// <remarks>
/// Components call <see cref="SubscribePartUpdates"/> /
/// <see cref="SubscribeChaosChanges"/> /
/// <see cref="SubscribeDivergence"/> or
/// <see cref="SubscribeSiteActivity"/> in <c>OnInitializedAsync</c>
/// and iterate until disposal. Back-pressure is handled by unbounded
/// channels - the sample's update volume is modest (one fact per
/// operator action plus seed traffic).
/// <para>
/// <b>Cluster-wide fan-out.</b> <see cref="FederationRouter"/> raises
/// <c>FactRouted</c> / <c>FactReplicated</c> only on the silo that
/// handled the fact, but a Blazor Server circuit is pinned to one
/// silo. Without additional plumbing, a user whose circuit lives on
/// silo B never sees a fact that landed on silo A. To fix this, every
/// silo's broadcaster publishes each incoming fact onto a cluster-wide
/// <b>Orleans memory stream</b>
/// (<see cref="StreamProviderName"/> · <see cref="StreamNamespace"/>)
/// and subscribes to the same stream. Orleans stream pub/sub delivers
/// each message to every subscribed silo, where the local broadcaster
/// fans out to its own per-circuit channels. A fact therefore reaches
/// every Blazor circuit regardless of which silo originated it or
/// which silo the user is connected to.
/// </para>
/// <para>
/// The same problem applies to <see cref="PartCrdtStore.PartChanged"/>:
/// a label add or operator assignment fires the event only on the
/// silo that wrote it, and a cross-cluster OR-Set apply fires it
/// only on whichever local silo the gRPC push transport happened to
/// dispatch into. To keep CRDT-card live updates uniform regardless
/// of where the mutation landed, every silo's broadcaster also
/// publishes / subscribes a cluster-wide <see cref="PartSerialNumber"/>
/// stream
/// (<see cref="StreamProviderName"/> · <see cref="PartChangeStreamNamespace"/>)
/// and re-runs the same per-circuit fan-out
/// (<see cref="PublishPartAsync"/>) on receipt.
/// </para>
/// <para>
/// Implementation is split across partial files for readability:
/// the main file holds class-level state and the
/// <see cref="IHostedService"/> lifecycle; <c>.Streaming.cs</c> holds
/// the cluster-wide Orleans stream subscribe / publish path;
/// <c>.Subscriptions.cs</c> holds the per-circuit subscribe APIs;
/// <c>.Snapshots.cs</c> holds the initial-state queries; and
/// <c>.FanOut.cs</c> holds the per-fact fan-out into subscriber
/// channels.
/// </para>
/// </remarks>
public sealed partial class DashboardBroadcaster : IHostedService
{
    /// <summary>
    /// Orleans stream provider name configured by <c>Program.cs</c>
    /// and the test cluster via <c>AddMemoryStreams</c>. Kept as a
    /// public constant so host / test wire-up and the broadcaster
    /// can't drift apart.
    /// </summary>
    public const string StreamProviderName = "DashboardStreams";

    /// <summary>
    /// Stream namespace used for the cluster-wide per-fact broadcast.
    /// Every <see cref="Fact"/> routed or replicated on any silo is
    /// published to a single stream inside this namespace; every silo
    /// subscribes so every Blazor circuit sees every fact.
    /// </summary>
    public const string StreamNamespace = "msmfg.dashboard.facts";

    /// <summary>
    /// Stream namespace used for the cluster-wide per-part-change
    /// broadcast. Every <see cref="PartCrdtStore.PartChanged"/> event
    /// - local CRDT mutation, shadow heal, or cross-cluster OR-Set
    /// apply landing on any silo - is published to a single stream
    /// inside this namespace; every silo subscribes so the part-detail
    /// CRDT card refreshes on every Blazor circuit, not just the
    /// circuit attached to the silo that handled the mutation.
    /// </summary>
    public const string PartChangeStreamNamespace = "msmfg.dashboard.part-changes";

    /// <summary>
    /// Default singleton stream id - one logical stream per cluster.
    /// A fixed key lets every silo subscribe to and publish on the
    /// exact same stream instance without coordination. Tests may
    /// pass a custom <see cref="StreamId"/> to the test-only ctor
    /// overload to isolate per-test traffic.
    /// </summary>
    public static readonly StreamId DefaultBroadcastStreamId =
        StreamId.Create(StreamNamespace, "broadcast");

    /// <summary>
    /// Default singleton stream id for the part-change stream - one
    /// logical stream per cluster, parallel to
    /// <see cref="DefaultBroadcastStreamId"/>. Tests get an
    /// auto-derived sibling id from the test-only ctor's
    /// <see cref="StreamId"/> argument so per-test isolation extends
    /// to both streams without requiring callers to pass two ids.
    /// </summary>
    public static readonly StreamId DefaultPartChangeStreamId =
        StreamId.Create(PartChangeStreamNamespace, "broadcast");

    private readonly FederationRouter _router;
    private readonly IClusterClient _client;
    private readonly IGrainFactory _grainFactory;
    private readonly PartCrdtStore _crdtStore;
    private readonly ILogger<DashboardBroadcaster> _logger;
    private readonly StreamId _streamId;
    private readonly StreamId _partChangeStreamId;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly ConcurrentDictionary<Guid, Channel<PartSummaryUpdate>> _partSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<ChaosOverview>> _chaosSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<DivergenceEvent>> _divSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<SiteActivityIndexEntry>> _activitySubs = new();

    // Remembers the last-published (baseline, lattice) state per part so
    // PublishPartAsync can decide whether a fresh summary should also
    // raise a DivergenceEvent. Concurrent access is fine - the fan-out
    // is serialised per fact inside PublishPartAsync.
    private readonly ConcurrentDictionary<PartSerialNumber, (ComplianceState Baseline, ComplianceState Lattice)> _lastStates = new();

    private IAsyncStream<Fact>? _broadcastStream;
    private StreamSubscriptionHandle<Fact>? _broadcastSubscription;

    private IAsyncStream<PartSerialNumber>? _partChangeStream;
    private StreamSubscriptionHandle<PartSerialNumber>? _partChangeSubscription;

    /// <summary>Creates the broadcaster (DI ctor).</summary>
    /// <remarks>
    /// Takes <see cref="IClusterClient"/> rather than a bare
    /// <see cref="IGrainFactory"/> because the broadcaster needs
    /// <see cref="ClusterClientStreamExtensions.GetStreamProvider"/>
    /// to reach the cluster-wide memory stream.
    /// <see cref="IClusterClient"/> also implements
    /// <see cref="IGrainFactory"/>, so every pre-existing grain call
    /// (e.g. <see cref="IPartitionChaosGrain"/>) flows through the
    /// same reference.
    /// <para>
    /// Also takes <see cref="PartCrdtStore"/> so the broadcaster can
    /// subscribe to <see cref="PartCrdtStore.PartChanged"/> and fan
    /// out a fresh <see cref="PartSummaryUpdate"/> whenever the
    /// per-part CRDT state changes (operator assignment, label add,
    /// or shadow heal). Without this, the part-detail page's CRDT
    /// card would be stale on every circuit other than the one that
    /// issued the mutation, until the user reloaded.
    /// </para>
    /// </remarks>
    public DashboardBroadcaster(
        FederationRouter router,
        IClusterClient client,
        PartCrdtStore crdtStore,
        ILogger<DashboardBroadcaster> logger)
        : this(router, client, crdtStore, logger, DefaultBroadcastStreamId)
    {
    }

    /// <summary>
    /// Test-only ctor overload accepting a custom broadcast
    /// <see cref="StreamId"/>. Used by the test fixtures to scope
    /// stream traffic to a single test and avoid cross-test event
    /// leakage on the shared TestCluster.
    /// </summary>
    internal DashboardBroadcaster(
        FederationRouter router,
        IClusterClient client,
        PartCrdtStore crdtStore,
        ILogger<DashboardBroadcaster> logger,
        StreamId streamId)
    {
        _router = router;
        _client = client;
        _grainFactory = client;
        _crdtStore = crdtStore;
        _logger = logger;
        _streamId = streamId;
        _partChangeStreamId = DerivePartChangeStreamId(streamId);
    }

    /// <summary>
    /// Derives the part-change stream id from the broadcast stream id
    /// so a single test-only constructor argument scopes traffic on
    /// both streams. Production wiring uses the default broadcast id,
    /// which maps to the default part-change id; per-test ids share
    /// the same key under the part-change namespace, giving identical
    /// per-test isolation without forcing every test to construct two
    /// stream ids.
    /// </summary>
    private static StreamId DerivePartChangeStreamId(StreamId factStreamId)
    {
        if (factStreamId.Equals(DefaultBroadcastStreamId))
        {
            return DefaultPartChangeStreamId;
        }
        var key = factStreamId.GetKeyAsString();
        return StreamId.Create(
            PartChangeStreamNamespace,
            string.IsNullOrEmpty(key) ? Guid.NewGuid().ToString("N") : key);
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Every fact - local or replicated - publishes to the cluster
        // stream (see remarks on the class). The local-fan-out work
        // happens only in OnBroadcastReceived so it runs uniformly
        // across silos.
        _router.FactRouted += OnFactForBroadcast;
        _router.FactReplicated += OnFactForBroadcast;
        _router.ChaosConfigChanged += OnChaosConfigChanged;
        _crdtStore.PartChanged += OnPartCrdtChanged;

        var provider = _client.GetStreamProvider(StreamProviderName);
        _broadcastStream = provider.GetStream<Fact>(_streamId);
        _partChangeStream = provider.GetStream<PartSerialNumber>(_partChangeStreamId);

        // Subscribe with bounded retry. A failure here means no live
        // dashboard updates on this silo - we surface it loudly via
        // Error but never throw back into the host's StartAsync
        // because the app is still functional without the live feed
        // (a page reload falls back to the initial snapshot path).
        await SubscribeWithRetryAsync(cancellationToken);
        await SubscribePartChangeWithRetryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _shutdownCts.Cancel();

        _router.FactRouted -= OnFactForBroadcast;
        _router.FactReplicated -= OnFactForBroadcast;
        _router.ChaosConfigChanged -= OnChaosConfigChanged;
        _crdtStore.PartChanged -= OnPartCrdtChanged;

        if (_broadcastSubscription is not null)
        {
            try
            {
                await _broadcastSubscription.UnsubscribeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to unsubscribe from dashboard broadcast stream");
            }
            _broadcastSubscription = null;
        }

        if (_partChangeSubscription is not null)
        {
            try
            {
                await _partChangeSubscription.UnsubscribeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to unsubscribe from dashboard part-change stream");
            }
            _partChangeSubscription = null;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Cancel any in-flight publish-retry or resubscribe loops so
        // they don't observe a half-torn-down broadcaster.
        if (!_shutdownCts.IsCancellationRequested)
        {
            _shutdownCts.Cancel();
        }

        // Detach stream subscription so a broadcaster disposed outside
        // the IHostedService lifecycle (e.g. `await using` in tests)
        // doesn't keep fanning out into completed channels.
        if (_broadcastSubscription is not null)
        {
            try
            {
                await _broadcastSubscription.UnsubscribeAsync();
            }
            catch
            {
                // Best-effort: cluster may already be shutting down.
            }
            _broadcastSubscription = null;
        }

        if (_partChangeSubscription is not null)
        {
            try
            {
                await _partChangeSubscription.UnsubscribeAsync();
            }
            catch
            {
                // Best-effort: cluster may already be shutting down.
            }
            _partChangeSubscription = null;
        }

        _router.FactRouted -= OnFactForBroadcast;
        _router.FactReplicated -= OnFactForBroadcast;
        _router.ChaosConfigChanged -= OnChaosConfigChanged;
        _crdtStore.PartChanged -= OnPartCrdtChanged;

        foreach (var sub in _partSubs.Values)
        {
            sub.Writer.TryComplete();
        }
        foreach (var sub in _chaosSubs.Values)
        {
            sub.Writer.TryComplete();
        }
        foreach (var sub in _divSubs.Values)
        {
            sub.Writer.TryComplete();
        }
        foreach (var sub in _activitySubs.Values)
        {
            sub.Writer.TryComplete();
        }
        _partSubs.Clear();
        _chaosSubs.Clear();
        _divSubs.Clear();
        _activitySubs.Clear();
        _lastStates.Clear();
        _shutdownCts.Dispose();
    }
}
