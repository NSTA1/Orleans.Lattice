using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice;
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

    /// <summary>
    /// Default coalescing window for per-part summary rebuilds. Stream
    /// handlers only mark a serial dirty; the background loop rebuilds each
    /// dirty part at most once per window, collapsing a burst of facts (or
    /// replication re-delivery) for the same part into a single fact-tree
    /// scan. The issue calls out ~1/sec as ample for the live dashboard.
    /// Tests inject a shorter window via the internal ctor for fast,
    /// deterministic assertions.
    /// </summary>
    private static readonly TimeSpan DefaultPartRebuildInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Hard cap on any single best-effort teardown step (waiting out the
    /// rebuild loop, unsubscribing a cluster stream) during
    /// <see cref="StopAsync"/> / <see cref="DisposeAsync"/>. A stream
    /// <c>UnsubscribeAsync</c> talks to the Orleans streaming pub-sub runtime,
    /// which may already be tearing down when the host stops; without a bound
    /// that call can hang indefinitely and wedge host shutdown, leaking the
    /// silo. Capping each step lets shutdown always make progress.
    /// </summary>
    private static readonly TimeSpan ShutdownStepTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Default cadence for the background view-vs-tree reconciliation pass.
    /// While a dashboard subscriber is attached, the rebuild loop periodically
    /// diffs the fact tree (truth) against the materialised
    /// <see cref="PartSummaryView"/> and queues any parts the view is missing
    /// for a rebuild. This is what makes writes that bypass
    /// <see cref="FederationRouter"/> (e.g. a direct <c>SetMany</c> seed, which
    /// raises no <c>FactRouted</c> event) eventually appear on the dashboard
    /// (issue #1048). Kept slower than the rebuild interval so steady-state
    /// reconciliation costs at most one key-scan per cadence, and only when a
    /// dashboard is actually being watched. Tests inject a custom value.
    /// </summary>
    private static readonly TimeSpan DefaultReconcileInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Default maximum number of missing parts queued per reconciliation pass.
    /// Bounds the catch-up rate so a large backfill (e.g. a 10,000-part seed)
    /// converges over several cadences instead of folding every discovered part
    /// in a single burst - which would reintroduce a scan spike. Tests inject a
    /// custom value.
    /// </summary>
    private const int DefaultReconcileBudget = 512;

    /// <summary>
    /// Upper bound on the exponential back-off the rebuild loop applies while
    /// summary upserts are failing (e.g. the WAL storage is saturated and
    /// phase-2 commits are timing out). Without this back-pressure the loop
    /// retries the whole dirty backlog every <see cref="DefaultPartRebuildInterval"/>
    /// and the reconcile pass keeps re-queuing the same parts, so a failing
    /// hot partition never drains - the retries pile more load on it, pin the
    /// silo CPU at 100%, and starve every other grain on the silo (including
    /// the cross-cluster replication shipper). Backing off lets the storage
    /// tier catch up, then the loop resumes its normal cadence once upserts
    /// succeed again.
    /// </summary>
    private static readonly TimeSpan MaxRebuildBackoff = TimeSpan.FromSeconds(30);

    private readonly FederationRouter _router;
    private readonly IClusterClient _client;
    private readonly IGrainFactory _grainFactory;
    private readonly PartCrdtStore _crdtStore;
    private readonly PartSummaryView _summaryView;
    private readonly ILogger<DashboardBroadcaster> _logger;
    private readonly TimeSpan _partRebuildInterval;
    private readonly TimeSpan _snapshotCacheTtl;
    private readonly TimeSpan _reconcileInterval;
    private readonly int _reconcileBudget;
    private DateTime _nextReconcileUtc = DateTime.MinValue;

    // Number of consecutive rebuild cycles whose drain saw at least one
    // failed summary upsert. Drives the rebuild loop's exponential back-off
    // (see MaxRebuildBackoff) so a saturated / failing storage tier sheds load
    // instead of congestion-collapsing under a full-rate retry storm. Reset to
    // zero as soon as a drain completes cleanly or the dirty set empties.
    private int _consecutiveFailedDrains;
    private readonly StreamId _streamId;
    private readonly StreamId _partChangeStreamId;
    private readonly CancellationTokenSource _shutdownCts = new();

    // Serials whose summary needs rebuilding, coalesced across the rebuild
    // window. The byte value is unused - this is a concurrent set keyed by
    // serial so repeated marks for the same part collapse to one rebuild.
    private readonly ConcurrentDictionary<PartSerialNumber, byte> _dirtyParts = new();
    private Task? _rebuildLoop;
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
        PartSummaryView summaryView,
        ILogger<DashboardBroadcaster> logger)
        : this(router, client, crdtStore, summaryView, logger, DefaultBroadcastStreamId)
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
        PartSummaryView summaryView,
        ILogger<DashboardBroadcaster> logger,
        StreamId streamId,
        TimeSpan? partRebuildInterval = null,
        TimeSpan? snapshotCacheTtl = null,
        TimeSpan? reconcileInterval = null,
        int? reconcileBudget = null)
    {
        _router = router;
        _client = client;
        _grainFactory = client;
        _crdtStore = crdtStore;
        _summaryView = summaryView;
        _logger = logger;
        _partRebuildInterval = partRebuildInterval ?? DefaultPartRebuildInterval;
        _snapshotCacheTtl = snapshotCacheTtl ?? DefaultSnapshotCacheTtl;
        _reconcileInterval = reconcileInterval ?? DefaultReconcileInterval;
        _reconcileBudget = reconcileBudget ?? DefaultReconcileBudget;
        _streamId = streamId;
        _partChangeStreamId = DerivePartChangeStreamId(streamId);
    }

    /// <summary>
    /// Test-only ctor overload that auto-provisions a private
    /// <see cref="PartSummaryView"/> over a unique tree id, so existing
    /// fixtures that don't exercise the materialised view can construct a
    /// broadcaster without wiring one. Each broadcaster gets an isolated
    /// summary tree, matching the per-test isolation the lattice / stream
    /// ids already provide.
    /// </summary>
    internal DashboardBroadcaster(
        FederationRouter router,
        IClusterClient client,
        PartCrdtStore crdtStore,
        ILogger<DashboardBroadcaster> logger,
        StreamId streamId,
        TimeSpan? partRebuildInterval = null,
        TimeSpan? snapshotCacheTtl = null,
        TimeSpan? reconcileInterval = null,
        int? reconcileBudget = null)
        : this(
            router,
            client,
            crdtStore,
            new PartSummaryView(client, NullLogger<PartSummaryView>.Instance, $"mfg-part-summary-{Guid.NewGuid():N}"),
            logger,
            streamId,
            partRebuildInterval,
            snapshotCacheTtl,
            reconcileInterval,
            reconcileBudget)
    {
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

        // Start the coalescing rebuild loop last, once the streams are wired.
        _rebuildLoop = Task.Run(() => RunPartRebuildLoopAsync(_shutdownCts.Token));
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _shutdownCts.Cancel();

        await StopRebuildLoopAsync();
        _dirtyParts.Clear();

        _router.FactRouted -= OnFactForBroadcast;
        _router.FactReplicated -= OnFactForBroadcast;
        _router.ChaosConfigChanged -= OnChaosConfigChanged;
        _crdtStore.PartChanged -= OnPartCrdtChanged;

        _broadcastSubscription = await TryUnsubscribeAsync(_broadcastSubscription, "broadcast");
        _partChangeSubscription = await TryUnsubscribeAsync(_partChangeSubscription, "part-change");
    }

    /// <summary>
    /// Waits out the background rebuild loop on shutdown, capped at
    /// <see cref="ShutdownStepTimeout"/> so a loop iteration that is mid-grain
    /// call when the cancel arrives can never wedge teardown. The loop has
    /// already been signalled to stop via <c>_shutdownCts</c>.
    /// </summary>
    private async Task StopRebuildLoopAsync()
    {
        if (_rebuildLoop is null)
        {
            return;
        }

        try
        {
            await _rebuildLoop.WaitAsync(ShutdownStepTimeout);
        }
        catch (Exception ex) when (ex is OperationCanceledException or TimeoutException)
        {
            // Best-effort drain on shutdown; the loop observes the cancel.
        }
        _rebuildLoop = null;
    }

    /// <summary>
    /// Best-effort unsubscribe from a cluster stream, capped at
    /// <see cref="ShutdownStepTimeout"/>. <c>UnsubscribeAsync</c> reaches the
    /// Orleans streaming pub-sub runtime, which may already be tearing down on
    /// host shutdown; an unbounded await there hangs <see cref="StopAsync"/>
    /// and leaks the silo (the contract-test host-teardown deadlock). Always
    /// returns <see langword="null"/> so the caller can clear its handle.
    /// </summary>
    private async Task<StreamSubscriptionHandle<T>?> TryUnsubscribeAsync<T>(
        StreamSubscriptionHandle<T>? subscription,
        string streamLabel)
    {
        if (subscription is null)
        {
            return null;
        }

        try
        {
            await subscription.UnsubscribeAsync().WaitAsync(ShutdownStepTimeout);
        }
        catch (Exception ex)
        {
            // Best-effort log: during late host teardown the logging
            // providers (e.g. the Windows EventLog provider) may already be
            // disposed, so writing the warning can itself throw (wrapped in an
            // AggregateException by the logger). Swallow any logging failure so
            // a best-effort unsubscribe never fails host shutdown.
            try
            {
                _logger.LogWarning(
                    ex,
                    "Failed to unsubscribe from dashboard {Stream} stream within {Timeout}",
                    streamLabel,
                    ShutdownStepTimeout);
            }
            catch
            {
            }
        }
        return null;
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

        await StopRebuildLoopAsync();

        // Detach stream subscription so a broadcaster disposed outside
        // the IHostedService lifecycle (e.g. `await using` in tests)
        // doesn't keep fanning out into completed channels. Bounded so a
        // pub-sub runtime that is already tearing down can't wedge disposal.
        _broadcastSubscription = await TryUnsubscribeAsync(_broadcastSubscription, "broadcast");
        _partChangeSubscription = await TryUnsubscribeAsync(_partChangeSubscription, "part-change");

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
        _dirtyParts.Clear();
        _shutdownCts.Dispose();
    }
}
