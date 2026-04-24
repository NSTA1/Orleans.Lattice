using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
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
/// channels — the sample's update volume is modest (one fact per
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
/// Locally-emitted facts arrive via <c>FactRouted</c>;
/// peer-replicated facts arrive via <c>FactReplicated</c> (raised
/// by the inbound replication endpoint after baseline replay). Both
/// events share the same handler because the downstream work —
/// publish to the stream, rebuild a <see cref="PartSummaryUpdate"/>
/// / <see cref="SiteActivityIndexEntry"/>, and fan out — is identical
/// regardless of fact origin. This is what lets a peer cluster's
/// dashboard refresh automatically as replicated facts land.
/// </para>
/// </remarks>
public sealed class DashboardBroadcaster : IHostedService
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
    /// Default singleton stream id — one logical stream per cluster.
    /// A fixed key lets every silo subscribe to and publish on the
    /// exact same stream instance without coordination. Tests may
    /// pass a custom <see cref="StreamId"/> to the test-only ctor
    /// overload to isolate per-test traffic.
    /// </summary>
    public static readonly StreamId DefaultBroadcastStreamId =
        StreamId.Create(StreamNamespace, "broadcast");

    private readonly FederationRouter _router;
    private readonly IClusterClient _client;
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<DashboardBroadcaster> _logger;
    private readonly StreamId _streamId;
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly ConcurrentDictionary<Guid, Channel<PartSummaryUpdate>> _partSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<ChaosOverview>> _chaosSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<DivergenceEvent>> _divSubs = new();
    private readonly ConcurrentDictionary<Guid, Channel<SiteActivityIndexEntry>> _activitySubs = new();

    private IAsyncStream<Fact>? _broadcastStream;
    private StreamSubscriptionHandle<Fact>? _broadcastSubscription;

    /// <summary>
    /// Tuneable publish-retry policy. A publish hitting a transient
    /// Azure Storage Queue hiccup (throttling, 500, brief network
    /// blip) should not silently drop a dashboard update — we retry
    /// with exponential backoff before logging and giving up. Values
    /// kept small: a dashboard update is time-sensitive, not worth
    /// holding onto for minutes.
    /// </summary>
    private static readonly TimeSpan[] PublishBackoff =
    {
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(400),
        TimeSpan.FromSeconds(2),
    };

    /// <summary>
    /// Subscribe-retry policy. Queue provisioning, PubSubStore
    /// readiness, and silo-wide startup ordering can all delay the
    /// broadcast stream from being ready when the hosted service
    /// starts; we retry subscribe with bounded backoff rather than
    /// crashing the host.
    /// </summary>
    private static readonly TimeSpan[] SubscribeBackoff =
    {
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(15),
    };

    // Remembers the last-published (baseline, lattice) state per part so
    // PublishPartAsync can decide whether a fresh summary should also
    // raise a DivergenceEvent. Concurrent access is fine — the fan-out
    // is serialised per fact inside PublishPartAsync.
    private readonly ConcurrentDictionary<PartSerialNumber, (ComplianceState Baseline, ComplianceState Lattice)> _lastStates = new();

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
    /// </remarks>
    public DashboardBroadcaster(FederationRouter router, IClusterClient client, ILogger<DashboardBroadcaster> logger)
        : this(router, client, logger, DefaultBroadcastStreamId)
    {
    }

    /// <summary>
    /// Test-only ctor overload accepting a custom broadcast
    /// <see cref="StreamId"/>. Used by the test fixtures to scope
    /// stream traffic to a single test and avoid cross-test event
    /// leakage on the shared TestCluster.
    /// </summary>
    internal DashboardBroadcaster(FederationRouter router, IClusterClient client, ILogger<DashboardBroadcaster> logger, StreamId streamId)
    {
        _router = router;
        _client = client;
        _grainFactory = client;
        _logger = logger;
        _streamId = streamId;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Every fact — local or replicated — publishes to the cluster
        // stream (see remarks on the class). The local-fan-out work
        // happens only in OnBroadcastReceived so it runs uniformly
        // across silos.
        _router.FactRouted += OnFactForBroadcast;
        _router.FactReplicated += OnFactForBroadcast;
        _router.ChaosConfigChanged += OnChaosConfigChanged;

        _broadcastStream = _client
            .GetStreamProvider(StreamProviderName)
            .GetStream<Fact>(_streamId);

        // Subscribe with bounded retry. A failure here means no live
        // dashboard updates on this silo — we surface it loudly via
        // Error but never throw back into the host's StartAsync
        // because the app is still functional without the live feed
        // (a page reload falls back to the initial snapshot path).
        await SubscribeWithRetryAsync(cancellationToken);
    }

    /// <summary>
    /// Subscribes to the broadcast stream with bounded exponential
    /// backoff. Also wires an <c>onError</c> handler so a mid-flight
    /// stream failure triggers a fresh subscribe attempt on a
    /// background task — Azure Storage Queue streams can surface
    /// errors when the agent loses its lease or the queue is
    /// transiently unavailable.
    /// </summary>
    private async Task SubscribeWithRetryAsync(CancellationToken cancellationToken)
    {
        if (_broadcastStream is null)
        {
            return;
        }

        for (var attempt = 0; attempt <= SubscribeBackoff.Length; attempt++)
        {
            if (cancellationToken.IsCancellationRequested || _shutdownCts.IsCancellationRequested)
            {
                return;
            }
            try
            {
                _broadcastSubscription = await _broadcastStream.SubscribeAsync(
                    OnBroadcastReceived,
                    OnSubscriptionError);
                if (attempt > 0)
                {
                    _logger.LogInformation(
                        "Subscribed to dashboard broadcast stream after {Attempts} attempt(s)",
                        attempt + 1);
                }
                return;
            }
            catch (Exception ex) when (attempt < SubscribeBackoff.Length)
            {
                _logger.LogWarning(
                    ex,
                    "Dashboard broadcast subscribe failed (attempt {Attempt}); retrying in {Delay}",
                    attempt + 1,
                    SubscribeBackoff[attempt]);
                try
                {
                    await Task.Delay(SubscribeBackoff[attempt], cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Dashboard broadcast subscribe permanently failed after {Attempts} attempts; live dashboard updates disabled on this silo",
                    SubscribeBackoff.Length + 1);
                return;
            }
        }
    }

    /// <summary>
    /// Stream <c>onError</c> callback. Logs the failure and kicks off
    /// a background resubscribe so a transient queue-agent fault
    /// doesn't permanently silence the feed on this silo. The
    /// existing handle is dropped — <see cref="SubscribeWithRetryAsync"/>
    /// will allocate a fresh one.
    /// </summary>
    private Task OnSubscriptionError(Exception ex)
    {
        _logger.LogWarning(ex, "Dashboard broadcast stream reported an error; attempting resubscribe");
        _broadcastSubscription = null;
        _ = Task.Run(() => SubscribeWithRetryAsync(_shutdownCts.Token));
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _shutdownCts.Cancel();

        _router.FactRouted -= OnFactForBroadcast;
        _router.FactReplicated -= OnFactForBroadcast;
        _router.ChaosConfigChanged -= OnChaosConfigChanged;

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
    }

    /// <summary>
    /// Builds a fresh snapshot for every part in the lattice backend.
    /// Components call this in <c>OnInitializedAsync</c> before starting
    /// their live subscription.
    /// </summary>
    public async Task<IReadOnlyList<PartSummaryUpdate>> GetInitialPartsAsync(CancellationToken cancellationToken = default)
    {
        var lattice = _router.GetBackend("lattice");
        var serials = await lattice.ListPartsAsync(cancellationToken);
        var results = new List<PartSummaryUpdate>(serials.Count);
        foreach (var serial in serials)
        {
            results.Add(await BuildSummaryAsync(serial, cancellationToken));
        }
        return results;
    }

    /// <summary>Reads the current chaos overview (used by the banner on initial render).</summary>
    public async Task<ChaosOverview> GetChaosOverviewAsync(CancellationToken cancellationToken = default)
    {
        var sites = await _router.ListSitesAsync();
        var backends = await _router.ListBackendChaosAsync();
        var partitioned = await _grainFactory
            .GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey)
            .IsPartitionedAsync();
        var replicationDisconnected = await _grainFactory
            .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
            .IsDisconnectedAsync();
        return BuildOverview(sites, backends, partitioned, replicationDisconnected);
    }

    /// <summary>
    /// Returns every part currently in a divergent state — baseline
    /// disagrees with lattice. Used by the <c>WatchDivergence</c> gRPC
    /// stream to seed its initial snapshot before switching to the live
    /// subscription.
    /// </summary>
    public async Task<IReadOnlyList<DivergenceEvent>> GetInitialDivergenceAsync(CancellationToken cancellationToken = default)
    {
        var initial = await GetInitialPartsAsync(cancellationToken);
        var results = new List<DivergenceEvent>();
        foreach (var part in initial)
        {
            if (part.Diverges)
            {
                results.Add(new DivergenceEvent
                {
                    Serial = part.Serial,
                    BaselineState = part.BaselineState,
                    LatticeState = part.LatticeState,
                    Resolved = false,
                });
            }
        }
        return results;
    }

    /// <summary>
    /// Live feed of part-summary updates. Yields one message per
    /// <see cref="FederationRouter.FactRouted"/> event, skipping parts
    /// the caller never asked about (the UI filters client-side).
    /// </summary>
    public async IAsyncEnumerable<PartSummaryUpdate> SubscribePartUpdates(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<PartSummaryUpdate>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _partSubs[id] = channel;
        try
        {
            await foreach (var update in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return update;
            }
        }
        finally
        {
            _partSubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>Live feed of chaos-overview updates.</summary>
    public async IAsyncEnumerable<ChaosOverview> SubscribeChaosChanges(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<ChaosOverview>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _chaosSubs[id] = channel;
        try
        {
            await foreach (var update in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return update;
            }
        }
        finally
        {
            _chaosSubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Live feed of divergence events. Yields a new <see cref="DivergenceEvent"/>
    /// whenever a part's baseline/lattice agreement changes — enters
    /// divergence, stays divergent with a new state pair, or resolves
    /// (<see cref="DivergenceEvent.Resolved"/> is <c>true</c>).
    /// </summary>
    public async IAsyncEnumerable<DivergenceEvent> SubscribeDivergence(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<DivergenceEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _divSubs[id] = channel;
        try
        {
            await foreach (var update in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return update;
            }
        }
        finally
        {
            _divSubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Live feed of site-activity entries — one message per fact
    /// routed locally (<see cref="FederationRouter.FactRouted"/>) or
    /// replicated from a peer cluster
    /// (<see cref="FederationRouter.FactReplicated"/>). The dashboard's
    /// "Inventory By Activity" sub-tab consumes this and merges entries
    /// that match its currently-selected <see cref="ProcessSite"/> into
    /// the displayed grid so a user watching a site sub-tab sees new
    /// activity appear immediately without re-running the range scan.
    /// </summary>
    /// <remarks>
    /// The broadcaster does not filter by site — every subscriber
    /// receives every entry and filters client-side. Volumes are
    /// modest (one entry per fact) and the per-subscriber channel is
    /// unbounded, matching the back-pressure model of the existing
    /// <see cref="SubscribePartUpdates"/> and
    /// <see cref="SubscribeChaosChanges"/> feeds.
    /// </remarks>
    public async IAsyncEnumerable<SiteActivityIndexEntry> SubscribeSiteActivity(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<SiteActivityIndexEntry>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _activitySubs[id] = channel;
        try
        {
            await foreach (var entry in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return entry;
            }
        }
        finally
        {
            _activitySubs.TryRemove(id, out _);
            channel.Writer.TryComplete();
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

        _router.FactRouted -= OnFactForBroadcast;
        _router.FactReplicated -= OnFactForBroadcast;
        _router.ChaosConfigChanged -= OnChaosConfigChanged;

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

    private void OnFactForBroadcast(object? sender, Fact fact) => _ = PublishToBroadcastStreamAsync(fact);

    private void OnChaosConfigChanged(object? sender, EventArgs e) => _ = PublishChaosAsync();

    /// <summary>
    /// Publishes the fact to the cluster-wide broadcast stream so
    /// every silo's broadcaster — including this one — can fan it out
    /// to its locally-attached Blazor circuits. Fire-and-forget from
    /// the router's perspective: a publish failure must not propagate
    /// back into the router's synchronous fan-out. Retries transient
    /// storage-queue failures with bounded exponential backoff before
    /// giving up — a dropped publish means one Blazor update is lost,
    /// not a persistent feed outage.
    /// </summary>
    private async Task PublishToBroadcastStreamAsync(Fact fact)
    {
        var stream = _broadcastStream;
        if (stream is null)
        {
            return;
        }
        for (var attempt = 0; attempt <= PublishBackoff.Length; attempt++)
        {
            if (_shutdownCts.IsCancellationRequested)
            {
                return;
            }
            try
            {
                await stream.OnNextAsync(fact);
                if (attempt > 0)
                {
                    _logger.LogInformation(
                        "Published fact {FactId} after {Attempts} attempt(s)",
                        fact.FactId,
                        attempt + 1);
                }
                return;
            }
            catch (Exception ex) when (attempt < PublishBackoff.Length)
            {
                _logger.LogDebug(
                    ex,
                    "Transient publish failure for fact {FactId} (attempt {Attempt}); retrying in {Delay}",
                    fact.FactId,
                    attempt + 1,
                    PublishBackoff[attempt]);
                try
                {
                    await Task.Delay(PublishBackoff[attempt], _shutdownCts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to publish fact {FactId} to dashboard broadcast stream after {Attempts} attempts",
                    fact.FactId,
                    PublishBackoff.Length + 1);
                return;
            }
        }
    }

    /// <summary>
    /// Handles a fact delivered via the cluster-wide broadcast stream.
    /// Runs on every subscribed silo, so per-circuit fan-out happens
    /// locally wherever the Blazor session is attached, regardless of
    /// which silo originated the fact. Combines the part-summary /
    /// divergence fan-out (<see cref="PublishPartAsync"/>) with the
    /// site-activity fan-out in a single entry point so the two paths
    /// can't drift (e.g. one wired to the stream and the other still
    /// local-only).
    /// </summary>
    /// <remarks>
    /// Swallows exceptions so a single bad fact cannot poison the
    /// stream agent: if the handler throws, Orleans would retry
    /// delivery indefinitely, blocking every subsequent message on
    /// the same queue. Per-step fan-out already has its own
    /// try/catch; the top-level catch here is the last line of
    /// defence.
    /// </remarks>
    private async Task OnBroadcastReceived(Fact fact, StreamSequenceToken? token)
    {
        try
        {
            await PublishPartAsync(fact);
            FanOutSiteActivity(fact);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Dashboard fan-out threw for fact {FactId}; dropping to protect stream agent",
                fact.FactId);
        }
    }

    /// <summary>
    /// Builds a <see cref="SiteActivityIndexEntry"/> from the in-memory
    /// fact and fans it out to every site-activity subscriber. Exposed
    /// as a standalone helper so <see cref="OnBroadcastReceived"/> can
    /// share the same logic as tests that invoke it directly.
    /// </summary>
    private void FanOutSiteActivity(Fact fact)
    {
        try
        {
            var entry = new SiteActivityIndexEntry(
                fact.Site,
                fact.Serial,
                fact.Hlc,
                SiteActivityIndex.DescribeActivity(fact));
            foreach (var sub in _activitySubs.Values)
            {
                sub.Writer.TryWrite(entry);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fan out site-activity entry for fact {FactId}", fact.FactId);
        }
    }

    private async Task PublishPartAsync(Fact fact)
    {
        try
        {
            var update = await BuildSummaryAsync(fact.Serial, CancellationToken.None);
            foreach (var sub in _partSubs.Values)
            {
                sub.Writer.TryWrite(update);
            }

            // Derive a divergence transition, if any, and fan that out on
            // the divergence channel. We publish on:
            //   - entry into divergence (previous absent or agreed; now disagrees)
            //   - state change while still divergent (both backends' states
            //     have shifted but they still disagree)
            //   - resolution (previously disagreed; now agrees)
            var newStates = (update.BaselineState, update.LatticeState);
            _lastStates.TryGetValue(update.Serial, out var oldStates);
            _lastStates[update.Serial] = newStates;

            var nowDiverges = update.Diverges;
            var wasDiverging = oldStates != default && oldStates.Baseline != oldStates.Lattice;

            if (!nowDiverges && !wasDiverging)
            {
                return;
            }

            DivergenceEvent? evt = null;
            if (nowDiverges && (!wasDiverging || oldStates != newStates))
            {
                evt = new DivergenceEvent
                {
                    Serial = update.Serial,
                    BaselineState = update.BaselineState,
                    LatticeState = update.LatticeState,
                    Resolved = false,
                };
            }
            else if (!nowDiverges && wasDiverging)
            {
                evt = new DivergenceEvent
                {
                    Serial = update.Serial,
                    BaselineState = update.BaselineState,
                    LatticeState = update.LatticeState,
                    Resolved = true,
                };
            }

            if (evt is not null)
            {
                foreach (var sub in _divSubs.Values)
                {
                    sub.Writer.TryWrite(evt);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build dashboard update for fact {FactId}", fact.FactId);
        }
    }

    private async Task PublishChaosAsync()
    {
        try
        {
            var overview = await GetChaosOverviewAsync();
            foreach (var sub in _chaosSubs.Values)
            {
                sub.Writer.TryWrite(overview);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to build chaos overview update");
        }
    }

    private async Task<PartSummaryUpdate> BuildSummaryAsync(
        PartSerialNumber serial,
        CancellationToken cancellationToken)
    {
        var baseline = _router.GetBackend("baseline");
        var lattice = _router.GetBackend("lattice");

        // One lattice-tree enumeration per summary: fetch facts once and
        // fold them locally for the lattice state. Opening a second
        // concurrent enumerator (via lattice.GetStateAsync, which itself
        // calls GetFactsAsync) multiplies the pressure on the tree grain.
        // Enumerator aborts (cold-start, scale-down, idle-expiry) are
        // recovered transparently inside LatticeFactBackend via the
        // resilient ScanEntriesAsync wrapper.
        var baselineStateTask = baseline.GetStateAsync(serial, cancellationToken);
        var factsTask = lattice.GetFactsAsync(serial, cancellationToken);
        await Task.WhenAll(baselineStateTask, factsTask);

        var facts = factsTask.Result;
        var latticeState = ComplianceFold.Fold(facts);
        // "Latest stage" reflects the part's furthest-along lifecycle
        // milestone, not just the last ProcessStepCompleted. The facts
        // list is HLC-ascending so the tail is the newest fact; map it
        // to a ProcessStage by fact kind — InspectionRecorded → NDT,
        // NCR/MRB/Rework → MRB, FinalAcceptance → FAI — otherwise a
        // FAI-accepted part would still show Machining.
        var latestStage = facts.Count == 0 ? null : StageOf(facts[^1]);

        return new PartSummaryUpdate
        {
            Serial = serial,
            Family = InferFamily(serial),
            LatestStage = latestStage,
            BaselineState = baselineStateTask.Result,
            LatticeState = latticeState,
            FactCount = facts.Count,
        };
    }

    private static ProcessStage? StageOf(Fact fact) => fact switch
    {
        ProcessStepCompleted step => step.Stage,
        InspectionRecorded => ProcessStage.NDT,
        NonConformanceRaised => ProcessStage.MRB,
        MrbDisposition => ProcessStage.MRB,
        ReworkCompleted => ProcessStage.MRB,
        FinalAcceptance => ProcessStage.FAI,
        _ => null,
    };

    private static ChaosOverview BuildOverview(
        IReadOnlyList<SiteState> sites,
        IReadOnlyList<BackendChaosState> backends,
        bool partitionActive,
        bool replicationDisconnected)
    {
        var paused = 0;
        var delayed = 0;
        var reordering = 0;
        foreach (var site in sites)
        {
            if (site.Config.IsPaused) paused++;
            if (site.Config.DelayMs > 0) delayed++;
            if (site.Config.ReorderEnabled) reordering++;
        }

        var flaky = new List<string>();
        foreach (var backend in backends)
        {
            if (backend.Config != BackendChaosConfig.Nominal)
            {
                flaky.Add(backend.Name);
            }
        }

        return new ChaosOverview
        {
            PausedSites = paused,
            DelayedSites = delayed,
            ReorderingSites = reordering,
            FlakyBackends = flaky,
            PartitionActive = partitionActive,
            ReplicationDisconnected = replicationDisconnected,
        };
    }

    private static string InferFamily(PartSerialNumber serial)
    {
        var value = serial.Value;
        var lastDash = value.LastIndexOf('-');
        if (lastDash <= 0)
        {
            return value;
        }
        var yearDash = value.LastIndexOf('-', lastDash - 1);
        return yearDash > 0 ? value[..yearDash] : value;
    }
}
