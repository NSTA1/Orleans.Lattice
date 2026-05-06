using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MultiSiteManufacturing.Host.Replication.Grains;
using Orleans;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Per-silo bridge from the canonical <see cref="LatticeReplicationMetrics.MeterName"/>
/// meter into a <see cref="ReplicationActivitySnapshot"/>, plus a
/// background loop that pushes that snapshot to the cluster-wide
/// <see cref="IClusterReplicationActivityGrain"/> every two seconds.
/// </summary>
/// <remarks>
/// <para>
/// The package's <c>ReplicationShipperGrain</c> activates on a single
/// silo per <c>(tree, peer)</c> pair, so its meter measurements only fire
/// in that silo's process. Without the cluster-wide aggregation hop,
/// browsers sticky-cookied to a non-shipping silo would never see the
/// `ship&#x2192;` indicator light up. The push-into-grain pattern lets
/// every silo's listener contribute its slice; the grain merges them
/// (max-of-timestamps, sum-of-counters) and the layout polls the grain.
/// </para>
/// <para>
/// Outbound activity is sourced from
/// <see cref="LatticeReplicationMetrics.WalEntriesShippedName"/> (per-entry
/// throughput) and <c>orleans.lattice.replication.ship.duration</c>
/// (per-batch outcome timestamps). Inbound activity is sourced from
/// <see cref="LatticeReplicationMetrics.ApplyDurationName"/> (per-batch
/// apply outcomes). Counters accumulate process-lifetime; the layout
/// renders only "time since last contact" so monotonic drift is fine.
/// </para>
/// <para>
/// The bind-mounted Grafana dashboards remain the canonical operator
/// surface for these signals - this adapter exists purely so the demo's
/// in-page status row keeps working from the same meter the dashboards
/// scrape, without re-introducing a host-rolled tracker grain.
/// </para>
/// </remarks>
internal sealed class ReplicationActivityTracker(
    IGrainFactory grains,
    SiloIdentity identity,
    IOptionsMonitor<LatticeReplicationOptions> replicationOptions,
    ILogger<ReplicationActivityTracker> logger)
    : IHostedService, IDisposable
{
    private const string ShipDurationName = "orleans.lattice.replication.ship.duration";
    private static readonly TimeSpan PushInterval = TimeSpan.FromMilliseconds(500);

    private readonly MeterListener _listener = new();
    private readonly ConcurrentDictionary<string, PeerCounters> _byPeer =
        new(StringComparer.Ordinal);

    private CancellationTokenSource? _stopCts;
    private Task? _pushTask;

    /// <summary>
    /// Returns a point-in-time snapshot of every peer the local silo has
    /// observed activity for. Empty until the first ship or apply batch.
    /// </summary>
    public ReplicationActivitySnapshot Snapshot()
    {
        var peers = new List<ReplicationPeerActivity>(_byPeer.Count);
        foreach (var (peer, counters) in _byPeer)
        {
            peers.Add(new ReplicationPeerActivity(
                peer,
                FromTicks(Interlocked.Read(ref counters.LastSentTicks)),
                FromTicks(Interlocked.Read(ref counters.LastReceivedTicks)),
                FromTicks(Interlocked.Read(ref counters.LastSendErrorTicks)),
                Interlocked.Read(ref counters.RowsSent),
                Interlocked.Read(ref counters.BatchesSent),
                Interlocked.Read(ref counters.BatchesReceived),
                Interlocked.Read(ref counters.SendErrors),
                Interlocked.Read(ref counters.ApplyErrors)));
        }
        return new ReplicationActivitySnapshot(peers);
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _listener.InstrumentPublished = OnInstrumentPublished;
        _listener.SetMeasurementEventCallback<long>(OnLongMeasurement);
        _listener.SetMeasurementEventCallback<double>(OnDoubleMeasurement);
        _listener.Start();

        _stopCts = new CancellationTokenSource();
        _pushTask = Task.Run(() => PushLoopAsync(_stopCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _listener.Dispose();
        if (_stopCts is { } cts)
        {
            try { cts.Cancel(); } catch (ObjectDisposedException) { }
        }
        if (_pushTask is { } task)
        {
            try { await task.ConfigureAwait(false); }
            catch (OperationCanceledException) { /* expected on shutdown */ }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _listener.Dispose();
        _stopCts?.Dispose();
    }

    /// <summary>
    /// Background loop: every <see cref="PushInterval"/>, push the local
    /// silo's snapshot into the cluster-wide grain so browsers connected
    /// to other silos can see this silo's slice of activity. Also fires
    /// an immediate push on entry so the cluster grain has data within a
    /// single tick of silo startup, instead of waiting a full interval.
    /// </summary>
    private async Task PushLoopAsync(CancellationToken cancellationToken)
    {
        var clusterGrain = grains.GetGrain<IClusterReplicationActivityGrain>(
            IClusterReplicationActivityGrain.SingletonKey);
        var localSiloId = $"{identity.ClusterName}-{identity.Id}";

        // Eager first push: don't make the UI wait a full PushInterval after
        // silo startup before any activity becomes visible. Silo runtime may
        // not be fully addressable yet on the very first attempt - the
        // catch below absorbs that and the timer-driven loop retries.
        await PushOnceAsync(clusterGrain, localSiloId, cancellationToken).ConfigureAwait(false);

        using var timer = new PeriodicTimer(PushInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            await PushOnceAsync(clusterGrain, localSiloId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PushOnceAsync(
        IClusterReplicationActivityGrain clusterGrain,
        string localSiloId,
        CancellationToken cancellationToken)
    {
        try
        {
            await clusterGrain.ReportAsync(localSiloId, Snapshot()).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected on shutdown.
        }
        catch (Exception ex)
        {
            // Replication can be running without the cluster grain
            // being addressable yet (e.g. early boot, gateway flap).
            // Swallow at debug-level - the next tick will retry.
            logger.LogDebug(ex, "Push of replication activity snapshot to cluster grain failed");
        }
    }

    private static void OnInstrumentPublished(Instrument instrument, MeterListener listener)
    {
        if (instrument.Meter.Name != LatticeReplicationMetrics.MeterName)
        {
            return;
        }

        switch (instrument.Name)
        {
            case LatticeReplicationMetrics.WalEntriesShippedName:
            case ShipDurationName:
            case LatticeReplicationMetrics.ApplyDurationName:
                listener.EnableMeasurementEvents(instrument);
                break;
        }
    }

    private void OnLongMeasurement(
        Instrument instrument,
        long measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        if (instrument.Name != LatticeReplicationMetrics.WalEntriesShippedName)
        {
            return;
        }

        var peer = ReadTag(tags, LatticeReplicationMetrics.TagPeer);
        if (peer is null)
        {
            return;
        }

        var counters = _byPeer.GetOrAdd(peer, static _ => new PeerCounters());
        Interlocked.Add(ref counters.RowsSent, measurement);
        Interlocked.Exchange(ref counters.LastSentTicks, DateTime.UtcNow.Ticks);
    }

    private void OnDoubleMeasurement(
        Instrument instrument,
        double measurement,
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        object? state)
    {
        // The package's apply.duration histogram is recorded by the
        // peer-agnostic ReplicationApplier with only a `tree` and
        // `outcome` tag. To attribute inbound activity to a peer in
        // the demo's status strip, fall back to the configured peer
        // for the tree (the package's per-tree options carry the list)
        // when the meter callback does not provide one. The sample's
        // topology is single-peer-per-cluster, so this is unambiguous.
        var peer = ReadTag(tags, LatticeReplicationMetrics.TagPeer)
            ?? ResolveFallbackPeer(instrument, tags);
        if (peer is null)
        {
            return;
        }

        var outcome = ReadTag(tags, LatticeReplicationMetrics.TagOutcome);
        var counters = _byPeer.GetOrAdd(peer, static _ => new PeerCounters());
        var nowTicks = DateTime.UtcNow.Ticks;

        if (instrument.Name == ShipDurationName)
        {
            if (string.Equals(outcome, LatticeReplicationMetrics.OutcomeFailure, StringComparison.Ordinal))
            {
                Interlocked.Increment(ref counters.SendErrors);
                Interlocked.Exchange(ref counters.LastSendErrorTicks, nowTicks);
            }
            else
            {
                Interlocked.Increment(ref counters.BatchesSent);
                Interlocked.Exchange(ref counters.LastSentTicks, nowTicks);
            }
        }
        else if (instrument.Name == LatticeReplicationMetrics.ApplyDurationName)
        {
            if (string.Equals(outcome, LatticeReplicationMetrics.OutcomeFailure, StringComparison.Ordinal))
            {
                Interlocked.Increment(ref counters.ApplyErrors);
            }
            else
            {
                Interlocked.Increment(ref counters.BatchesReceived);
                Interlocked.Exchange(ref counters.LastReceivedTicks, nowTicks);
            }
        }
    }

    /// <summary>
    /// Resolves a peer name when the meter callback omitted the
    /// <see cref="LatticeReplicationMetrics.TagPeer"/> tag. The package's
    /// <c>apply.duration</c> histogram is recorded peer-agnostically -
    /// only the <c>tree</c> tag is present - so to surface inbound
    /// activity per peer we look the tree id up in the per-tree
    /// <see cref="LatticeReplicationOptions"/>. With the sample's
    /// single-peer topology this is unambiguous; for richer topologies
    /// the operator dashboards (Grafana / OTel) remain the canonical
    /// per-peer breakdown.
    /// </summary>
    private string? ResolveFallbackPeer(
        Instrument instrument,
        ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        if (instrument.Name != LatticeReplicationMetrics.ApplyDurationName)
        {
            return null;
        }

        var treeId = ReadTag(tags, LatticeReplicationMetrics.TagTree);
        if (treeId is null)
        {
            return null;
        }

        var opts = replicationOptions.Get(treeId);
        var peers = opts.ReplicationPeers;
        if (peers is null || peers.Count == 0)
        {
            return null;
        }

        // Single peer: unambiguous attribution. Multiple peers: pick
        // the first as a best-effort label - operators wanting per-peer
        // accuracy should consult the dashboards.
        return peers.First();
    }

    private static string? ReadTag(
        ReadOnlySpan<KeyValuePair<string, object?>> tags,
        string key)
    {
        foreach (var pair in tags)
        {
            if (string.Equals(pair.Key, key, StringComparison.Ordinal))
            {
                return pair.Value as string;
            }
        }
        return null;
    }

    private static DateTime? FromTicks(long ticks) =>
        ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);

    private sealed class PeerCounters
    {
        public long LastSentTicks;
        public long LastReceivedTicks;
        public long LastSendErrorTicks;
        public long RowsSent;
        public long BatchesSent;
        public long BatchesReceived;
        public long SendErrors;
        public long ApplyErrors;
    }
}

/// <summary>
/// Process-wide point-in-time view of the per-peer activity counters
/// aggregated by <see cref="ReplicationActivityTracker"/>. Used both as
/// the in-process snapshot the layout polls and as the wire payload the
/// per-silo tracker pushes into <see cref="IClusterReplicationActivityGrain"/>.
/// </summary>
/// <param name="Peers">One entry per peer the silo has observed activity against.</param>
/// <param name="ContributingSilos">Cluster-qualified silo ids whose pushes contributed to this snapshot. Empty for per-silo snapshots; populated only by the cluster aggregator.</param>
[GenerateSerializer]
internal sealed record ReplicationActivitySnapshot(
    [property: Id(0)] IReadOnlyList<ReplicationPeerActivity> Peers,
    [property: Id(1)] IReadOnlyList<string> ContributingSilos)
{
    /// <summary>Convenience constructor for per-silo snapshots that don't carry contributor metadata.</summary>
    public ReplicationActivitySnapshot(IReadOnlyList<ReplicationPeerActivity> peers)
        : this(peers, System.Array.Empty<string>())
    {
    }
}

/// <summary>
/// Per-peer counters and timestamps surfaced by
/// <see cref="ReplicationActivityTracker.Snapshot"/> and merged
/// across silos by <see cref="IClusterReplicationActivityGrain"/>.
/// </summary>
/// <param name="Peer">The remote peer cluster id (matches the <c>peer</c> meter tag).</param>
/// <param name="LastSentUtc">Wall-clock time of the most recent successful outbound batch or per-entry ship; <see langword="null"/> if never observed.</param>
/// <param name="LastReceivedUtc">Wall-clock time of the most recent successful inbound apply batch from this peer; <see langword="null"/> if never observed.</param>
/// <param name="LastSendErrorUtc">Wall-clock time of the most recent failed outbound batch; <see langword="null"/> if never observed.</param>
/// <param name="RowsSent">Process-lifetime count of WAL entries acknowledged by this peer.</param>
/// <param name="BatchesSent">Process-lifetime count of successful outbound ship batches to this peer.</param>
/// <param name="BatchesReceived">Process-lifetime count of successful inbound apply batches from this peer.</param>
/// <param name="SendErrors">Process-lifetime count of failed outbound ship attempts.</param>
/// <param name="ApplyErrors">Process-lifetime count of failed inbound apply attempts.</param>
[GenerateSerializer]
internal sealed record ReplicationPeerActivity(
    [property: Id(0)] string Peer,
    [property: Id(1)] DateTime? LastSentUtc,
    [property: Id(2)] DateTime? LastReceivedUtc,
    [property: Id(3)] DateTime? LastSendErrorUtc,
    [property: Id(4)] long RowsSent,
    [property: Id(5)] long BatchesSent,
    [property: Id(6)] long BatchesReceived,
    [property: Id(7)] long SendErrors,
    [property: Id(8)] long ApplyErrors);
