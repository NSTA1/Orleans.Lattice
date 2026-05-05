using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Surfaces per-peer "recently shipped" and "recently received" indicators
/// for the in-page replication status strip. Subscribes to the canonical
/// <see cref="LatticeReplicationMetrics.MeterName"/> meter via a
/// <see cref="MeterListener"/> and aggregates a small fixed set of
/// instruments into an in-memory snapshot the layout component can poll
/// on a 1-second cadence.
/// </summary>
/// <remarks>
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
internal sealed class ReplicationActivityTracker : IHostedService, IDisposable
{
    private const string ShipDurationName = "orleans.lattice.replication.ship.duration";

    private readonly MeterListener _listener = new();
    private readonly ConcurrentDictionary<string, PeerCounters> _byPeer =
        new(StringComparer.Ordinal);

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
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _listener.Dispose();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose() => _listener.Dispose();

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
        var peer = ReadTag(tags, LatticeReplicationMetrics.TagPeer);
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
/// aggregated by <see cref="ReplicationActivityTracker"/>.
/// </summary>
/// <param name="Peers">One entry per peer the local silo has observed activity against.</param>
internal sealed record ReplicationActivitySnapshot(IReadOnlyList<ReplicationPeerActivity> Peers);

/// <summary>
/// Per-peer counters and timestamps surfaced by
/// <see cref="ReplicationActivityTracker.Snapshot"/>.
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
internal sealed record ReplicationPeerActivity(
    string Peer,
    DateTime? LastSentUtc,
    DateTime? LastReceivedUtc,
    DateTime? LastSendErrorUtc,
    long RowsSent,
    long BatchesSent,
    long BatchesReceived,
    long SendErrors,
    long ApplyErrors);
