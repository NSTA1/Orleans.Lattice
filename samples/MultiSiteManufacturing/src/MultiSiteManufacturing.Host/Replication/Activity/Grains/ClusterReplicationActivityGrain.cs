using MultiSiteManufacturing.Host.Replication;

namespace MultiSiteManufacturing.Host.Replication.Grains;

/// <summary>
/// In-memory cluster-singleton aggregator for per-silo replication
/// activity snapshots. Holds the most recent push from every silo
/// (keyed by cluster-qualified silo id) with the wall-clock time it
/// arrived; <see cref="SnapshotAsync"/> drops entries older than
/// <see cref="StaleAfter"/> and merges the remainder per peer.
/// </summary>
/// <remarks>
/// State is deliberately non-persistent: the per-silo trackers re-push
/// every two seconds, so a grain re-activation repopulates within one
/// push interval. Persisting would only add latency without changing
/// observed behaviour for the demo's freshness windows.
/// </remarks>
internal sealed class ClusterReplicationActivityGrain : Grain, IClusterReplicationActivityGrain
{
    /// <summary>
    /// Window after which an unrenewed silo report is excluded from the
    /// merged view. Picked to comfortably exceed the per-silo push
    /// interval (2s) plus a few ticks of grain-call latency, so a healthy
    /// silo never fades out, while a crashed silo disappears within a
    /// single Blazor poll cycle of the layout's amber/stale thresholds.
    /// </summary>
    private static readonly TimeSpan StaleAfter = TimeSpan.FromSeconds(30);

    private readonly Dictionary<string, (DateTime When, ReplicationActivitySnapshot Snapshot)> _bySilo =
        new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task ReportAsync(string siloId, ReplicationActivitySnapshot snapshot)
    {
        ArgumentException.ThrowIfNullOrEmpty(siloId);
        ArgumentNullException.ThrowIfNull(snapshot);
        _bySilo[siloId] = (DateTime.UtcNow, snapshot);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<ReplicationActivitySnapshot> SnapshotAsync()
    {
        var cutoff = DateTime.UtcNow - StaleAfter;
        var byPeer = new Dictionary<string, ReplicationPeerActivity>(StringComparer.Ordinal);
        var contributing = new List<string>();

        foreach (var (siloId, entry) in _bySilo)
        {
            if (entry.When < cutoff)
            {
                continue;
            }
            contributing.Add(siloId);
            foreach (var peer in entry.Snapshot.Peers)
            {
                byPeer[peer.Peer] = byPeer.TryGetValue(peer.Peer, out var existing)
                    ? Merge(existing, peer)
                    : peer;
            }
        }

        contributing.Sort(StringComparer.Ordinal);
        var ordered = byPeer.Values
            .OrderBy(p => p.Peer, StringComparer.Ordinal)
            .ToList();
        return Task.FromResult(new ReplicationActivitySnapshot(ordered, contributing));
    }

    /// <summary>
    /// Per-peer merge: take the freshest timestamp on each axis (sent /
    /// received / send-error) and sum the process-lifetime counters
    /// across silos. Counter sums can overcount briefly during shipper
    /// grain migration, but the layout only renders relative timestamps
    /// and overflow-safe long arithmetic so the discrepancy is invisible
    /// to operators.
    /// </summary>
    private static ReplicationPeerActivity Merge(
        ReplicationPeerActivity a,
        ReplicationPeerActivity b) =>
        new(a.Peer,
            MaxNullable(a.LastSentUtc, b.LastSentUtc),
            MaxNullable(a.LastReceivedUtc, b.LastReceivedUtc),
            MaxNullable(a.LastSendErrorUtc, b.LastSendErrorUtc),
            a.RowsSent + b.RowsSent,
            a.BatchesSent + b.BatchesSent,
            a.BatchesReceived + b.BatchesReceived,
            a.SendErrors + b.SendErrors,
            a.ApplyErrors + b.ApplyErrors);

    private static DateTime? MaxNullable(DateTime? a, DateTime? b) =>
        (a, b) switch
        {
            (null, null) => null,
            (null, _) => b,
            (_, null) => a,
            _ => a > b ? a : b,
        };
}
