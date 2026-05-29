using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// ASP.NET Core / Kubernetes probe target for the replication shipper.
/// Aggregates the per-peer telemetry captured by
/// <see cref="ReplicationPeerStats"/> into a single
/// <see cref="HealthCheckResult"/>: the worst per-peer classification
/// across <see cref="LatticeReplicationHealthCheckOptions.EntriesBehind"/>,
/// <see cref="LatticeReplicationHealthCheckOptions.LastContactSeconds"/>,
/// and <see cref="LatticeReplicationHealthCheckOptions.ConsecutiveErrors"/>
/// becomes the aggregate verdict. A peer that sits in the
/// <see cref="HealthStatus.Degraded"/> tier for longer than
/// <see cref="LatticeReplicationHealthCheckOptions.UnhealthyAfter"/> is
/// escalated to <see cref="HealthStatus.Unhealthy"/>.
/// </summary>
/// <remarks>
/// The check is intentionally pure with respect to its own state: the
/// only mutable surface is a per-peer "first-degraded-at" timestamp map
/// used to implement the sustained-degraded escalation. The check does
/// not poll, schedule, or invoke RPCs - it is driven entirely by the
/// host's health-checks pipeline, so it is cheap to call on a Kubernetes
/// readiness probe cadence. Registered under the name
/// <see cref="LatticeReplicationHealthCheckOptions.DefaultName"/> unless
/// the caller overrides it via
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplicationHealthCheck"/>.
/// </remarks>
internal sealed class LatticeReplicationHealthCheck(
    ReplicationPeerStats peerStats,
    IOptionsMonitor<LatticeReplicationHealthCheckOptions> optionsMonitor,
    ILogger<LatticeReplicationHealthCheck> logger,
    TimeProvider? timeProvider = null) : IHealthCheck
{
    private readonly ReplicationPeerStats _peerStats =
        peerStats ?? throw new ArgumentNullException(nameof(peerStats));
    private readonly IOptionsMonitor<LatticeReplicationHealthCheckOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILogger<LatticeReplicationHealthCheck> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Per-peer "first-degraded-at" timestamps. A peer is added on the
    /// transition from <c>Healthy</c> to <c>Degraded</c> and removed on
    /// the inverse transition. The map size is bounded by the number of
    /// distinct <c>(tree, peer)</c> pairs the local sender has ever
    /// recorded telemetry for, which is itself bounded by
    /// <see cref="ReplicationPeerStats"/>'s own state map.
    /// </summary>
    private readonly ConcurrentDictionary<PeerKey, DateTimeOffset> _degradedSince = new();

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();

        var name = string.IsNullOrEmpty(context.Registration?.Name)
            ? LatticeReplicationHealthCheckOptions.DefaultName
            : context.Registration.Name;
        var options = _optionsMonitor.Get(name);
        var snapshot = _peerStats.Snapshot();
        var now = _time.GetUtcNow();

        // Track which keys we observed this probe so any peer that
        // dropped out of telemetry between probes is cleared from the
        // degraded-since map.
        var observed = new HashSet<PeerKey>();
        var aggregate = HealthStatus.Healthy;
        var data = new Dictionary<string, object>(StringComparer.Ordinal);
        var degradedPeers = new List<string>();
        var unhealthyPeers = new List<string>();

        foreach (var peer in snapshot)
        {
            var key = new PeerKey(peer.Tree, peer.Peer, peer.Direction);
            observed.Add(key);

            var perPeer = Classify(peer, options);
            if (perPeer == HealthStatus.Degraded)
            {
                var since = _degradedSince.GetOrAdd(key, now);
                if (options.UnhealthyAfter > TimeSpan.Zero
                    && now - since >= options.UnhealthyAfter)
                {
                    perPeer = HealthStatus.Unhealthy;
                }
            }
            else if (perPeer == HealthStatus.Healthy)
            {
                _degradedSince.TryRemove(key, out _);
            }
            else
            {
                // Hard-unhealthy: drop any prior degraded-since record so
                // a future recovery starts a fresh grace window.
                _degradedSince.TryRemove(key, out _);
            }

            var label = peer.Direction == ReplicationContactDirection.Outbound
                ? $"{peer.Tree}/{peer.Peer}"
                : $"{peer.Tree}/{peer.Peer} (inbound)";
            if (perPeer == HealthStatus.Degraded)
            {
                degradedPeers.Add(label);
            }
            else if (perPeer == HealthStatus.Unhealthy)
            {
                unhealthyPeers.Add(label);
            }

            if (perPeer < aggregate)
            {
                aggregate = perPeer;
            }
        }

        // Garbage-collect degraded-since entries for peers that fell out
        // of telemetry between probes (e.g. a removed cross-cluster peer).
        foreach (var stale in _degradedSince.Keys)
        {
            if (!observed.Contains(stale))
            {
                _degradedSince.TryRemove(stale, out _);
            }
        }

        data["peers"] = snapshot.Count;
        data["degraded"] = degradedPeers.Count;
        data["unhealthy"] = unhealthyPeers.Count;
        if (degradedPeers.Count > 0)
        {
            data["degradedPeers"] = degradedPeers.ToArray();
        }
        if (unhealthyPeers.Count > 0)
        {
            data["unhealthyPeers"] = unhealthyPeers.ToArray();
        }

        var description = aggregate switch
        {
            HealthStatus.Healthy => "All replication peers are within configured back-pressure thresholds.",
            HealthStatus.Degraded => $"{degradedPeers.Count} replication peer(s) degraded.",
            HealthStatus.Unhealthy => $"{unhealthyPeers.Count} replication peer(s) unhealthy, {degradedPeers.Count} degraded.",
            _ => null,
        };

        if (aggregate != HealthStatus.Healthy)
        {
            _logger.LogDebug(
                "Replication health check returning {Status}: {Description}",
                aggregate,
                description);
        }

        return Task.FromResult(new HealthCheckResult(aggregate, description, exception: null, data: data));
    }

    /// <summary>
    /// Per-peer classification before sustained-degraded escalation.
    /// Returns the worst of the three signals' classifications. A
    /// <c>null</c> tier disables that signal.
    /// </summary>
    private static HealthStatus Classify(
        ReplicationPeerSnapshot peer,
        LatticeReplicationHealthCheckOptions options)
    {
        var worst = HealthStatus.Healthy;

        // entries_behind / bytes_behind and the outbound contact /
        // error tiers apply only to outbound rows (the receiver does
        // not track an outbound backlog from itself). Inbound rows
        // carry zero EntriesBehind and zero ConsecutiveErrors by
        // construction so the existing tier comparisons would never
        // trigger on them anyway, but skipping the comparison up
        // front keeps the intent legible.
        if (peer.Direction == ReplicationContactDirection.Outbound)
        {
            if (options.EntriesBehind is { } entries)
            {
                if (peer.EntriesBehind > entries.Unhealthy)
                {
                    worst = HealthStatus.Unhealthy;
                }
                else if (peer.EntriesBehind > entries.Degraded && worst > HealthStatus.Degraded)
                {
                    worst = HealthStatus.Degraded;
                }
            }

            if (options.LastContactSeconds is { } contact && !double.IsNaN(peer.LastContactSeconds))
            {
                if (peer.LastContactSeconds > contact.Unhealthy)
                {
                    worst = HealthStatus.Unhealthy;
                }
                else if (peer.LastContactSeconds > contact.Degraded && worst > HealthStatus.Degraded)
                {
                    worst = HealthStatus.Degraded;
                }
            }

            if (options.ConsecutiveErrors is { } errors)
            {
                if (peer.ConsecutiveErrors > errors.Unhealthy)
                {
                    worst = HealthStatus.Unhealthy;
                }
                else if (peer.ConsecutiveErrors > errors.Degraded && worst > HealthStatus.Degraded)
                {
                    worst = HealthStatus.Degraded;
                }
            }
        }
        else
        {
            // Inbound row. Only the optional inbound-silence tier is
            // meaningful; EntriesBehind / BytesBehind are zero by
            // construction and ConsecutiveErrors on inbound is
            // covered by a future inbound-error tier (not in this
            // option set).
            var crit = options.InboundCriticalAfter;
            var degr = options.InboundDegradedAfter;
            if (!double.IsNaN(peer.LastContactSeconds))
            {
                if (crit != System.Threading.Timeout.InfiniteTimeSpan
                    && peer.LastContactSeconds > crit.TotalSeconds)
                {
                    worst = HealthStatus.Unhealthy;
                }
                else if (degr != System.Threading.Timeout.InfiniteTimeSpan
                    && peer.LastContactSeconds > degr.TotalSeconds
                    && worst > HealthStatus.Degraded)
                {
                    worst = HealthStatus.Degraded;
                }
            }
        }

        return worst;
    }

    private readonly record struct PeerKey(string Tree, string Peer, ReplicationContactDirection Direction);
}
