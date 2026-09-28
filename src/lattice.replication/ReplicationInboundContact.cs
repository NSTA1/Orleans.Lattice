using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The single rule for attributing an inbound apply to its origin peer in
/// <see cref="ReplicationPeerStats"/>. Shared by the canonical
/// <see cref="ReplicationApplier"/> batch path and the
/// <see cref="DeadLetterTrackingReplicationApplier"/> branches that apply entries
/// one at a time, so every receive path records exactly one contact per applied
/// run, under the same rules.
/// </summary>
internal static class ReplicationInboundContact
{
    /// <summary>
    /// Records an inbound contact for <paramref name="representative"/>'s
    /// <c>(tree, origin)</c> pair. Entries with no origin (range deletes and other
    /// system-internal records) and local-origin entries (the loopback defence
    /// path) have no inbound peer and are skipped. Best-effort: never throws.
    /// </summary>
    /// <param name="peerStats">The telemetry state, or <see langword="null"/> when none is registered.</param>
    /// <param name="options">The replication options, read per tree for the local cluster id.</param>
    /// <param name="representative">An entry of the applied run.</param>
    /// <param name="success">Whether the run applied.</param>
    public static void Record(
        ReplicationPeerStats? peerStats,
        IOptionsMonitor<LatticeReplicationOptions> options,
        WalRecord representative,
        bool success)
    {
        if (peerStats is null
            || string.IsNullOrEmpty(representative.OriginClusterId)
            || string.IsNullOrEmpty(representative.TreeId))
        {
            return;
        }

        var resolved = options.Get(representative.TreeId);
        if (string.Equals(representative.OriginClusterId, resolved.ClusterId, StringComparison.Ordinal))
        {
            return;
        }

        if (success)
        {
            peerStats.RecordInboundSuccess(representative.TreeId, representative.OriginClusterId);
        }
        else
        {
            peerStats.RecordInboundError(representative.TreeId, representative.OriginClusterId);
        }
    }
}
