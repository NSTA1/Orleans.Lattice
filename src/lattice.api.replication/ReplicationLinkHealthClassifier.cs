using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Derives a link's <see cref="ReplicationLinkHealth"/> from the fields its
/// telemetry row already carries, against the configured
/// <see cref="LatticeReplicationStatusOptions"/> thresholds. Pure: it reads no
/// clock (the row's elapsed time was measured when it was read), so it is
/// deterministic under test.
/// </summary>
internal static class ReplicationLinkHealthClassifier
{
    /// <summary>
    /// Classifies <paramref name="row"/>. The worst signal wins; a link that has
    /// never made a successful contact and crosses no bound is
    /// <see cref="ReplicationLinkHealth.Unknown"/>.
    /// </summary>
    /// <param name="row">The link's telemetry row.</param>
    /// <param name="options">The thresholds. Must not be <see langword="null"/>.</param>
    /// <returns>The derived health.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public static ReplicationLinkHealth Classify(in ReplicationPeerStatusRow row, LatticeReplicationStatusOptions options) =>
        Classify(in row, options, out _);

    /// <summary>Classifies the link and returns the specific blocking cause, when stalled.</summary>
    /// <param name="row">The link's telemetry row.</param>
    /// <param name="options">The thresholds. Must not be <see langword="null"/>.</param>
    /// <param name="stallReason">The re-seed or dead-letter cause, or <see langword="null"/>.</param>
    /// <returns>The derived health.</returns>
    public static ReplicationLinkHealth Classify(
        in ReplicationPeerStatusRow row,
        LatticeReplicationStatusOptions options,
        out ReplicationLinkStallReason? stallReason)
    {
        ArgumentNullException.ThrowIfNull(options);

        stallReason = null;
        var contacted = !double.IsNaN(row.LastContactSeconds);
        var worst = ReplicationLinkHealth.Healthy;

        if (row.Direction == ReplicationContactDirection.Outbound)
        {
            worst = Worse(worst, Tier(row.EntriesBehind, options.LaggingEntriesBehind, options.StalledEntriesBehind));
            if (contacted)
            {
                worst = Worse(worst, Tier(row.LastContactSeconds, options.LaggingAfterNoContact, options.StalledAfterNoContact));
            }
        }
        else if (contacted)
        {
            worst = Worse(
                worst,
                Tier(row.LastContactSeconds, options.InboundLaggingAfterNoContact, options.InboundStalledAfterNoContact));
        }

        worst = Worse(
            worst,
            Tier(row.ConsecutiveErrors, options.LaggingConsecutiveErrors, options.StalledConsecutiveErrors));

        // A peer the sender took off the log after a trim lost records it never
        // shipped receives no saga records until it is re-seeded (#4534).
        if (row.ReseedRequiredSeconds is not null)
        {
            stallReason = ReplicationLinkStallReason.ReseedRequired;
            return ReplicationLinkHealth.Stalled;
        }

        // A full dead-letter queue keeps the link's next unparkable entry
        // unacknowledged, so the link makes no progress until an operator drains
        // the queue (#4603).
        if (row.DeadLetterFullSeconds is not null)
        {
            stallReason = ReplicationLinkStallReason.DeadLetterQueueFull;
            return ReplicationLinkHealth.Stalled;
        }

        return worst == ReplicationLinkHealth.Healthy && !contacted
            ? ReplicationLinkHealth.Unknown
            : worst;
    }

    private static ReplicationLinkHealth Tier(long value, long? lagging, long? stalled)
    {
        if (stalled is { } s && value > s)
        {
            return ReplicationLinkHealth.Stalled;
        }

        return lagging is { } l && value > l ? ReplicationLinkHealth.Lagging : ReplicationLinkHealth.Healthy;
    }

    private static ReplicationLinkHealth Tier(double seconds, TimeSpan? lagging, TimeSpan? stalled)
    {
        if (stalled is { } s && seconds > s.TotalSeconds)
        {
            return ReplicationLinkHealth.Stalled;
        }

        return lagging is { } l && seconds > l.TotalSeconds ? ReplicationLinkHealth.Lagging : ReplicationLinkHealth.Healthy;
    }

    private static ReplicationLinkHealth Worse(ReplicationLinkHealth a, ReplicationLinkHealth b) =>
        (int)a >= (int)b ? a : b;
}
