namespace Orleans.Lattice.Replication;

/// <summary>
/// Configuration thresholds for <see cref="LatticeReplicationHealthCheck"/>.
/// Bound via the named-options instance whose name matches the health check's
/// registered name (default <c>"orleans.lattice.replication"</c>); a host that
/// registers the health check under a different name binds against that name.
/// </summary>
/// <remarks>
/// The health check classifies every <c>(tree, peer)</c> pair captured in
/// <see cref="ReplicationPeerStats"/> against three tiered signals -
/// <see cref="EntriesBehind"/>, <see cref="LastContactSeconds"/>, and
/// <see cref="ConsecutiveErrors"/> - and aggregates the worst per-peer
/// classification into a single <c>Healthy</c> / <c>Degraded</c> /
/// <c>Unhealthy</c> verdict. A <c>Degraded</c> peer that does not recover
/// within <see cref="UnhealthyAfter"/> is escalated to <c>Unhealthy</c>.
/// <para>
/// Each tiered threshold ships with a soft (degraded) and hard (unhealthy)
/// bound. Setting either to <c>null</c> disables that signal entirely, so a
/// host that only cares about lag can null out the contact / error signals
/// without rebuilding the check.
/// </para>
/// </remarks>
public sealed class LatticeReplicationHealthCheckOptions
{
    /// <summary>
    /// Tiered bound on <see cref="ReplicationPeerSnapshot.EntriesBehind"/>:
    /// a peer whose backlog exceeds <see cref="Tier.Degraded"/> reports
    /// <c>Degraded</c>; a peer whose backlog exceeds <see cref="Tier.Unhealthy"/>
    /// reports <c>Unhealthy</c> immediately (no sustained-degraded grace
    /// window). Defaults to <see cref="DefaultEntriesBehind"/>.
    /// </summary>
    public LongTier? EntriesBehind { get; set; } = DefaultEntriesBehind;

    /// <summary>
    /// Tiered bound on <see cref="ReplicationPeerSnapshot.LastContactSeconds"/>:
    /// a peer whose last-successful-contact age exceeds
    /// <see cref="Tier.Degraded"/> reports <c>Degraded</c>; exceeding
    /// <see cref="Tier.Unhealthy"/> reports <c>Unhealthy</c> immediately.
    /// A peer that has never been contacted (NaN <c>LastContactSeconds</c>)
    /// is treated as not yet probed and excluded from this signal; the
    /// <see cref="ConsecutiveErrors"/> bound covers the
    /// "we tried and failed" case. Defaults to <see cref="DefaultLastContactSeconds"/>.
    /// </summary>
    public DoubleTier? LastContactSeconds { get; set; } = DefaultLastContactSeconds;

    /// <summary>
    /// Tiered bound on <see cref="ReplicationPeerSnapshot.ConsecutiveErrors"/>:
    /// a peer whose ship-attempt failure streak exceeds
    /// <see cref="Tier.Degraded"/> reports <c>Degraded</c>; exceeding
    /// <see cref="Tier.Unhealthy"/> reports <c>Unhealthy</c> immediately.
    /// Defaults to <see cref="DefaultConsecutiveErrors"/>.
    /// </summary>
    public LongTier? ConsecutiveErrors { get; set; } = DefaultConsecutiveErrors;

    /// <summary>
    /// Duration a peer must remain in the <c>Degraded</c> tier before its
    /// contribution to the aggregate verdict escalates to <c>Unhealthy</c>.
    /// Resets to <c>null</c> the moment the peer drops back below the
    /// soft (degraded) bound on every signal. Defaults to
    /// <see cref="DefaultUnhealthyAfter"/>; set to
    /// <see cref="TimeSpan.Zero"/> to disable the grace window and treat
    /// every degraded sample as unhealthy on the next probe.
    /// </summary>
    public TimeSpan UnhealthyAfter { get; set; } = DefaultUnhealthyAfter;

    /// <summary>
    /// Default for <see cref="EntriesBehind"/>: 1 000 entries soft,
    /// 10 000 entries hard. Sized so a steady-state catch-up of a few
    /// shipper batches does not flap the probe while a sustained backlog
    /// (~40 batches at the default <c>ShipBatchSize</c> of 256) registers
    /// promptly.
    /// </summary>
    public static readonly LongTier DefaultEntriesBehind = new(1_000L, 10_000L);

    /// <summary>
    /// Default for <see cref="LastContactSeconds"/>: 30 s soft, 5 min
    /// hard. Aligned with the maintenance fall-off probe cadence so a
    /// peer that misses one probe is degraded and a peer that misses
    /// ten probes is unhealthy.
    /// </summary>
    public static readonly DoubleTier DefaultLastContactSeconds = new(30d, 300d);

    /// <summary>
    /// Default for <see cref="ConsecutiveErrors"/>: 5 errors soft, 50
    /// errors hard. Sized so a small transient burst clears without
    /// flapping while a sustained outage (long enough for the backoff
    /// to ramp to <c>ShipBackoffMax</c>) escalates promptly.
    /// </summary>
    public static readonly LongTier DefaultConsecutiveErrors = new(5L, 50L);

    /// <summary>
    /// Default for <see cref="UnhealthyAfter"/>: 60 s. Sized so a
    /// transient degraded blip (one or two probe cadences) does not
    /// escalate while a sustained back-pressure event of a minute or
    /// more does.
    /// </summary>
    public static readonly TimeSpan DefaultUnhealthyAfter = TimeSpan.FromSeconds(60d);

    /// <summary>
    /// Default registered name for the health check. Hosts that register
    /// it under a different name supply the alternative name to
    /// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplicationHealthCheck"/>
    /// and bind named options under the same name.
    /// </summary>
    public const string DefaultName = "orleans.lattice.replication";

    /// <summary>
    /// Tiered bound shape for <see cref="long"/>-valued signals. A peer
    /// whose observed signal is strictly greater than <see cref="Degraded"/>
    /// classifies as at least <c>Degraded</c>; one whose observed signal
    /// is strictly greater than <see cref="Unhealthy"/> classifies as
    /// <c>Unhealthy</c> immediately.
    /// </summary>
    /// <param name="Degraded">Soft bound. Must be non-negative and less than or equal to <paramref name="Unhealthy"/>.</param>
    /// <param name="Unhealthy">Hard bound. Must be non-negative and greater than or equal to <paramref name="Degraded"/>.</param>
    public readonly record struct LongTier(long Degraded, long Unhealthy);

    /// <summary>
    /// Tiered bound shape for <see cref="double"/>-valued signals (today
    /// only <see cref="LastContactSeconds"/>). Semantics mirror
    /// <see cref="LongTier"/>; <see cref="double.NaN"/> samples are
    /// excluded from comparison.
    /// </summary>
    /// <param name="Degraded">Soft bound. Must be non-negative and less than or equal to <paramref name="Unhealthy"/>.</param>
    /// <param name="Unhealthy">Hard bound. Must be non-negative and greater than or equal to <paramref name="Degraded"/>.</param>
    public readonly record struct DoubleTier(double Degraded, double Unhealthy);
}
