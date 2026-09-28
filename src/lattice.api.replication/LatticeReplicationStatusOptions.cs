namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Options for the replication peer-status facade
/// (<see cref="ILatticeReplicationStatus"/>): the thresholds that turn a link's
/// measured backlog, error streak and time since last contact into its
/// <see cref="ReplicationLinkHealth"/>. Bound by
/// <see cref="LatticeApiReplicationServiceCollectionExtensions.AddLatticeReplicationStatusApi"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each signal has a <i>lagging</i> and a <i>stalled</i> bound. A link whose
/// signal is strictly greater than the lagging bound is at least
/// <see cref="ReplicationLinkHealth.Lagging"/>; strictly greater than the stalled
/// bound, <see cref="ReplicationLinkHealth.Stalled"/>. The worst signal wins.
/// Setting a bound to <see langword="null"/> disables it. A link that has never
/// made a successful contact and trips no bound is
/// <see cref="ReplicationLinkHealth.Unknown"/>.
/// </para>
/// <para>
/// The outbound defaults match the replication health check's defaults
/// (<c>LatticeReplicationHealthCheckOptions</c>), so the facade and the readiness
/// probe agree out of the box. Backlog bounds apply to outbound links only, since
/// an inbound link tracks no backlog. The error-streak bounds apply in both
/// directions. The inbound contact bounds are disabled by default, because an
/// inbound link is refreshed only when the peer writes, so an idle peer is not an
/// unhealthy one.
/// </para>
/// </remarks>
public sealed class LatticeReplicationStatusOptions
{
    /// <summary>Default for <see cref="LaggingEntriesBehind"/>: 1,000 entries.</summary>
    public const long DefaultLaggingEntriesBehind = 1_000L;

    /// <summary>Default for <see cref="StalledEntriesBehind"/>: 10,000 entries.</summary>
    public const long DefaultStalledEntriesBehind = 10_000L;

    /// <summary>Default for <see cref="LaggingConsecutiveErrors"/>: 5 errors.</summary>
    public const long DefaultLaggingConsecutiveErrors = 5L;

    /// <summary>Default for <see cref="StalledConsecutiveErrors"/>: 50 errors.</summary>
    public const long DefaultStalledConsecutiveErrors = 50L;

    /// <summary>Default for <see cref="LaggingAfterNoContact"/>: 30 seconds.</summary>
    public static readonly TimeSpan DefaultLaggingAfterNoContact = TimeSpan.FromSeconds(30);

    /// <summary>Default for <see cref="StalledAfterNoContact"/>: 5 minutes.</summary>
    public static readonly TimeSpan DefaultStalledAfterNoContact = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Outbound backlog, in WAL entries, above which a link is
    /// <see cref="ReplicationLinkHealth.Lagging"/>. Defaults to
    /// <see cref="DefaultLaggingEntriesBehind"/>; <see langword="null"/> disables it.
    /// </summary>
    public long? LaggingEntriesBehind { get; set; } = DefaultLaggingEntriesBehind;

    /// <summary>
    /// Outbound backlog, in WAL entries, above which a link is
    /// <see cref="ReplicationLinkHealth.Stalled"/>. Defaults to
    /// <see cref="DefaultStalledEntriesBehind"/>; <see langword="null"/> disables it.
    /// </summary>
    public long? StalledEntriesBehind { get; set; } = DefaultStalledEntriesBehind;

    /// <summary>
    /// Consecutive failed contacts, in either direction, above which a link is
    /// <see cref="ReplicationLinkHealth.Lagging"/>. Defaults to
    /// <see cref="DefaultLaggingConsecutiveErrors"/>; <see langword="null"/> disables it.
    /// </summary>
    public long? LaggingConsecutiveErrors { get; set; } = DefaultLaggingConsecutiveErrors;

    /// <summary>
    /// Consecutive failed contacts, in either direction, above which a link is
    /// <see cref="ReplicationLinkHealth.Stalled"/>. Defaults to
    /// <see cref="DefaultStalledConsecutiveErrors"/>; <see langword="null"/> disables it.
    /// </summary>
    public long? StalledConsecutiveErrors { get; set; } = DefaultStalledConsecutiveErrors;

    /// <summary>
    /// Time since the last successful outbound contact above which a link is
    /// <see cref="ReplicationLinkHealth.Lagging"/>. The outbound liveness probe
    /// refreshes an idle link, so silence here means the peer is not answering.
    /// Defaults to <see cref="DefaultLaggingAfterNoContact"/>; <see langword="null"/> disables it.
    /// </summary>
    public TimeSpan? LaggingAfterNoContact { get; set; } = DefaultLaggingAfterNoContact;

    /// <summary>
    /// Time since the last successful outbound contact above which a link is
    /// <see cref="ReplicationLinkHealth.Stalled"/>. Defaults to
    /// <see cref="DefaultStalledAfterNoContact"/>; <see langword="null"/> disables it.
    /// </summary>
    public TimeSpan? StalledAfterNoContact { get; set; } = DefaultStalledAfterNoContact;

    /// <summary>
    /// Time since the peer's entries were last applied locally above which an
    /// inbound link is <see cref="ReplicationLinkHealth.Lagging"/>. Disabled
    /// (<see langword="null"/>) by default: an inbound link is refreshed only
    /// when the peer writes.
    /// </summary>
    public TimeSpan? InboundLaggingAfterNoContact { get; set; }

    /// <summary>
    /// Time since the peer's entries were last applied locally above which an
    /// inbound link is <see cref="ReplicationLinkHealth.Stalled"/>. Disabled
    /// (<see langword="null"/>) by default.
    /// </summary>
    public TimeSpan? InboundStalledAfterNoContact { get; set; }
}
