namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree anti-entropy digest-probe scheduler grain. On a low-frequency,
/// jittered cadence it reads each shard's local projection digest and
/// compares it against every peer's digest fetched over the replication
/// push transport, emitting the digest-probe comparison and mismatch
/// counters. The probe is strictly read-only and ships dark (gated on
/// <see cref="LatticeReplicationOptions.DigestProbeEnabled"/>, default
/// off).
/// <para>
/// Grain key format: tree name verbatim. One activation per tree; silo
/// loss triggers automatic migration via the standard Orleans
/// cluster-singleton model.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationDigestProbeGrain)]
internal interface IReplicationDigestProbeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Activates the grain and registers its keepalive reminder so it
    /// runs forever (until the host is shut down). Idempotent. A no-op
    /// in terms of comparison work while
    /// <see cref="LatticeReplicationOptions.DigestProbeEnabled"/> is
    /// <see langword="false"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task EnsureActiveAsync(CancellationToken cancellationToken);
}
