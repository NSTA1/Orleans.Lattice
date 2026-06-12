namespace Orleans.Lattice.Replication;

/// <summary>
/// Pluggable seam for the anti-entropy digest probe RPC. Separate from
/// <see cref="IReplicationTransport"/> so the probe transport can be
/// registered, substituted, and evolved independently of the live-push
/// transport. The default DI registration is a no-op so the rest of the
/// detection pipeline can be wired up in isolation; the gRPC binding
/// replaces it with a real implementation that invokes the probe RPC over
/// the same per-peer channel cache the push transport uses.
/// <para>
/// The probe is strictly read-only: implementations send a
/// <see cref="DigestProbeRequest"/> to the named peer and return the
/// peer's <see cref="DigestProbeResponse"/>. They never mutate data or
/// advance any cursor.
/// </para>
/// </summary>
public interface IReplicationDigestProbeTransport
{
    /// <summary>
    /// Asks the peer identified by <paramref name="targetClusterId"/> for
    /// its <see cref="LeafProjectionDigest"/> covering the shard named in
    /// <paramref name="request"/>, and returns the peer's response.
    /// </summary>
    /// <param name="targetClusterId">The peer cluster id to probe. Must be non-empty.</param>
    /// <param name="request">The tree id and shard index to probe.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DigestProbeResponse> ProbeDigestAsync(
        string targetClusterId,
        DigestProbeRequest request,
        CancellationToken cancellationToken);
}
