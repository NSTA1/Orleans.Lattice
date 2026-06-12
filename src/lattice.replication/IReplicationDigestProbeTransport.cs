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

    /// <summary>
    /// Asks the peer identified by <paramref name="targetClusterId"/> for its
    /// subtree projection digest covering the cluster-stable key-range named in
    /// <paramref name="request"/> (<see cref="MerkleWalkProbeRequest.RangeStartKey"/>
    /// inclusive, <see cref="MerkleWalkProbeRequest.RangeEndKey"/> exclusive) at
    /// the requested walk depth, and returns the peer's response. Used by the
    /// read-only Merkle-walk drift-localisation pass to compare a local
    /// subtree's digest against the remote peer's digest for the same
    /// key-range, so divergence can be narrowed to a single leaf or small leaf
    /// set independently of each cluster's physical B+ tree layout.
    /// <para>
    /// The probe is strictly read-only. A default no-op implementation returns
    /// <see cref="MerkleWalkProbeResponse.Unavailable"/> so the localisation
    /// pass aborts cleanly with reason
    /// <see cref="MerkleWalkAbortReason.RemoteUnavailable"/> until a transport
    /// that can compute a key-range subtree digest on the remote side is wired
    /// in. Computing an arbitrary-key-range digest on the remote requires a
    /// range-fold over the remote leaf chain that the current shipping surface
    /// does not expose; see the anti-entropy Merkle-walk documentation for the
    /// honest scope of this limitation.
    /// </para>
    /// </summary>
    /// <param name="targetClusterId">The peer cluster id to probe. Must be non-empty.</param>
    /// <param name="request">The tree id, shard index, key-range, and depth to probe.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync(
        string targetClusterId,
        MerkleWalkProbeRequest request,
        CancellationToken cancellationToken)
        => Task.FromResult(MerkleWalkProbeResponse.Unavailable);
}
