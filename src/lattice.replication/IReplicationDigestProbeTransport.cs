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
    /// <see cref="MerkleWalkAbortReason.RemoteUnavailable"/> when no transport
    /// that can compute a key-range subtree digest on the remote side is wired
    /// in. The gRPC binding overrides this: it resolves the peer's
    /// <see cref="ILattice.GetLeafProjectionDigestForRangeAsync"/> over the
    /// same per-peer channel cache the push transport uses, folding the remote
    /// shard subtree bounded by the request's separator-key range into a
    /// content-comparable digest. The fold is content-only, so two clusters
    /// holding the same logical entries in the range compute the same digest
    /// independently of each cluster's physical B+ tree layout.
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

    /// <summary>
    /// Asks the peer identified by <paramref name="targetClusterId"/> for the
    /// high-water-mark clock it has durably applied for the
    /// <paramref name="treeName"/> stream originating from
    /// <paramref name="originClusterId"/>. The targeted leaf re-replay repair
    /// stage uses this cursor to bound which retained write-ahead-log entries it
    /// re-ships: only entries whose clock is strictly greater than the returned
    /// value can be missing remotely, so re-sending is bounded to the genuine
    /// gap rather than the whole retained range.
    /// <para>
    /// The probe is strictly read-only. A default implementation returns
    /// <see cref="Orleans.Lattice.HybridLogicalClock.Zero"/> - the conservative
    /// answer that re-ships every in-range retained entry and relies on the
    /// receiver's per-origin idempotent dedup to discard duplicates. The gRPC
    /// binding overrides this: it resolves the peer's
    /// <c>IReplicationHighWaterMarkGrain.GetAsync(originClusterId)</c> over the
    /// same per-peer channel cache the push transport uses, so the re-replay
    /// bound is tightened to the genuine gap and re-ships only entries above
    /// the peer's reported watermark.
    /// </para>
    /// </summary>
    /// <param name="targetClusterId">The peer cluster id to probe. Must be non-empty.</param>
    /// <param name="treeName">The logical replicated-tree name.</param>
    /// <param name="originClusterId">The origin cluster id whose applied watermark is requested.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<Orleans.Lattice.HybridLogicalClock> GetPeerHighWaterMarkAsync(
        string targetClusterId,
        string treeName,
        string originClusterId,
        CancellationToken cancellationToken)
        => Task.FromResult(Orleans.Lattice.HybridLogicalClock.Zero);

    /// <summary>
    /// Advertises a per-batch content-hash manifest to the peer identified
    /// by <paramref name="targetClusterId"/> and returns the subset of
    /// manifest entries the peer is missing (does not already hold
    /// byte-identical content for), so the sender can ship only the
    /// genuinely-needed payloads and elide the redundant ones. This is the
    /// sender-manifest / receiver-pull-missing half of the opt-in
    /// content-hash payload-elision round trip; it composes with, and is
    /// gated behind, the same content-hash dedup master switch that drives
    /// the re-send-rate measurement.
    /// <para>
    /// The exchange is read-only with respect to the shipped value bytes -
    /// it never ships a payload - but it is <b>not</b> side-effect free on
    /// the receiver: for a manifest entry whose content the receiver
    /// already holds but whose <see cref="ContentManifestEntry.Hlc"/> is
    /// newer than the receiver's recorded clock for that key (the
    /// idempotent re-set of an identical value), the receiver advances its
    /// per-origin high-water-mark via a metadata-only apply and reports the
    /// advanced clock in <see cref="ContentManifestResponse.AdvancedHlc"/>
    /// without the payload ever travelling. This preserves the
    /// advance-strictly-on-ack cursor contract and per-origin
    /// high-water-mark monotonicity across the elision path.
    /// </para>
    /// <para>
    /// A default implementation returns
    /// <see cref="ContentManifestResponse.NotSupported"/> -
    /// <see cref="ContentManifestResponse.ExchangeSupported"/> is
    /// <see langword="false"/> - so a transport (or peer) that has not
    /// implemented the exchange behaves exactly as today: the sender treats
    /// every entry as missing and ships the full batch verbatim. Real
    /// transports (the gRPC binding) override this to invoke the
    /// pull-missing RPC over the same per-peer channel cache the push
    /// transport uses.
    /// </para>
    /// </summary>
    /// <param name="targetClusterId">The peer cluster id to exchange with. Must be non-empty.</param>
    /// <param name="request">The tree id, origin cluster id, and per-entry content-hash manifest.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ContentManifestResponse> ExchangeContentManifestAsync(
        string targetClusterId,
        ContentManifestRequest request,
        CancellationToken cancellationToken)
        => Task.FromResult(ContentManifestResponse.NotSupported);

    /// <summary>
    /// Pulls the raw bytes of a shared compression dictionary the local
    /// provider does not yet hold from a peer that advertised it, so an
    /// auto-training cluster converges onto a peer's trained dictionary
    /// instead of failing to decode frames compressed against it. The
    /// caller passes the advertised dictionary id; the peer answers with
    /// the bytes and their content fingerprint, which the caller verifies
    /// against the advertised fingerprint before installing.
    /// <para>
    /// A default implementation returns
    /// <see cref="CompressionDictionaryPullResponse.NotSupported"/> -
    /// <see cref="CompressionDictionaryPullResponse.ExchangeSupported"/> is
    /// <see langword="false"/> - so a transport (or peer) that has not
    /// implemented the pull behaves exactly as today: the caller leaves the
    /// dictionary uninstalled and retries on a later tick. Real transports
    /// (the gRPC binding) override this to invoke the pull RPC over the
    /// same per-peer channel cache the push transport uses.
    /// </para>
    /// </summary>
    /// <param name="targetClusterId">The peer cluster id to pull from. Must be non-empty.</param>
    /// <param name="request">The advertised dictionary id to resolve.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<CompressionDictionaryPullResponse> PullCompressionDictionaryAsync(
        string targetClusterId,
        CompressionDictionaryPullRequest request,
        CancellationToken cancellationToken)
        => Task.FromResult(CompressionDictionaryPullResponse.NotSupported);
}
