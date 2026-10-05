namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// gRPC metadata (header) names used by
/// <c>Orleans.Lattice.Replication.Grpc</c> for transport-level
/// authentication. The shared-secret credential travels as a custom
/// metadata header rather than as an <c>Authorization: Bearer</c>
/// entry so that an upstream HTTP-level auth filter on the receiver
/// host is free to enforce a different scheme without conflicting with
/// this transport's authenticator.
/// </summary>
internal static class LatticeReplicationGrpcMetadataNames
{
    /// <summary>
    /// Header that carries the outbound shared-secret credential.
    /// Sent by the gRPC sender on every batch when the
    /// <see cref="IReplicationSecretProvider"/> resolves a non-null
    /// secret for the destination peer; read by the receiver-side
    /// interceptor and matched against the accepted-set.
    /// </summary>
    public const string SecretHeader = "x-lattice-replication-secret";

    /// <summary>
    /// Header that carries the sender's local cluster id. Sent on outbound
    /// live-push, digest-probe, snapshot, and saga-control calls for peer
    /// attribution. Receiver-side gates refuse origin-taking calls when it is
    /// absent or disagrees with the body-declared origin. With credential-to-origin
    /// binding enabled, the shared-secret interceptor also requires the presented
    /// secret to match the one configured for the stamped origin.
    /// </summary>
    public const string OriginClusterIdHeader = "x-lattice-replication-origin";

    /// <summary>
    /// Header that carries <see cref="ReplicationBatch.ReseedAfterEpoch"/> on a
    /// live push (issue #4534): present only while the sender has taken the
    /// receiver off the log and waits for it to re-seed.
    /// </summary>
    public const string ReseedAfterEpochHeader = "x-lattice-replication-reseed-after";

    /// <summary>
    /// Header that carries the sender's applied low watermark for the batch's
    /// tree at the receiver (issue #4586) on a push. Read only after the
    /// caller's origin is authenticated, parsed strictly and bounded; a
    /// missing or malformed value means the batch vouches for nothing.
    /// </summary>
    public const string SourceFrontierHeader = "x-lattice-replication-source-frontier";

    /// <summary>
    /// Header that carries the source tree lineage the sender read a pushed
    /// batch under (issue #4673), as a <see cref="Guid"/> in the <c>D</c>
    /// format. Read only after the caller's origin is authenticated and parsed
    /// strictly. A receiver that drained the sender under another lineage
    /// refuses the batch; a missing header (a sender that predates it) applies
    /// as before, and a malformed one is refused.
    /// </summary>
    public const string SourceLineageHeader = "x-lattice-replication-source-lineage";
}
