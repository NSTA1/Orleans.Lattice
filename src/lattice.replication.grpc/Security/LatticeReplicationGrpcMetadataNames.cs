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
    /// Header that carries the sender's local cluster id. Sent on every batch
    /// for diagnostics and peer attribution; the receiver-side interceptor does
    /// not use it to select a secret and validates against the configured accepted
    /// secret set.
    /// </summary>
    public const string OriginClusterIdHeader = "x-lattice-replication-origin";
}
