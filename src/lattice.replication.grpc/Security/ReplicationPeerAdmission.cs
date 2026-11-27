namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// How a method bound on the replication gRPC host admits its caller, as registered
/// with <see cref="ReplicationAdmittedPeer"/>. Every class first passes the shared-secret
/// interceptor, which (with <c>BindCredentialToOriginCluster</c> on) binds the presented
/// credential to the transport-stamped origin.
/// </summary>
internal enum ReplicationPeerAdmission
{
    /// <summary>
    /// The method consumes no body-declared origin; it answers a read under the
    /// interceptor's credential gate and its own per-tree enrollment checks.
    /// </summary>
    PeerRead,

    /// <summary>
    /// The method consumes a body-declared origin, which must equal the transport-stamped
    /// origin (<see cref="ReplicationAdmittedPeer.EnsureDeclaredOriginIsCaller"/>).
    /// </summary>
    OriginBound,

    /// <summary>
    /// A cross-cluster control method: the stamped origin must be the credential-bound
    /// peer (<see cref="ReplicationAdmittedPeer.CredentialBoundOrigin"/>) and an authorized
    /// peer of the current topology, and every body-declared identity is bound to it.
    /// </summary>
    ControlPeer,
}
