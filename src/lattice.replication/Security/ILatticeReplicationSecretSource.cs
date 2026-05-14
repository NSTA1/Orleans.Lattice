namespace Orleans.Lattice.Replication;

/// <summary>
/// Transport-agnostic source of shared-secret material used to
/// authenticate cross-cluster replication batches. Implementations
/// supply the outbound secret stamped on locally-shipped batches and
/// the set of inbound secrets accepted by the receiver. The default
/// implementation is environment-variable-backed
/// (<see cref="LatticeReplicationEnvironmentVariables"/>); hosts that
/// need richer rotation semantics register their own implementation
/// via <see cref="LatticeReplicationSecurityServiceCollectionExtensions.AddLatticeReplicationSecrets{TSource}(Orleans.Hosting.ISiloBuilder)"/>.
/// </summary>
/// <remarks>
/// <para>
/// The interface is deliberately small and async so it can be
/// implemented over arbitrary secret stores - <c>Azure.Identity</c>,
/// AWS Secrets Manager, HashiCorp Vault, etc. The authenticator
/// caches the result with a configurable
/// <see cref="LatticeReplicationSecurityOptions.SecretRefreshInterval"/>,
/// so a remote-store implementation is not called on every batch.
/// </para>
/// <para>
/// Implementations <b>must</b> be safe to call from multiple threads
/// concurrently; the authenticator may invoke them from any of its
/// timer ticks.
/// </para>
/// </remarks>
public interface ILatticeReplicationSecretSource
{
    /// <summary>
    /// Returns the outbound shared secret stamped on batches the local
    /// cluster ships to the named peer. Returning
    /// <see langword="null"/> or an empty string causes the local
    /// shipper to attach no credential, which the receiver's
    /// authenticator will reject as <c>Unauthenticated</c>.
    /// </summary>
    /// <param name="peerClusterId">
    /// The cluster id of the destination peer. Implementations that
    /// partition secrets per peer key off this value; implementations
    /// that use a single cluster-wide secret ignore it.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the set of inbound secrets the local cluster currently
    /// accepts. The receiver authenticates a batch when its presented
    /// credential matches any entry in the returned snapshot, which
    /// is what enables zero-downtime rotation: the operator publishes
    /// <c>{old, new}</c>, flips senders to <c>new</c>, then narrows
    /// the accepted set to <c>{new}</c>.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken);
}
