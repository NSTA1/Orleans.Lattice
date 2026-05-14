namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal caching facade over <see cref="ILatticeReplicationSecretSource"/>.
/// Transports resolve credentials through this seam rather than calling
/// the user-supplied source directly; the cache absorbs the per-batch
/// invocation rate so a remote secret store is consulted at most once
/// per <see cref="LatticeReplicationSecurityOptions.SecretRefreshInterval"/>.
/// </summary>
internal interface IReplicationSecretProvider
{
    /// <summary>
    /// Returns the outbound shared secret for the named peer, served
    /// from the in-memory cache when one is current.
    /// </summary>
    ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the currently-accepted inbound secret snapshot, served
    /// from the in-memory cache when one is current.
    /// </summary>
    ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Constant-time check: returns <see langword="true"/> when
    /// <paramref name="presented"/> matches any entry in the currently
    /// cached accepted-set. Implemented as a single helper so the
    /// caller does not have to enumerate <see cref="LatticeReplicationAcceptedSecrets.Secrets"/>
    /// directly (which would invite non-constant-time comparisons).
    /// </summary>
    ValueTask<bool> IsAcceptedAsync(string? presented, CancellationToken cancellationToken);
}
