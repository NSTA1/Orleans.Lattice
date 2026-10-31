using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// The replication credential the two sample regions share: one secret,
/// generated for each run and never written anywhere, that each region presents
/// to its peer and accepts from it.
/// </summary>
/// <remarks>
/// Authenticating the link is not optional on a tenant estate. A region joining
/// a tenant backfills the tenant's trees with snapshots from an Online region,
/// and that region exports a tenant snapshot only to a peer whose identity the
/// transport has authenticated by binding its credential to its origin cluster
/// (<see cref="LatticeReplicationSecurityOptions.BindCredentialToOriginCluster"/>,
/// which needs <see cref="LatticeReplicationSecurityOptions.RequireAuthentication"/>).
/// With either off, the export is refused and the joining region stays
/// <c>Backfilling</c>. One secret for both directions satisfies origin binding:
/// the secret a region would send its peer is the one the peer presents.
/// </remarks>
/// <param name="secret">The run's shared secret.</param>
internal sealed class SampleLinkSecretSource(string secret) : ILatticeReplicationSecretSource
{
    private readonly LatticeReplicationAcceptedSecrets _accepted = new([secret], "sample-run");

    /// <inheritdoc />
    public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken) =>
        ValueTask.FromResult<string?>(secret);

    /// <inheritdoc />
    public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult(_accepted);
}
