using Orleans.Runtime;
using Orleans.Runtime.Services;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Cluster-wide <see cref="IReplicationPeerStatusReader"/>: reads every active
/// silo's <see cref="IReplicationPeerStatusGrainService"/> and merges the answers.
/// <see cref="ReplicationPeerStats"/> is a per-silo singleton, so a read of the
/// local silo alone would silently omit every link whose shipper or applier ran
/// elsewhere.
/// </summary>
internal sealed class ClusterReplicationPeerStatusReader
    : GrainServiceClient<IReplicationPeerStatusGrainService>, IReplicationPeerStatusReader
{
    private readonly IClusterMembershipService _membership;

    /// <summary>Initialises the reader.</summary>
    /// <param name="serviceProvider">The silo service provider. Must not be <see langword="null"/>.</param>
    /// <param name="membership">The cluster membership view used to enumerate active silos. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public ClusterReplicationPeerStatusReader(IServiceProvider serviceProvider, IClusterMembershipService membership)
        : base(serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider)))
    {
        ArgumentNullException.ThrowIfNull(membership);
        _membership = membership;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ReplicationPeerStatusRow>> ReadAsync(
        ReplicationPeerStatusReadRequest request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        var silos = new List<SiloAddress>();
        foreach (var member in _membership.CurrentSnapshot.Members.Values)
        {
            if (member.Status == SiloStatus.Active)
            {
                silos.Add(member.SiloAddress);
            }
        }

        return ReplicationPeerStatusFanOut.ReadAsync(
            silos,
            (silo, req) => GetGrainService(silo).ReadAsync(req),
            request,
            cancellationToken);
    }
}
