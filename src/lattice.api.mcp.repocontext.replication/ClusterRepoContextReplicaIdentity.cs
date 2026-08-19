using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication;

/// <summary>
/// The multi-cluster <see cref="IRepoContextReplicaIdentity"/>: authors agent-memory
/// CRDT writes under the replication <see cref="LatticeReplicationOptions.ClusterId"/>
/// so two clusters' concurrent writes to the same memory key mint distinct dots and
/// both survive the merge. Falls back to the local single-cluster id when the cluster
/// id is unset, so a partially-configured host still writes a stable, non-empty
/// replica id rather than throwing on the write path.
/// </summary>
/// <param name="options">The replication options monitor supplying the cluster id.</param>
internal sealed class ClusterRepoContextReplicaIdentity(IOptionsMonitor<LatticeReplicationOptions> options)
    : IRepoContextReplicaIdentity
{
    /// <inheritdoc />
    public string ReplicaId
    {
        get
        {
            var clusterId = options.Get(Options.DefaultName).ClusterId;
            return string.IsNullOrWhiteSpace(clusterId)
                ? LocalRepoContextReplicaIdentity.LocalReplicaId
                : clusterId;
        }
    }
}
