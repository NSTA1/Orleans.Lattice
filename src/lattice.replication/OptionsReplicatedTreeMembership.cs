using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The real <see cref="IReplicatedTreeMembership"/> for a replication-enabled
/// host. Projects the per-tree opt-in map on
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> so the backup
/// package's fail-fast shared-sink guard, and the restore saga dispatcher, can
/// learn which trees participate in the cross-cluster replication set without
/// the backup package taking a dependency on the replication package.
/// <para>
/// Registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// to replace the backup package's default no-op
/// <see cref="NoReplicatedTreeMembership"/>. The no-op remains the fallback for a
/// backup-only host where replication is not wired. Reads
/// <see cref="IOptionsMonitor{TOptions}.CurrentValue"/> on every call so a
/// runtime options change is reflected without a restart.
/// </para>
/// </summary>
internal sealed class OptionsReplicatedTreeMembership(
    IOptionsMonitor<LatticeReplicationOptions> options) : IReplicatedTreeMembership
{
    /// <inheritdoc />
    public IReadOnlyCollection<string> ReplicatedTrees
    {
        get
        {
            var map = options.CurrentValue.ReplicatedTrees;
            return map is null || map.Count == 0
                ? Array.Empty<string>()
                : [.. map.Keys];
        }
    }

    /// <inheritdoc />
    public bool IsReplicated(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var map = options.CurrentValue.ReplicatedTrees;
        return map is not null && map.ContainsKey(treeId);
    }
}
