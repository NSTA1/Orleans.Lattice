using Microsoft.Extensions.Options;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The dynamic <see cref="IReplicatedTreeMembership"/> for a host that opted into
/// runtime replication configuration via
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>.
/// A tree is replicated when it is enabled in the compiled
/// <see cref="CompiledReplicationConfig"/> snapshot <b>or</b> present in the
/// static <see cref="LatticeReplicationOptions.ReplicatedTrees"/> seed map, so the
/// options map continues to act as a floor while runtime enables overlay on top.
/// Replaces <see cref="OptionsReplicatedTreeMembership"/> when the config-tree
/// anchor is active.
/// </summary>
internal sealed class SnapshotReplicatedTreeMembership(
    CompiledReplicationConfigSnapshotMaintainer maintainer,
    IOptionsMonitor<LatticeReplicationOptions> options) : IReplicatedTreeMembership
{
    /// <inheritdoc />
    public IReadOnlyCollection<string> ReplicatedTrees
    {
        get
        {
            maintainer.EnsureWarmStarted();
            var snapshot = maintainer.Current;
            var staticMap = options.CurrentValue.ReplicatedTrees;
            var enabled = snapshot.EnabledTrees;

            var set = new HashSet<string>(StringComparer.Ordinal);
            for (var i = 0; i < enabled.Count; i++)
            {
                set.Add(enabled[i]);
            }

            if (staticMap is not null)
            {
                foreach (var treeId in staticMap.Keys)
                {
                    set.Add(treeId);
                }
            }

            return set;
        }
    }

    /// <inheritdoc />
    public bool IsReplicated(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        maintainer.EnsureWarmStarted();
        if (maintainer.Current.TryGetTree(treeId, out var projection) && projection.Enabled)
        {
            return true;
        }

        var staticMap = options.CurrentValue.ReplicatedTrees;
        return staticMap is not null && staticMap.ContainsKey(treeId);
    }
}
