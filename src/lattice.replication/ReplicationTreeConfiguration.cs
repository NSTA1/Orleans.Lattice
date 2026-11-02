using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Decides whether a tree id has any replication configuration on this cluster.
/// The shipper uses it to recognise a stray: a shipper for a name that was never
/// enrolled, such as an operator alias of an enrolled tree (#5676). The alias
/// observer uses it to avoid activating one.
/// </summary>
internal static class ReplicationTreeConfiguration
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="treeId"/> resolves no
    /// merge mode, is absent from the static
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map, and has no row
    /// in the runtime replication configuration. A runtime-disabled or ambiguous
    /// tree still has its row, so it is configured. The mode is resolved for
    /// <paramref name="treeId"/> itself and never through a registry alias.
    /// </summary>
    /// <param name="replicationContext">The host's replication context.</param>
    /// <param name="options">The replication options.</param>
    /// <param name="runtimeConfig">The runtime configuration snapshot, when the host has one.</param>
    /// <param name="treeId">The tree id to classify.</param>
    /// <returns>Whether the tree has no replication configuration at all.</returns>
    public static async ValueTask<bool> IsUnconfiguredAsync(
        ILatticeReplicationContext replicationContext,
        IOptionsMonitor<LatticeReplicationOptions> options,
        CompiledReplicationConfigSnapshotMaintainer? runtimeConfig,
        string treeId)
    {
        ArgumentNullException.ThrowIfNull(replicationContext);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(treeId);

        if (replicationContext.ResolveMergeMode(treeId) is not null
            || options.CurrentValue.ReplicatedTrees?.ContainsKey(treeId) == true
            || options.Get(treeId).ReplicatedTrees?.ContainsKey(treeId) == true)
        {
            return false;
        }

        if (runtimeConfig is null)
        {
            return true;
        }

        // A cold snapshot would report a disabled tree as unknown.
        await runtimeConfig.EnsureWarmAsync().ConfigureAwait(false);
        return !runtimeConfig.Current.TryGetTree(treeId, out _);
    }
}
