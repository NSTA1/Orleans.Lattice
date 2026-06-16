using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Replication-package <see cref="ILatticeReplicationContext"/> implementation.
/// Reports replication as enabled, exposes the configured
/// <see cref="LatticeReplicationOptions.ClusterId"/> as the local replica id,
/// and delegates per-tree merge-mode resolution to the existing
/// <see cref="ILatticeMergeModeResolver"/> (which is itself backed by
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> and caches per tree),
/// keeping a single configuration source of truth.
/// <para>
/// It surfaces configuration only - never transport endpoints, peer topology,
/// or secrets - so core features can consume it without taking a dependency on
/// the replication wire surface.
/// </para>
/// </summary>
internal sealed class ConfiguredLatticeReplicationContext(
    ILatticeMergeModeResolver mergeModeResolver,
    IOptionsMonitor<LatticeReplicationOptions> options) : ILatticeReplicationContext
{
    /// <inheritdoc />
    public bool IsReplicationEnabled => true;

    /// <inheritdoc />
    public string LocalReplicaId => options.CurrentValue.ClusterId ?? string.Empty;

    /// <inheritdoc />
    public LatticeMergeMode? ResolveMergeMode(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return mergeModeResolver.Resolve(treeId);
    }
}
