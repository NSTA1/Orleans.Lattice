using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Test <see cref="ILatticeReplicationContext"/> that mirrors the integration
/// fixtures' silo-side <c>AllowAllLwwRegisterResolver</c>: it opts every tree
/// in to replication, resolving a per-tree merge mode from a shared overrides
/// map and defaulting any un-listed tree to
/// <see cref="LatticeMergeMode.LwwRegister"/>. It supplies the receiver-side
/// enrollment source the applier requires after the fail-closed hardening
/// (issue #1398) without forcing each test to enumerate its ad-hoc tree ids.
/// </summary>
internal sealed class OverridesReplicationContext(
    ConcurrentDictionary<string, LatticeMergeMode>? overrides = null,
    string localReplicaId = "") : ILatticeReplicationContext
{
    private readonly ConcurrentDictionary<string, LatticeMergeMode> _overrides = overrides ?? new();

    /// <inheritdoc />
    public bool IsReplicationEnabled => true;

    /// <inheritdoc />
    public string LocalReplicaId => localReplicaId;

    /// <inheritdoc />
    public LatticeMergeMode? ResolveMergeMode(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return _overrides.TryGetValue(treeId, out var mode) ? mode : LatticeMergeMode.LwwRegister;
    }
}
