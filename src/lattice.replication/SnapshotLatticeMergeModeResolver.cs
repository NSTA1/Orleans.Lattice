using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The dynamic <see cref="ILatticeMergeModeResolver"/> for a host that opted into
/// runtime replication configuration via
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(Orleans.Hosting.ISiloBuilder, System.Action{LatticeReplicationOptions}, bool)"/>.
/// It reads the compiled <see cref="CompiledReplicationConfig"/> snapshot first
/// and falls back to the static
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>-backed
/// <see cref="ConfiguredLatticeMergeModeResolver"/> when the tree carries no
/// usable runtime mode. Replaces the options-only resolver when the config-tree
/// anchor is active.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail closed.</b> When the snapshot marks a tree's mode ambiguous
/// (<see cref="ReplicationConfigProjection.Ambiguous"/> - the multi-value
/// register surfaced more than one live mode after a concurrent divergent
/// assignment), <see cref="Resolve"/> returns <see langword="null"/> so
/// <see cref="ReplicationMutationObserver"/> short-circuits before the sink and
/// shipping for that tree pauses. The resolver never silently picks one of the
/// divergent modes. Ambiguity wins even when the tree is also declared in the
/// static seed map, because the runtime divergence is a deliberate operator
/// state that must be resolved before egress resumes.
/// </para>
/// <para>
/// <b>Hot path.</b> <see cref="Resolve"/> reads a single volatile snapshot
/// reference and does one dictionary lookup; it allocates nothing and only
/// consults the fallback resolver (itself an O(1) cached dictionary read) when
/// the snapshot has no enabled, unambiguous mode for the tree.
/// </para>
/// </remarks>
internal sealed class SnapshotLatticeMergeModeResolver(
    CompiledReplicationConfigSnapshotMaintainer maintainer,
    ConfiguredLatticeMergeModeResolver fallback) : ILatticeMergeModeResolver
{
    /// <inheritdoc />
    public LatticeMergeMode? Resolve(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        maintainer.EnsureWarmStarted();

        if (maintainer.Current.TryGetTree(treeId, out var projection))
        {
            // Fail closed: a divergent (ambiguous) runtime mode pauses shipping
            // for this tree until an operator reconciles it. Never fall through
            // to the static fallback, which would silently pick a mode.
            if (projection.Ambiguous)
            {
                return null;
            }

            if (projection.Enabled && projection.Mode is { } mode)
            {
                return mode;
            }
        }

        // No enabled, unambiguous runtime mode: defer to the static seed/fallback
        // so existing static deployments and enabled-but-not-yet-moded trees keep
        // their configured behaviour.
        return fallback.Resolve(treeId);
    }
}
