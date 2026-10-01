using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The receiver's single enrollment and merge-mode rule for inbound entries, shared by the
/// <see cref="ReplicationApplier"/> admission gate (issues #1267, #1398) and the inbound
/// per-peer contact attribution in <see cref="ReplicationInboundContact"/> (#4021), so a run
/// the gate drops can never be recorded as contact from its peer.
/// </summary>
internal static class ReplicationInboundAdmission
{
    /// <summary>
    /// Resolves the merge mode this receiver applies to <paramref name="treeId"/>, preferring the
    /// injected <see cref="ILatticeReplicationContext"/> (the same per-tree resolver the shipper,
    /// change feed and bootstrap path consult) and falling back to the raw
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map when no context is supplied.
    /// <paramref name="hasEnrollmentSource"/> reports whether any enrollment configuration was
    /// available at all; when it is <see langword="false"/> the gate cannot be evaluated and the
    /// caller fails closed. Returns <see langword="null"/> for a tree that has an enrollment source
    /// but is not enrolled.
    /// </summary>
    /// <param name="replicationContext">The host's replication context, or <see langword="null"/>.</param>
    /// <param name="options">The replication options, read per tree.</param>
    /// <param name="treeId">The tree to resolve.</param>
    /// <param name="hasEnrollmentSource">Whether any enrollment source was available.</param>
    /// <returns>The locally resolved merge mode, or <see langword="null"/>.</returns>
    public static LatticeMergeMode? ResolveLocalMergeMode(
        ILatticeReplicationContext? replicationContext,
        IOptionsMonitor<LatticeReplicationOptions> options,
        string treeId,
        out bool hasEnrollmentSource)
    {
        if (replicationContext is not null)
        {
            hasEnrollmentSource = true;
            return replicationContext.ResolveMergeMode(treeId);
        }

        var trees = options.Get(treeId).ReplicatedTrees;
        if (trees is not null)
        {
            hasEnrollmentSource = true;
            return trees.TryGetValue(treeId, out var mode) ? mode : null;
        }

        hasEnrollmentSource = false;
        return null;
    }

    /// <summary>
    /// Reports whether the receiver admits <paramref name="entry"/>: its tree is enrolled here and
    /// its peer-supplied wire mode equals the locally resolved mode. Fails closed - no enrollment
    /// source, a non-enrolled tree and a mode mismatch are all not admitted. A single cached
    /// lookup with no allocation.
    /// </summary>
    /// <param name="replicationContext">The host's replication context, or <see langword="null"/>.</param>
    /// <param name="options">The replication options, read per tree.</param>
    /// <param name="entry">The inbound entry (a run's representative).</param>
    /// <returns><see langword="true"/> when the entry's run is admitted.</returns>
    public static bool IsAdmitted(
        ILatticeReplicationContext? replicationContext,
        IOptionsMonitor<LatticeReplicationOptions> options,
        in WalRecord entry)
    {
        var localMode = ResolveLocalMergeMode(replicationContext, options, entry.TreeId, out var hasEnrollmentSource);
        return hasEnrollmentSource && localMode is { } mode && entry.Mode == mode;
    }
}
