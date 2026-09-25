namespace Orleans.Lattice;

/// <summary>
/// Classifies a <see cref="LatticeMutation"/> as either a user-driven write
/// or a library-internal maintenance write. Replication-aware observers
/// use the value to decide whether the mutation should be propagated to
/// peer clusters.
/// </summary>
/// <remarks>
/// <para>
/// User-driven writes - <c>SetAsync</c>, <c>DeleteAsync</c>,
/// <c>DeleteRangeAsync</c>, <c>SetIfVersionAsync</c>, <c>GetOrSetAsync</c>,
/// <c>SetManyAsync</c>, <c>SetManyAtomicAsync</c>, <c>BulkLoadAsync</c>,
/// and the compensating atomic write an atomic-action saga issues to
/// restore a completed step - emit with
/// <see cref="MutationCategory.User"/> (the default).
/// </para>
/// <para>
/// A mutation emitted inside a <c>LatticeMaintenanceContext</c> scope
/// carries <see cref="MutationCategory.Maintenance"/>. Today tombstone
/// compaction is the only producer: its reap envelopes are written inside
/// that scope. Structural rewrites (leaf splits, cross-shard migration, tree
/// merges) re-append entries with their original origin and timestamp under
/// the ambient category, which is <see cref="MutationCategory.User"/> outside
/// compaction.
/// </para>
/// <para>
/// The category is recorded on the WAL record. Compaction does not cross
/// cluster boundaries: the replication observer skips its ship-loop nudge
/// for a maintenance mutation, and the shipper drops tombstone-reap
/// envelopes by their mutation kind; every peer compacts its own copy
/// independently. The classification is independent of
/// <see cref="LatticeMutation.OriginClusterId"/>.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.MutationCategory)]
public enum MutationCategory
{
    /// <summary>A user-driven write authored through the public <see cref="ILattice"/> surface.</summary>
    User = 0,

    /// <summary>A library-internal maintenance write; today only tombstone compaction emits it.</summary>
    Maintenance = 1,
}
