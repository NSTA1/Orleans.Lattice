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
/// and saga compensation rolls - emit with
/// <see cref="MutationCategory.User"/> (the default).
/// </para>
/// <para>
/// Library-internal mutation sites (resize / rebalance / compaction /
/// internal structural rewrite) emit with
/// <see cref="MutationCategory.Maintenance"/> by wrapping their work in
/// a <c>LatticeMaintenanceContext</c> scope. Replication observers skip
/// the WAL append for <see cref="MutationCategory.Maintenance"/> emits on
/// replicated trees so structural maintenance does not cross cluster
/// boundaries; downstream peers run the same maintenance independently.
/// </para>
/// <para>
/// This classification is independent of
/// <see cref="LatticeMutation.OriginClusterId"/> - a remote-origin
/// maintenance mutation would still be
/// <see cref="MutationCategory.Maintenance"/> and still skip the WAL.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.MutationCategory)]
public enum MutationCategory
{
    /// <summary>A user-driven write authored through the public <see cref="ILattice"/> surface.</summary>
    User = 0,

    /// <summary>A library-internal maintenance write produced by structural coordination (resize, rebalance, compaction, internal rewrite).</summary>
    Maintenance = 1,
}
