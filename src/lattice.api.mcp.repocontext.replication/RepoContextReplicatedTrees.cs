using Orleans.Lattice.Api.Mcp.RepoContext;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Replication;

/// <summary>
/// The reserved repository-context tree to <see cref="LatticeMergeMode"/> enrolment
/// map that <see cref="RepoContextReplicationServiceCollectionExtensions.EnableRepoContextMultiCluster(Orleans.Hosting.ISiloBuilder, System.Action{Orleans.Lattice.Replication.LatticeReplicationOptions}, bool)"/>
/// merges into <c>LatticeReplicationOptions.ReplicatedTrees</c>. It encodes the one
/// piece of knowledge an operator must not get wrong: which repository-context tree
/// replicates under which convergence rule.
/// <para>
/// Every repository-context tree in <see cref="RepoContextTrees.All"/> is enrolled.
/// The merge mode of each is fixed by <b>how the store authors that tree's values</b>,
/// not by taste:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     <see cref="RepoContextTrees.VectorMembership"/> is authored as an
///     <see cref="Orleans.Lattice.OrFlag"/> (one add-wins presence bit per embedded
///     source), so it enrols under <see cref="LatticeMergeMode.OrFlag"/>. This is the
///     load-bearing choice: a source embedded on one cluster and pruned on another
///     must converge <i>add-wins</i> by CRDT merge, never delete-wins - otherwise
///     active-active convergence silently drops the embedding and degrades retrieval
///     to keyword mode. The helper <b>force-pins</b> this mode even over a host that
///     mis-declared it.
///     </description>
///   </item>
///   <item>
///     <description>
///     <see cref="RepoContextTrees.Memory"/> is authored as an
///     <see cref="Orleans.Lattice.LatticeMergeMode.MvRegister"/> (a multi-value
///     register whose concurrent values are serialized memory records), so it enrols
///     under <see cref="LatticeMergeMode.MvRegister"/>. This is the second
///     load-bearing choice: two clusters writing the same memory key concurrently
///     must both survive and fold back through the record model's CRDT merge, never
///     last-writer-wins, otherwise active-active convergence silently drops one
///     write (and its CRDT sub-state) entirely. The helper <b>force-pins</b> this
///     mode even over a host that mis-declared it.
///     </description>
///   </item>
///   <item>
///     <description>
///     Every other tree - the structural and symbol stores of record, the
///     rebuildable content and cross-reference projections, the per-session reuse
///     bookkeeping, and the vector payload and metadata projections - is authored as a
///     plain last-writer-wins value through <c>ILattice.SetAsync</c>, so each enrols
///     under <see cref="LatticeMergeMode.LwwRegister"/> by default. The helper lets a
///     host override any of these (they are per-key LWW or immutable and
///     content-addressed, so a deployment with a single authoritative writer per key
///     may pick a different mode), but never membership or memory.
///     </description>
///   </item>
/// </list>
/// <para>
/// The map is deliberately explicit rather than a blanket "everything in
/// <see cref="RepoContextTrees.All"/> is last-writer-wins": enrolling a future
/// CRDT-authored tree under LWW would reintroduce exactly the silent-loss bug the
/// membership pin exists to prevent. <c>RepoContextReplicatedTreesTests</c> asserts
/// this map's keys equal <see cref="RepoContextTrees.All"/>, so adding a tree to the
/// layout without giving it a deliberate replication mode fails the build.
/// </para>
/// </summary>
internal static class RepoContextReplicatedTrees
{
    /// <summary>
    /// The convergence rule the <see cref="RepoContextTrees.VectorMembership"/>
    /// presence tree is pinned to. Add-wins so a concurrent re-embed on one cluster
    /// survives a prune on another instead of being lost delete-wins.
    /// </summary>
    internal const LatticeMergeMode MembershipMode = LatticeMergeMode.OrFlag;

    /// <summary>
    /// The convergence rule the <see cref="RepoContextTrees.Memory"/> agent-memory
    /// tree is pinned to. Multi-value so two clusters' concurrent writes to the same
    /// memory key both survive (each mints its own dot) and fold back through the
    /// record model's own CRDT merge, instead of one whole record - and its CRDT
    /// sub-state - being lost delete-wins the way a last-writer-wins register would.
    /// </summary>
    internal const LatticeMergeMode MemoryMode = LatticeMergeMode.MvRegister;

    /// <summary>
    /// The default convergence rule for every non-membership repository-context tree.
    /// These trees are authored as whole last-writer-wins values, so last-writer-wins
    /// is the only mode consistent with how they are written; a host may still override
    /// an individual tree.
    /// </summary>
    internal const LatticeMergeMode DefaultMode = LatticeMergeMode.LwwRegister;

    /// <summary>
    /// Builds the reserved repository-context enrolment map: every tree in
    /// <see cref="RepoContextTrees.All"/> paired with the
    /// <see cref="LatticeMergeMode"/> that matches how the store authors it.
    /// </summary>
    /// <returns>
    /// An ordinal-keyed map of repository-context tree id to its replication merge mode.
    /// </returns>
    internal static IReadOnlyDictionary<string, LatticeMergeMode> BuildEnrolmentMap()
    {
        return new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            [RepoContextTrees.Structural] = DefaultMode,
            [RepoContextTrees.Symbol] = DefaultMode,
            [RepoContextTrees.Memory] = MemoryMode,
            [RepoContextTrees.Content] = DefaultMode,
            [RepoContextTrees.CrossReference] = DefaultMode,
            [RepoContextTrees.Session] = DefaultMode,
            [RepoContextTrees.VectorPayload] = DefaultMode,
            [RepoContextTrees.VectorMetadata] = DefaultMode,
            [RepoContextTrees.VectorMembership] = MembershipMode,
        };
    }
}
