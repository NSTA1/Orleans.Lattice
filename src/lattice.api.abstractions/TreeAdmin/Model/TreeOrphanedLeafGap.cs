namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// One region of a tree over which an orphaned-leaf audit or repair could not
/// establish a verdict, and why (issue 3301). The control-API mirror of the core
/// orphaned-leaf audit gap DTO.
/// </summary>
/// <remarks>
/// Findings and gaps answer different questions. A finding says "there is an orphan
/// here"; a gap says "I could not tell whether there is an orphan here". A report
/// carrying gaps has not cleared the tree, however empty its findings list is - see
/// <see cref="TreeOrphanedLeafReport.VerdictComplete"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeOrphanedLeafGap)]
[Immutable]
public sealed record TreeOrphanedLeafGap
{
    /// <summary>The physical shard whose examination is incomplete.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>Why the pass could not establish a verdict here.</summary>
    [Id(1)] public TreeOrphanedLeafGapReason Reason { get; init; }

    /// <summary>
    /// The leaf the pass stopped on, when the gap names one. For a severed sibling
    /// chain this is the leaf whose successor pointer is broken, which is where an
    /// operator should look.
    /// </summary>
    [Id(2)] public string? LeafId { get; init; }

    /// <summary>
    /// The key the pass would have continued from, when the gap has one. For a
    /// severed sibling chain this is the start of the keyspace whose leaves were
    /// reached only by re-entry, or not at all.
    /// </summary>
    [Id(3)] public string? KeyHint { get; init; }
}
