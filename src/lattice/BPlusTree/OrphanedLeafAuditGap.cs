namespace Orleans.Lattice;

/// <summary>
/// One region of a tree over which an orphaned-leaf pass could not establish a
/// verdict (issue 3301), and why.
/// <para>
/// Findings and gaps answer different questions. A finding says "there is an
/// orphan here"; a gap says "I could not tell whether there is an orphan
/// here". A report carrying gaps has NOT cleared the tree, however empty its
/// findings list is - see
/// <see cref="OrphanedLeafRepairReport.VerdictComplete"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrphanedLeafAuditGap)]
[Immutable]
public readonly record struct OrphanedLeafAuditGap
{
    /// <summary>The physical shard whose examination is incomplete.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>Why the pass could not establish a verdict here.</summary>
    [Id(1)] public OrphanedLeafAuditGapReason Reason { get; init; }

    /// <summary>
    /// The leaf the pass stopped on, rendered as a string, when the gap names
    /// one; otherwise <see langword="null"/>. For a truncated chain this is
    /// the leaf whose successor pointer is severed, which is where an operator
    /// should look.
    /// </summary>
    [Id(2)] public string? LeafId { get; init; }

    /// <summary>
    /// The key the pass would have continued from, when the gap has one;
    /// otherwise <see langword="null"/>. For a truncated chain this is the
    /// severed leaf's exclusive high bound - the start of the keyspace whose
    /// leaves were reached only by re-entry, or not at all.
    /// </summary>
    [Id(3)] public string? KeyHint { get; init; }
}
