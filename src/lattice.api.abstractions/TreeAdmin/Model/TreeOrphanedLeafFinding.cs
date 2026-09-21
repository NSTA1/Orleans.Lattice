namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// One descent-unreachable leaf the orphaned-leaf audit or repair found, and what
/// was decided about it. The control-API mirror of the core orphaned-leaf finding
/// DTO.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeOrphanedLeafFinding)]
[Immutable]
public sealed record TreeOrphanedLeafFinding
{
    /// <summary>The physical shard whose sibling chain the leaf is spliced into.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>
    /// The leaf's grain key, rendered as a string so the control-API surface carries
    /// no dependency on Orleans' <c>GrainId</c>.
    /// </summary>
    [Id(1)] public required string LeafId { get; init; }

    /// <summary>The leaf's inclusive low key, or <c>null</c> when it holds none.</summary>
    [Id(2)] public string? LowKeyInclusive { get; init; }

    /// <summary>The leaf's exclusive high key, or <c>null</c> when it holds none.</summary>
    [Id(3)] public string? HighKeyExclusive { get; init; }

    /// <summary>How many keys the leaf holds.</summary>
    [Id(4)] public int KeyCount { get; init; }

    /// <summary>
    /// How many of those keys were shown to be readable by descent elsewhere in the
    /// tree, so unsplicing the leaf would not lose them.
    /// </summary>
    [Id(5)] public int VerifiedKeyCount { get; init; }

    /// <summary>What was decided about this leaf.</summary>
    [Id(6)] public TreeOrphanedLeafDisposition Disposition { get; init; }

    /// <summary>
    /// The first key that could not be verified, when the disposition is
    /// <see cref="TreeOrphanedLeafDisposition.RefusedUnverifiedKeys"/>; otherwise
    /// <c>null</c>.
    /// </summary>
    [Id(7)] public string? UnverifiedKey { get; init; }

    /// <summary>
    /// <see langword="true"/> when the leaf is orphaned but was deliberately left
    /// alone because unsplicing it could not be shown to be safe.
    /// </summary>
    public bool IsRefusal
        => Disposition is not TreeOrphanedLeafDisposition.Repaired
            and not TreeOrphanedLeafDisposition.Repairable;
}
