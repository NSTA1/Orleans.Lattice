namespace Orleans.Lattice;

/// <summary>
/// One leaf the orphaned-leaf repair pass found spliced into a shard's sibling
/// chain while being unreachable by descent from the shard root, and what the
/// pass decided about it.
/// <para>
/// An orphan is not a cosmetic defect. It holds a WAL materialiser pin that
/// can never advance, because nothing routes writes to it, so it never
/// checkpoints; that pin is still counted in the trim floor, so the WAL never
/// trims and - compaction being strictly downstream of trim - never compacts
/// either. One orphan removes every bound on a tree's WAL growth,
/// permanently. See issue 3265 for the split seam that mints one and issue
/// 3269 for this repair path.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrphanedLeafFinding)]
[Immutable]
public readonly record struct OrphanedLeafFinding
{
    /// <summary>The physical shard the orphan was found in.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>
    /// The orphan's grain id, rendered as a string so the finding carries no
    /// dependency on Orleans' <c>GrainId</c> in a public result type.
    /// </summary>
    [Id(1)] public string LeafId { get; init; }

    /// <summary>The low bound the orphan declares, inclusive.</summary>
    [Id(2)] public string? LowKeyInclusive { get; init; }

    /// <summary>The high bound the orphan declares, exclusive.</summary>
    [Id(3)] public string? HighKeyExclusive { get; init; }

    /// <summary>
    /// How many live keys the orphan materialised. Non-zero is the ordinary
    /// case rather than an alarming one: rows materialise at activation by
    /// replaying the WAL through a predicate keyed on
    /// <c>(ShardIndex, LowKeyInclusive, HighKeyExclusive)</c> and never on leaf
    /// identity, so an orphan sharing bounds with a live leaf materialises a
    /// full shadow copy of its range.
    /// </summary>
    [Id(4)] public int KeyCount { get; init; }

    /// <summary>
    /// Length of the verified prefix before the first missing key or routing
    /// contradiction, not a census. By default later keys are untested.
    /// Survey mode preserves this meaning; use its separate counts below.
    /// </summary>
    [Id(5)] public int VerifiedKeyCount { get; init; }

    /// <summary>What the pass decided. See <see cref="OrphanedLeafDisposition"/>.</summary>
    [Id(6)] public OrphanedLeafDisposition Disposition { get; init; }

    /// <summary>
    /// The first key that could not be proven duplicated, when
    /// <see cref="Disposition"/> is
    /// <see cref="OrphanedLeafDisposition.RefusedUnverifiedKeys"/> or
    /// <see cref="OrphanedLeafDisposition.RefusedRoutingContradiction"/>;
    /// otherwise <see langword="null"/>. Named so an operator can go and look
    /// at the row rather than being told only that something failed.
    /// </summary>
    [Id(7)] public string? UnverifiedKey { get; init; }

    /// <summary>All keys readable elsewhere in a completed survey; null when not surveyed.</summary>
    [Id(8)] public int? SurveyVerifiedKeyCount { get; init; }

    /// <summary>All keys absent from their routed owner in a completed survey; null when not surveyed. This is not proof of data loss.</summary>
    [Id(9)] public int? SurveyMissingKeyCount { get; init; }

    /// <summary>All keys routing back to this supposedly unreachable leaf in a completed survey; null when not surveyed.</summary>
    [Id(10)] public int? SurveyRoutingContradictionKeyCount { get; init; }

    /// <summary>
    /// Whether this finding describes a refusal - anything other than
    /// <see cref="OrphanedLeafDisposition.Repaired"/> and
    /// <see cref="OrphanedLeafDisposition.Repairable"/>.
    /// </summary>
    public bool IsRefusal
        => Disposition is not (OrphanedLeafDisposition.Repaired or OrphanedLeafDisposition.Repairable);
}
