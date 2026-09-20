namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// What the orphaned-leaf audit or repair decided about one descent-unreachable
/// leaf. The control-API mirror of the core orphaned-leaf disposition enum.
/// </summary>
/// <remarks>
/// Every value other than <see cref="Repaired"/> and <see cref="Repairable"/> is a
/// refusal: the leaf is orphaned, but unsplicing it could not be shown to be safe,
/// so nothing was changed.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeOrphanedLeafDisposition)]
public enum TreeOrphanedLeafDisposition
{
    /// <summary>The leaf was unspliced and its state cleared.</summary>
    Repaired = 0,

    /// <summary>
    /// The leaf would be unspliced by the repair verb. Reported by the audit,
    /// which changes nothing.
    /// </summary>
    Repairable = 1,

    /// <summary>
    /// At least one key the leaf holds was not shown to be readable by descent, so
    /// unsplicing it could lose data.
    /// </summary>
    RefusedUnverifiedKeys = 2,

    /// <summary>The leaf holds more keys than the verification budget allows.</summary>
    RefusedKeyCountExceeded = 3,

    /// <summary>The leaf or its shard is in a state that blocks structural change.</summary>
    RefusedBlockingState = 4,

    /// <summary>The sibling chain changed under the walk, so the verdict is stale.</summary>
    RefusedChainRace = 5,

    /// <summary>
    /// Descent routes to this leaf after all, contradicting the orphan finding.
    /// </summary>
    RefusedRoutingContradiction = 6,
}
