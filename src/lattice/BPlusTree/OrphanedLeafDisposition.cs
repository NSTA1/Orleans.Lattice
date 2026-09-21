namespace Orleans.Lattice;

/// <summary>
/// What the orphaned-leaf repair pass decided about one leaf it found in the
/// sibling chain but could not reach by descent from the shard root.
/// <para>
/// Every value other than <see cref="Repaired"/> and <see cref="Repairable"/>
/// is a <b>refusal</b>. The pass fails closed: a leaf it does not fully
/// understand is left exactly as it was found and reported, never unspliced on
/// a partial proof. An operator reading a refusal is being told that repair
/// needs a human, not that repair failed transiently.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrphanedLeafDisposition)]
public enum OrphanedLeafDisposition
{
    /// <summary>
    /// The leaf was unspliced from the sibling chain and its state - including
    /// its WAL materialiser pins - was cleared. This value is only ever
    /// reported by a repair run, never by an inspection.
    /// </summary>
    Repaired = 0,

    /// <summary>
    /// Every safety check passed and the leaf would have been unspliced, but
    /// this was an inspection rather than a repair. Reported by
    /// <see cref="ILattice.InspectOrphanedLeavesAsync"/>.
    /// </summary>
    Repairable = 1,

    /// <summary>
    /// REFUSAL. At least one key the orphan holds is not readable from the
    /// descent-reachable leaf that owns its range, so unsplicing it could
    /// discard the only surviving copy of a row.
    /// <para>
    /// This is the load-bearing safety check. The one orphan pair ever
    /// measured held keys that <em>were</em> duplicated on live leaves, but
    /// that is a sample of one and nothing about the mechanism that mints an
    /// orphan guarantees it, so duplication is proven per key rather than
    /// assumed.
    /// </para>
    /// </summary>
    RefusedUnverifiedKeys = 2,

    /// <summary>
    /// REFUSAL. The orphan holds more keys than one pass will verify. A leaf
    /// holds at most one leaf's worth of rows, so an orphan above that bound
    /// is itself evidence of a topology this pass does not model, and
    /// verifying a prefix of its keys would prove nothing about the rest.
    /// </summary>
    RefusedKeyCountExceeded = 3,

    /// <summary>
    /// REFUSAL. The leaf carries split, seal, or prepared-transaction state
    /// that could still resurrect rows, or a mutation was in flight on it. The
    /// second is itself surprising - nothing should route a write to a leaf no
    /// descent reaches - and is reported rather than reasoned past.
    /// </summary>
    RefusedBlockingState = 4,

    /// <summary>
    /// REFUSAL. The chain moved underneath the pass: the predecessor no longer
    /// pointed at this leaf when the unsplice was attempted, so a split landed
    /// during the repair. Nothing was mutated, and a later pass sees a settled
    /// topology.
    /// </summary>
    RefusedChainRace = 5,

    /// <summary>
    /// REFUSAL. The leaf looked unreachable when probed on its own low bound,
    /// but a key it holds routed back to it. The two observations contradict
    /// each other, so the pass declines and reports rather than choosing one.
    /// </summary>
    RefusedRoutingContradiction = 6,
}
