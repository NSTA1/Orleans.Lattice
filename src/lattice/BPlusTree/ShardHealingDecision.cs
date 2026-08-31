namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The outcome of one over-split healing sweep for a tree: either a
/// consolidation was admitted, or the single clause that refused it.
/// <para>
/// Every refusal is reported distinctly rather than collapsed into a bare
/// "no", because the whole point of an automatic healer is that an operator
/// can tell "this tree does not need healing" from "this tree needs healing
/// and something is holding it back". The value is published per sweep on
/// <c>orleans.lattice.shard.healing.decisions</c> and returned by
/// <see cref="IShardHealingOrchestratorGrain.GetHealingReportAsync"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardHealingDecision)]
internal enum ShardHealingDecision
{
    /// <summary>
    /// No healing sweep has run yet for this tree. The deliberate default so
    /// an unobserved orchestrator never reports a decision it did not make.
    /// </summary>
    NotObserved = 0,

    /// <summary>A consolidation was admitted this sweep.</summary>
    Admitted = 1,

    /// <summary>
    /// Automatic healing is switched off by
    /// <see cref="LatticeOptions.ShardHealingEnabled"/> - the per-mechanism
    /// kill switch.
    /// </summary>
    Disabled = 2,

    /// <summary>
    /// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/> is zero,
    /// so no fold may be admitted. Distinct from
    /// <see cref="Disabled"/> because the observer still runs and still
    /// reports the tree's backlog; only admission is closed.
    /// </summary>
    AdmissionClosed = 3,

    /// <summary>
    /// The tree carries no more physical shards than its configured base, so
    /// there is nothing to heal. The steady state of a healthy tree.
    /// </summary>
    NotOverSplit = 4,

    /// <summary>
    /// The tree's load is concentrated rather than uniform, so consolidating
    /// would recreate the hot spot an adaptive split exists to relieve. The
    /// skew half of the hysteresis contract with the splitter.
    /// </summary>
    SkewedLoad = 5,

    /// <summary>
    /// An adaptive split is in flight on the tree. Folds serialise behind
    /// splits: the consolidation coordinator refuses a donor or survivor with
    /// a split in flight, so admitting here would only produce a fault.
    /// </summary>
    SplitInFlight = 6,

    /// <summary>
    /// A resize, reshard, merge, snapshot, or pending bulk graft is in flight
    /// on the tree. Those operations mutate topology in ways a fold cannot be
    /// safely interleaved with, so healing stands aside until they finish -
    /// the same suppression the split monitor applies for the same reason.
    /// </summary>
    TreeMaintenance = 7,

    /// <summary>
    /// The tree is inside the post-split anti-oscillation window
    /// (<see cref="LatticeOptions.ShardHealingCooldown"/>).
    /// </summary>
    Cooldown = 8,

    /// <summary>
    /// Foreground load is high enough that healing yields
    /// (<see cref="LatticeOptions.ShardHealingBackpressureOpsPerSecond"/>).
    /// </summary>
    Backpressure = 9,

    /// <summary>
    /// The tree already has
    /// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/> folds in
    /// flight. Healing is progressing; this sweep simply adds nothing.
    /// </summary>
    AtCapacity = 10,

    /// <summary>
    /// The tree is over-split and every gate is open, but no foldable pair
    /// could be selected this sweep - either the routing map has fewer than
    /// two physical shards left, or the cheapest adjacent pair overlaps a fold
    /// already in flight. Transient: the next sweep re-plans against the map
    /// the in-flight fold leaves behind.
    /// </summary>
    NoFoldablePair = 11,
}
