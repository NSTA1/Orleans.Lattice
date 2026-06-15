namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// The phase of a single background tag-index reconciliation sweep driven by
/// <see cref="Grains.TagIndexReconcileGrain"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TagIndexReconcilePhase)]
internal enum TagIndexReconcilePhase
{
    /// <summary>
    /// No sweep is in flight. The grain is idle waiting for the next schedule
    /// reminder firing.
    /// </summary>
    Idle = 0,

    /// <summary>
    /// Comparing each covered tree's current digest fingerprint against the
    /// baseline stored from the last successful reconcile; trees whose
    /// fingerprint differs (or whose digest is unavailable) are collected for
    /// the repair phase.
    /// </summary>
    Probe = 1,

    /// <summary>
    /// Deep-scanning and repairing each tree the probe phase flagged divergent,
    /// via the on-demand reconcile path, then advancing that tree's baseline.
    /// </summary>
    Repair = 2,
}
