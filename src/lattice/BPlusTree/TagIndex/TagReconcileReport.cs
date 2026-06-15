namespace Orleans.Lattice;

/// <summary>
/// Idempotent outcome of an on-demand tag-index reconcile pass
/// (<see cref="ILatticeTagIndex.ReconcileAsync(string?, string?, System.Threading.CancellationToken)"/>),
/// reporting how much of the keyspace was inspected and how many orphaned
/// membership rows were repaired.
/// </summary>
/// <param name="TreesCovered">The number of subject trees inspected (always 1 for a single-tree reconcile).</param>
/// <param name="KeysScanned">The number of live primary-tree keys scanned in the requested range.</param>
/// <param name="MembershipRowsScanned">The number of in-range membership rows examined.</param>
/// <param name="OrphanRowsRemoved">The number of membership rows deleted because their key no longer exists in the primary tree.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.TagReconcileReport)]
public readonly record struct TagReconcileReport(
    [property: Id(0)] int TreesCovered,
    [property: Id(1)] int KeysScanned,
    [property: Id(2)] int MembershipRowsScanned,
    [property: Id(3)] int OrphanRowsRemoved)
{
    /// <summary>An empty report (all counters zero).</summary>
    public static TagReconcileReport Empty => new(0, 0, 0, 0);

    /// <summary>
    /// Returns a report whose counters are the element-wise sum of this report
    /// and <paramref name="other"/>. Used to aggregate per-tree reconcile
    /// passes into a single multi-tree report.
    /// </summary>
    /// <param name="other">The report to add.</param>
    public TagReconcileReport Combine(TagReconcileReport other) => new(
        TreesCovered + other.TreesCovered,
        KeysScanned + other.KeysScanned,
        MembershipRowsScanned + other.MembershipRowsScanned,
        OrphanRowsRemoved + other.OrphanRowsRemoved);
}
