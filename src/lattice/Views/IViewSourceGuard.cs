namespace Orleans.Lattice.Views;

/// <summary>
/// Resolves the materialised views that derive from a given source tree, so the
/// tree-deletion path can reject deleting a source that still has dependent views
/// (a SQL <c>RESTRICT</c>-style guard). Registered only when
/// <c>AddLatticeViews</c> is called; a host without views never resolves it, so
/// the base tree-deletion path is unaffected.
/// </summary>
internal interface IViewSourceGuard
{
    /// <summary>
    /// Returns the names of every materialised view whose source is
    /// <paramref name="sourceTreeId"/>, deduplicated and ordered. Empty when no
    /// view derives from the tree. The lookup is authoritative across the durable
    /// runtime-view registry, the startup-declared registrations, and the
    /// in-memory catalog, so a runtime view that has not yet activated on the
    /// calling silo - or whose durable record has not yet landed - is still seen.
    /// </summary>
    /// <param name="sourceTreeId">The source tree id to find dependent views for.</param>
    /// <param name="cancellationToken">A token to cancel the lookup.</param>
    Task<IReadOnlyList<string>> FindDependentViewsAsync(string sourceTreeId, CancellationToken cancellationToken = default);
}
