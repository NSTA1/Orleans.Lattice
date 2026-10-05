namespace Orleans.Lattice;

/// <summary>
/// Notified by the tree registry <b>before</b> it persists a change to a tree's
/// content lineage (<c>TreeRegistryEntry.Lineage</c>, issue #4537): a new
/// registration, an unregistration, or a write that re-stamps the lineage (an
/// alias moved to a different tree, a removed alias, a shadow-cutover restore
/// or its revert, an explicit alias carry). A consumer that caches claims made
/// under the old lineage - the replication receiver's applied low watermark,
/// for example - drops them here, so a crash between this call and the write
/// can only leave it conservatively degraded, never trusting the old lineage
/// over the new contents.
/// <para>
/// Fail closed: an exception from an observer propagates and the registry does
/// not persist the change.
/// </para>
/// </summary>
internal interface ITreeLineageObserver
{
    /// <summary>
    /// Called before the registry persists a lineage change for
    /// <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The logical tree whose lineage is about to change.</param>
    /// <param name="currentLineage">The persisted lineage, or <see langword="null"/> when none (unregistered or legacy).</param>
    /// <param name="nextLineage">The lineage about to be persisted, or <see langword="null"/> when the tree is being unregistered.</param>
    /// <param name="cancellationToken">Cancels the notification.</param>
    Task OnLineageChangingAsync(string treeId, Guid? currentLineage, Guid? nextLineage, CancellationToken cancellationToken = default);
}
