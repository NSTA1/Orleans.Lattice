namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Fires a prompt tag-index reconcile for every tag index covering a subject
/// tree whose physical identity has just been swapped under its registry alias
/// (shadow-cutover restore, tree resize, reshard).
/// <para>
/// A tag index is maintained inline in a sibling index tree and is not affected
/// by write-ahead-log tail orphaning, but a restore reverts the subject tree's
/// contents without reprojecting the index, so the index keeps reflecting the
/// pre-restore state until the next scheduled reconcile sweep. This trigger
/// closes that window: it enumerates the registered index trees, and for each
/// index that covers the swapped tree runs a synchronous digest-gated sweep so
/// the index converges to the restored / rebuilt subject state immediately.
/// </para>
/// </summary>
internal interface ITagIndexReconcileTrigger
{
    /// <summary>
    /// Reconciles every tag index that covers <paramref name="subjectTreeId"/>.
    /// Best-effort: a failure to enumerate the index trees or to reconcile an
    /// individual index is logged and swallowed, since the recurring scheduled
    /// sweep remains the correctness backstop and the swap operation that fired
    /// the trigger must not fault on a reconcile hiccup.
    /// </summary>
    Task TriggerForTreeAsync(string subjectTreeId, CancellationToken cancellationToken = default);
}
