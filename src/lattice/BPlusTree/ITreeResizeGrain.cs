using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for resizing a tree by changing its <see cref="Orleans.Lattice.BPlusTree.ResolvedLatticeOptions.MaxLeafKeys"/>
/// and/or <see cref="Orleans.Lattice.BPlusTree.ResolvedLatticeOptions.MaxInternalChildren"/>. One activation exists per tree,
/// keyed by <c>{treeId}</c>
/// <para>
/// Resize uses an online snapshot to create a new physical tree with the desired sizing,
/// then swaps the tree alias so that reads and writes are redirected to the new tree.
/// The source tree remains fully available for reads and writes throughout; every
/// accepted mutation is shadow-forwarded to the destination so that no data is lost.
/// The old physical tree is soft-deleted and will be purged after the configured
/// <see cref="LatticeOptions.SoftDeleteDuration"/>. During the soft-delete window,
/// the resize can be undone with <see cref="UndoResizeAsync"/>.
/// </para>
/// </summary>
[Alias(TypeAliases.ITreeResizeGrain)]
internal interface ITreeResizeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a resize of the tree. An offline snapshot is taken to a new
    /// physical tree with the specified sizing. Once the snapshot completes,
    /// the tree alias is swapped and the old physical tree is soft-deleted.
    /// <para>
    /// Idempotent - if a resize is already in progress with the same parameters,
    /// the call is a no-op. If a resize is in progress with different parameters,
    /// an <see cref="InvalidOperationException"/> is thrown.
    /// </para>
    /// </summary>
    /// <param name="newMaxLeafKeys">The new maximum number of keys per leaf node. Must be greater than 1.</param>
    /// <param name="newMaxInternalChildren">The new maximum number of children per internal node. Must be greater than 2.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown if a resize is already in progress with different parameters.
    /// </exception>
    Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren);

    /// <summary>
    /// Runs the resize operation synchronously - processes all remaining phases
    /// in a single call. Intended for integration testing and manual triggers.
    /// Must be called after <see cref="ResizeAsync"/> to start the operation.
    /// </summary>
    Task RunResizePassAsync();

    /// <summary>
    /// Undoes the most recent resize synchronously by recovering the old physical
    /// tree, moving the logical tree back onto it together with its shard map,
    /// restoring the original registry configuration,
    /// and deleting the new snapshot tree. Available at every phase of the
    /// resize: before the alias swap the destination tree is simply discarded,
    /// and after it the old physical tree is restored - recovering it from
    /// soft-delete only when the Cleanup phase already deleted it, since in
    /// the earlier phases it is still live.
    /// <para>
    /// This is the run-to-completion trigger, intended for integration testing and
    /// manual use in the manner of <see cref="RunResizePassAsync"/>: it takes the
    /// coordinator's turn, so it queues behind an in-flight phase and holds its
    /// caller for the whole unwind. The public undo path uses
    /// <see cref="RequestUndoAsync"/> instead, which is admitted immediately
    /// (issue 3923).
    /// </para>
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no resize exists to undo, if the persisted resize state is
    /// incomplete, or if the old tree has already been purged.
    /// </exception>
    Task UndoResizeAsync();

    /// <summary>
    /// Accepts an undo of the most recent resize and returns without waiting for
    /// the unwind. Persists the intent in a slot separate from the coordinator's
    /// phase state and arms the phase loop, which observes it at its next tick or
    /// snapshot slice boundary and runs the same phase-aware unwind as
    /// <see cref="UndoResizeAsync"/>. Poll <see cref="GetUndoProgressAsync"/> to
    /// follow it.
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> so it is
    /// admitted while a resize phase holds the coordinator's turn - the moment an
    /// undo is most needed. It never mutates coordinator phase state. Idempotent:
    /// a request while an undo is already pending is acknowledged again.
    /// </para>
    /// </summary>
    /// <returns>The operation id of the resize being undone.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no resize exists to undo; the message names the most recently
    /// undone resize, when there is one, so a retry after a successful undo is not
    /// mistaken for a failure.
    /// </exception>
    [AlwaysInterleave]
    Task<string> RequestUndoAsync();

    /// <summary>
    /// Reports whether an accepted undo is still unwinding and, when an accepted
    /// undo was withdrawn because it could not be applied, which resize it named
    /// and why. Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>
    /// so it answers while a phase is in flight. A pure read.
    /// </summary>
    [AlwaysInterleave]
    Task<ResizeUndoProgress> GetUndoProgressAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator is idle - either no resize
    /// has ever been initiated, or the last one has run to completion.
    /// Returns <c>false</c> while a resize is in flight. An accepted undo of a
    /// resize that had already completed does not turn this back to
    /// <c>false</c>, so completion stays monotonic for a given resize; read
    /// <see cref="GetUndoProgressAsync"/> for the unwind. Marked
    /// <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> so a status
    /// read is not held behind an in-flight phase. A pure read.
    /// </summary>
    [AlwaysInterleave]
    Task<bool> IsIdleAsync();

    /// <summary>
    /// Reports whether this tree's resize still forbids the shard migrations that
    /// move virtual slots between shards (adaptive split, online consolidation;
    /// issue #4452): <see langword="true"/> while a resize is in flight, while an
    /// undo is pending or running, and - once a resize completed - for as long as
    /// any shard of the copy it replaced still mirrors into the resized copy,
    /// which it does through the soft-delete window until the purge (or an undo)
    /// clears its shadow-forward state. A migration on the resized copy in that
    /// window would change a layout the replaced copy's index-addressed mirror,
    /// and a saga bound to that copy, cannot follow.
    /// <para>
    /// Fails closed: an answer it cannot establish - a shard probe that throws or
    /// times out, a state change not yet persisted - is <see langword="true"/>.
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> so a
    /// migration's check is not held behind a resize phase or snapshot slice.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<bool> HoldsShardMigrationsAsync();

    /// <summary>
    /// Reports whether this tree's resize state still names
    /// <paramref name="physicalTreeId"/> - as the old physical tree an undo would
    /// recover, or as the destination an in-flight or completed resize built. A
    /// retired copy that is no longer named can never be recovered through this
    /// coordinator, which is what lets the WAL GC release the retention of an
    /// undone resize's destination deleted by a build that predates the discard
    /// (issue #3930). A pure read of in-memory state, interleaved so it never
    /// queues behind a snapshot pass.
    /// </summary>
    /// <param name="physicalTreeId">The physical tree id to look for.</param>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<bool> ReferencesPhysicalTreeAsync(string physicalTreeId);

    /// <summary>
    /// Reports how far the resize has durably got, in the work units
    /// <see cref="ResizeProgress"/> describes. During the
    /// <see cref="State.ResizePhase.Snapshot"/> phase the copied shards are read
    /// from the snapshot coordinator's own durable progress. Answers from the
    /// resize state as last persisted, like every interleaved read on this
    /// coordinator, so it never runs ahead of work a reactivated coordinator would
    /// resume from. A pure read.
    /// </summary>
    [AlwaysInterleave]
    Task<ResizeProgress> GetProgressAsync();
}
