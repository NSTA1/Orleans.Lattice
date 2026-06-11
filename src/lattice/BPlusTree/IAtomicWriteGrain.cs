namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Saga coordinator for atomic multi-key writes. One grain activation
/// per in-flight batch, keyed by <c>{treeId}/{operationId}</c>. Applies each
/// write sequentially through <see cref="ILattice"/>, persists progress after
/// every step, and compensates already-committed keys when a step throws.
/// <para>
/// Compensation rewrites the pre-saga value (or tombstones the key when it was
/// absent before the saga) with a freshly-ticked <c>HybridLogicalClock</c>, so
/// LWW merge semantics guarantee the rollback wins over the partial write.
/// Crash recovery is reminder-driven: on reactivation the grain consults its
/// persisted <see cref="State.AtomicWriteState.Phase"/> and resumes.
/// </para>
/// <para>
/// Readers may observe a brief partial-visibility window between the first
/// and last committed write; callers needing strict isolation should layer
/// version-guarded reads (<see cref="ILattice.GetWithVersionAsync"/> +
/// <see cref="ILattice.SetIfVersionAsync"/>) on top.
/// </para>
/// </summary>
[Alias(TypeAliases.IAtomicWriteGrain)]
internal interface IAtomicWriteGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts (or resumes) the atomic write saga for <paramref name="entries"/>
    /// against the tree identified by <paramref name="treeId"/>. Returns when
    /// every entry has been committed. Throws the originating exception after
    /// successful compensation if any step fails mid-flight.
    /// </summary>
    /// <param name="treeId">Logical tree ID to write into.</param>
    /// <param name="entries">Key-value pairs to commit atomically. Must not contain duplicate keys.</param>
    Task ExecuteAsync(string treeId, List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Starts (or resumes) a <em>guarded</em> atomic write saga: the batch is
    /// committed all-or-nothing only if every key's pre-saga value satisfies
    /// <paramref name="predicate"/>. The predicate is evaluated once, during
    /// the prepare phase, against each key's captured pre-saga snapshot; a key
    /// with no live pre-saga value counts as a non-match. When any key fails
    /// the saga transitions to <see cref="AtomicWritePhase.PreconditionFailed"/>
    /// and commits nothing. Returns the terminal outcome rather than throwing
    /// on a precondition miss. Re-attach (same operationId) returns the
    /// memoized outcome without re-evaluating the predicate.
    /// </summary>
    /// <param name="treeId">Logical tree ID to write into.</param>
    /// <param name="entries">Key-value pairs to commit atomically. Must not contain duplicate keys.</param>
    /// <param name="predicate">Server-side predicate IR evaluated against each key's pre-saga value.</param>
    Task<AtomicWriteOutcome> ExecuteGuardedAsync(
        string treeId,
        List<KeyValuePair<string, byte[]>> entries,
        LatticePredicateNode predicate);

    /// <summary>
    /// Returns <c>true</c> when the saga has finished (either all writes
    /// committed or compensation completed) or has not been started.
    /// </summary>
    Task<bool> IsCompleteAsync();

    /// <summary>
    /// Prepare-and-pause entry point for a cross-tree atomic write. Runs the
    /// same prepare + execute staging as <see cref="ExecuteGuardedAsync"/>
    /// (capturing pre-saga values, evaluating <paramref name="predicate"/> if
    /// supplied, and staging every write into the leaf pending buckets hidden
    /// from readers), then - instead of recording the per-tree terminal
    /// decision - registers the per-tree registry to delegate this saga's txid
    /// to the coordinator identified by <paramref name="coordinatorKey"/> and
    /// <b>pauses</b>. The saga stays paused until the coordinator calls
    /// <see cref="FinalizeAsync"/>.
    /// <para>
    /// Returns the participant's vote:
    /// <see cref="CrossTreePrepareVote.Prepared"/> when staging succeeded,
    /// <see cref="CrossTreePrepareVote.PreconditionFailed"/> when the guard
    /// failed (nothing staged, sub-saga terminal), or
    /// <see cref="CrossTreePrepareVote.Failed"/> when staging hit a genuine
    /// error (sub-saga self-compensated and is terminal-failed).
    /// </para>
    /// </summary>
    /// <param name="treeId">Logical tree ID to write into.</param>
    /// <param name="entries">Key-value pairs to commit atomically. Must not contain duplicate keys.</param>
    /// <param name="predicate">Optional server-side guard IR evaluated against each key's pre-saga value.</param>
    /// <param name="coordinatorKey">Key of the <see cref="ILatticeCrossTreeTxGrain"/> that owns the global decision.</param>
    /// <param name="participants">
    /// The full, canonical (ordinal-sorted) participant tree-id set of the
    /// enclosing cross-tree atomic write. Persisted on the sub-saga and stamped
    /// onto every per-shard terminal record (<see cref="WalRecord.CrossTreeParticipants"/>)
    /// so the receiver-side cross-tree visibility barrier can scope its wait set.
    /// </param>
    Task<CrossTreePrepareVote> PrepareForCoordinatorAsync(
        string treeId,
        List<KeyValuePair<string, byte[]>> entries,
        LatticePredicateNode? predicate,
        string coordinatorKey,
        IReadOnlyList<string> participants);

    /// <summary>
    /// Finalizes a sub-saga previously paused by
    /// <see cref="PrepareForCoordinatorAsync"/>. When <paramref name="commit"/>
    /// is <c>true</c> the saga records the per-tree commit decision and fans out
    /// the per-leaf commit terminals (making the staged writes visible); when
    /// <c>false</c> it records the abort decision and fans out the abort
    /// terminals (dropping the staged buckets, restoring the pre-saga view).
    /// Idempotent - safe to call repeatedly after a coordinator crash. A no-op
    /// when the sub-saga is already terminal or was never prepared.
    /// </summary>
    /// <param name="commit"><c>true</c> to commit the staged writes; <c>false</c> to abort them.</param>
    Task FinalizeAsync(bool commit);
}
