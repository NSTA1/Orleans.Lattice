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
    /// Cross-cluster atomic-batch apply entry-point. Starts (or
    /// resumes) a saga against the tree identified by
    /// <paramref name="treeId"/> with each entry's source-side
    /// <c>(Timestamp, OriginClusterId, VectorClock, ExpiresAtTicks)</c>
    /// preserved verbatim onto the persisted
    /// <see cref="Primitives.LwwValue{T}"/>. Each per-key call is
    /// wrapped in nested
    /// <see cref="LatticeOriginContext.With(string?)"/> +
    /// <see cref="LatticeVectorClockContext.With(Primitives.VersionVector?)"/> +
    /// <see cref="LatticeHlcOverrideContext.With(Primitives.HybridLogicalClock?)"/>
    /// scopes so the leaf grain re-stamps the authoring cluster's
    /// metadata bit-identically — preserving the source-HLC-preservation
    /// invariant the per-entry apply seam already enforces. The saga is
    /// keyed by <c>{treeId}/{transactionId}</c>; resubmissions with the
    /// same id re-attach to the original saga and inherit its terminal
    /// outcome. Returns an <see cref="AtomicApplyResult"/> rather than
    /// throwing on saga failure so the receiver-side adapter can route
    /// batched and per-entry applies through a common outcome path.
    /// </summary>
    /// <param name="treeId">Logical tree ID to apply into.</param>
    /// <param name="applyEntries">
    /// The per-entry source metadata. Must not contain duplicate keys
    /// (mirrors the local saga's invariant). Empty batches return
    /// <see cref="AtomicApplyOutcome.Committed"/> with
    /// <c>AppliedCount == 0</c>.
    /// </param>
    /// <param name="originClusterId">
    /// The id of the remote cluster that authored the batch. Saga-wide
    /// because every entry in a producer's saga shares one origin.
    /// </param>
    Task<AtomicApplyResult> ExecuteApplyAsync(
        string treeId,
        List<AtomicApplyEntry> applyEntries,
        string originClusterId);

    /// <summary>
    /// Returns <c>true</c> when the saga has finished (either all writes
    /// committed or compensation completed) or has not been started.
    /// </summary>
    Task<bool> IsCompleteAsync();
}
