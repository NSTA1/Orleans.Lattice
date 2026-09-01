namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// A durable outbox entry: the index write one grain intends to perform, or has
/// attempted without confirming, recorded in the index-registry system tree
/// before the write is issued.
/// </summary>
/// <remarks>
/// <para>
/// The entry exists to close the window in which a grain's own state commit
/// succeeds and the index write that should follow it does not - because the
/// tree rejected it, or because the silo stopped in between. Without a durable
/// record of the intent, that failure is invisible: the grain's state says one
/// thing, the index says another, and nothing in the system knows they disagree
/// until a full backfill sweeps the grain again. With one, the drain retries the
/// exact same batch, under the exact same idempotency key, until it lands.
/// </para>
/// <para>
/// It carries the whole plan rather than a "this grain is dirty" flag on
/// purpose. A flag would oblige the retry to re-read the grain's state, which
/// means activating it - so a fault that took the index down would be repaired
/// by waking every affected grain. The plan is self-contained, so the drain is a
/// tree-to-tree operation that never touches a grain.
/// </para>
/// <para>
/// One entry per grain per index. A grain that writes again before its previous
/// write is confirmed replaces its own entry, and because every plan is computed
/// against the last <i>confirmed</i> projection rather than the last attempted
/// one, the replacement subsumes what it replaces.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexPendingProjection)]
internal sealed class GrainIndexPendingProjection
{
    /// <summary>Initialises an outbox entry.</summary>
    /// <param name="indexName">The index the write belongs to. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded key of the grain being projected. Must not be <c>null</c>.</param>
    /// <param name="operationId">The atomic batch's idempotency key. Must not be <c>null</c>.</param>
    /// <param name="plan">The batch to apply. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexPendingProjection(
        string indexName,
        string grainKey,
        string operationId,
        GrainIndexUpdatePlan plan)
    {
        ArgumentNullException.ThrowIfNull(indexName);
        ArgumentNullException.ThrowIfNull(grainKey);
        ArgumentNullException.ThrowIfNull(operationId);
        ArgumentNullException.ThrowIfNull(plan);
        IndexName = indexName;
        GrainKey = grainKey;
        OperationId = operationId;
        Plan = plan;
    }

    /// <summary>The index whose tree the plan applies to.</summary>
    [Id(0)] public string IndexName { get; }

    /// <summary>The encoded grain key the plan projects.</summary>
    [Id(1)] public string GrainKey { get; }

    /// <summary>
    /// The idempotency key of the atomic batch. It is generated once, when the
    /// entry is written, and reused by every retry so a batch that actually
    /// committed before the caller learned of it re-attaches instead of running
    /// a second time.
    /// </summary>
    [Id(2)] public string OperationId { get; }

    /// <summary>The upserts, tombstones, and resulting projection to apply.</summary>
    [Id(3)] public GrainIndexUpdatePlan Plan { get; }
}
