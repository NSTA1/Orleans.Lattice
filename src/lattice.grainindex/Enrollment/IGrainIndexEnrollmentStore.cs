namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// The per-grain bookkeeping the enrolment path keeps in the index-registry
/// system tree: the seen markers a backfill skips over, and the pending
/// projections the outbox drain converges.
/// </summary>
/// <remarks>
/// This is a separate abstraction from the per-index definition store, not a
/// wider one, because the two have different shapes and different lifetimes: a
/// definition is written once per index per silo start, and these are written
/// per grain per mutation. They share the tree and the key scheme, which is what
/// keeps a scan of one kind from ever seeing the other.
/// </remarks>
internal interface IGrainIndexEnrollmentStore
{
    /// <summary>
    /// Reads the enrolment record for one grain in one index, or <c>null</c>
    /// when the grain has never been enrolled.
    /// </summary>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The record, or <c>null</c> when the grain is not enrolled.</returns>
    Task<GrainIndexEnrollmentRecord?> ReadEnrollmentAsync(
        string indexName,
        string grainKey,
        CancellationToken cancellationToken);

    /// <summary>
    /// Durably records an index write before it is attempted, so a failure
    /// after this point is recoverable rather than invisible.
    /// </summary>
    /// <param name="pending">The outbox entry. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the entry is durable.</returns>
    Task WritePendingAsync(GrainIndexPendingProjection pending, CancellationToken cancellationToken);

    /// <summary>
    /// Records that a grain's index write landed: writes the seen marker with
    /// the confirmed projection and clears the outbox entry, in one
    /// all-or-nothing batch.
    /// </summary>
    /// <remarks>
    /// The two must move together. Clearing the outbox without recording the
    /// projection would leave the next diff working from a stale baseline, and
    /// recording the projection without clearing the outbox would have the drain
    /// re-apply a batch that has already landed.
    /// </remarks>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="projection">The projection the index now holds. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the batch commits.</returns>
    Task CompleteAsync(
        string indexName,
        string grainKey,
        GrainIndexProjection projection,
        CancellationToken cancellationToken);

    /// <summary>
    /// Removes a grain's enrolment entirely - both the seen marker and any
    /// outstanding outbox entry - for a grain that has left the index.
    /// </summary>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the batch commits.</returns>
    Task WithdrawAsync(string indexName, string grainKey, CancellationToken cancellationToken);

    /// <summary>
    /// Streams every outstanding outbox entry, across every index, in key
    /// order.
    /// </summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The outstanding entries.</returns>
    IAsyncEnumerable<GrainIndexPendingProjection> ScanPendingAsync(CancellationToken cancellationToken);
}
