namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The single place a <see cref="GrainIndexUpdatePlan"/> is turned into tree
/// calls, so the foreground enrolment path and the background outbox drain
/// cannot drift apart in how they write.
/// </summary>
/// <remarks>
/// The choice between the plain and the mixed atomic overload is the load-bearing
/// part. A plan with tombstones must ride the mixed batch, because that is what
/// makes a moved value's old key vanish in the same visibility flip that makes
/// its new key appear; splitting it into a write and a delete would let a
/// concurrent scan see the grain at both values, or at neither.
/// <para>
/// Being the one write site also makes it the one honest place to count entries:
/// the index's entry-count instrument moves exactly when the tree does, on every
/// route, and cannot be double counted by a caller that also reports its own
/// bookkeeping.
/// </para>
/// </remarks>
internal static class GrainIndexPlanApplier
{
    /// <summary>
    /// Applies <paramref name="plan"/> to <paramref name="tree"/> as one
    /// all-or-nothing batch, or does nothing when the plan is empty.
    /// </summary>
    /// <param name="tree">The index's backing tree. Must not be <c>null</c>.</param>
    /// <param name="plan">The plan to apply. Must not be <c>null</c>.</param>
    /// <param name="indexTag">
    /// The index's pre-built telemetry tag, from
    /// <see cref="GrainIndexMetrics.IndexTag(string)"/>.
    /// </param>
    /// <param name="operationId">The batch's idempotency key. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels before the batch is submitted.</param>
    /// <returns>A task that completes when the batch commits, or immediately when the plan is empty.</returns>
    internal static Task ApplyAsync(
        ILattice tree,
        GrainIndexUpdatePlan plan,
        KeyValuePair<string, object?> indexTag,
        string operationId,
        CancellationToken cancellationToken)
    {
        if (plan.IsEmpty)
            return Task.CompletedTask;

        var write = plan.Deletes.Count == 0
            ? tree.SetManyAtomicAsync(plan.UpsertList, operationId, cancellationToken)
            : tree.SetManyAtomicAsync(plan.UpsertList, plan.Deletes, operationId, cancellationToken);

        // Awaiting is only worth an async state machine when there is something
        // to record. With the instrument disabled the batch's own task is
        // returned untouched, which is exactly what this method did before it
        // was instrumented.
        return GrainIndexMetrics.Entries.Enabled
            ? RecordAsync(write, plan.EntryDelta, indexTag)
            : write;
    }

    private static async Task RecordAsync(
        Task write,
        int entryDelta,
        KeyValuePair<string, object?> indexTag)
    {
        await write.ConfigureAwait(true);
        GrainIndexMetrics.RecordEntryDelta(indexTag, entryDelta);
    }
}
