namespace Orleans.Lattice.Replication;

/// <summary>
/// Producer-side tracker for in-flight atomic-batch sagas. The
/// replication-side commit-time observer
/// (<see cref="ReplicationMutationObserver"/>) reports every
/// emit carrying a non-zero <see cref="ReplogEntry.AtomicBatchSize"/>
/// to this seam; the snapshot provider consults it before reading
/// the producer's tree state so the bootstrap snapshot does not
/// split an atomic batch across the snapshot / incremental boundary.
/// <para>
/// A saga is considered <i>in flight</i> from the replication-side
/// perspective when at least one of its emits has been observed but
/// fewer than <see cref="ReplogEntry.AtomicBatchSize"/> have been
/// observed in total. Because the
/// <see cref="ReplicationMutationObserver"/> fires synchronously
/// inside the grain's write path, this observation count is a
/// faithful proxy for "the producer's tree state has the first
/// <c>k</c> keys of the saga visible and the remaining
/// <c>N-k</c> keys not yet committed". Snapshotting against an
/// in-flight saga risks taking the partial-set view; the snapshot
/// provider therefore quiesces (waits for in-flight sagas to
/// finish emitting up to
/// <see cref="LatticeReplicationOptions.SnapshotSagaQuiesceTimeout"/>),
/// then captures any still-in-flight transaction ids as the
/// snapshot's <see cref="SnapshotStream.SagaBlacklist"/>.
/// </para>
/// <para>
/// Hosts can register a custom implementation via DI before
/// calling
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>;
/// the default
/// <see cref="InMemoryInFlightSagaTracker"/> is registered via
/// <c>TryAddSingleton</c>.
/// </para>
/// </summary>
public interface IInFlightSagaTracker
{
    /// <summary>
    /// Reports a single atomic-batch entry emit for the supplied
    /// tree. The implementation tracks the total observation count
    /// per <c>(treeName, transactionId)</c> tuple; once the count
    /// reaches <paramref name="batchSize"/> the transaction is no
    /// longer considered in flight.
    /// <para>
    /// Calls for non-atomic entries (entries with
    /// <see cref="ReplogEntry.AtomicBatchSize"/> &lt;= 0) must be
    /// suppressed at the call site; this method assumes a positive
    /// batch size.
    /// </para>
    /// <para>
    /// <b>Call-site contract:</b> the
    /// <see cref="ReplicationMutationObserver"/> invokes this seam
    /// <i>before</i> any per-emit short-circuit (mode resolver,
    /// per-key filter, sink write). The tracker's count is
    /// therefore a proxy for "the producer's tree state has
    /// committed this many of the saga's keys", not "this many of
    /// the saga's keys reached the WAL". The two are equivalent in
    /// the steady-state replicated case but diverge when individual
    /// siblings are filter-rejected or mode-skipped — the tracker
    /// must observe every committed sibling so the in-flight count
    /// reflects the producer's tree state, not the WAL projection.
    /// Maintenance-category mutations are excluded at the call
    /// site (they are structural rewrites, not user-authored
    /// causal events).
    /// </para>
    /// </summary>
    /// <param name="treeName">The replicated tree's id. Must be non-null and non-empty.</param>
    /// <param name="transactionId">
    /// The producer-side <see cref="ReplogEntry.TransactionId"/>;
    /// must be non-empty for atomic-batch tracking.
    /// </param>
    /// <param name="batchSize">
    /// The total entry count of the enclosing atomic transaction.
    /// Must be strictly positive; the tracker advances the
    /// transaction to <i>complete</i> once it has observed this
    /// many distinct emits for the same id.
    /// </param>
    void ObserveEmission(string treeName, Guid transactionId, int batchSize);

    /// <summary>
    /// Returns the set of atomic-batch transaction ids currently in
    /// flight for the supplied tree — i.e. those whose observed
    /// emit count is strictly between zero and the declared batch
    /// size. Order is unspecified.
    /// </summary>
    IReadOnlyList<Guid> GetInFlightTransactions(string treeName);

    /// <summary>
    /// Returns <c>true</c> when at least one transaction id in
    /// <paramref name="candidates"/> is currently in flight for the
    /// supplied tree. Allocation-free fast path consumed by
    /// <see cref="LatticeSnapshotProvider"/>'s quiesce-poll loop so
    /// the per-tick membership check does not allocate a fresh
    /// <see cref="List{T}"/> via
    /// <see cref="GetInFlightTransactions(string)"/> on every poll.
    /// <para>
    /// The default implementation falls back to
    /// <see cref="GetInFlightTransactions(string)"/> + a linear
    /// overlap scan so non-overriding implementers remain
    /// source-compatible. Implementations that maintain an indexed
    /// in-flight set should override this to a single-pass hash
    /// probe.
    /// </para>
    /// </summary>
    /// <param name="treeName">The replicated tree's id. Must be non-null and non-empty.</param>
    /// <param name="candidates">
    /// The candidate transaction id set to test for in-flight overlap.
    /// Must be non-null; an empty collection returns <c>false</c>.
    /// </param>
    bool AnyInFlight(string treeName, IReadOnlyCollection<Guid> candidates)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentNullException.ThrowIfNull(candidates);

        if (candidates.Count == 0)
        {
            return false;
        }

        var inFlight = GetInFlightTransactions(treeName);
        if (inFlight.Count == 0)
        {
            return false;
        }

        // O(N*M) fallback. Override for an indexed implementation;
        // the concrete InMemoryInFlightSagaTracker does so.
        if (candidates is HashSet<Guid> hash)
        {
            for (var i = 0; i < inFlight.Count; i++)
            {
                if (hash.Contains(inFlight[i]))
                {
                    return true;
                }
            }
            return false;
        }

        for (var i = 0; i < inFlight.Count; i++)
        {
            foreach (var id in candidates)
            {
                if (id == inFlight[i])
                {
                    return true;
                }
            }
        }
        return false;
    }
}
