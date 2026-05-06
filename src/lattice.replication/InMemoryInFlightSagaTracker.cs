using System.Collections.Concurrent;
using System.Diagnostics;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default in-process <see cref="IInFlightSagaTracker"/>. Tracks
/// per-<c>(treeName, transactionId)</c> emit counts in a
/// <see cref="ConcurrentDictionary{TKey, TValue}"/> keyed by the
/// composite tuple, removing each row as soon as the observed
/// count reaches the declared batch size so the dictionary's
/// steady-state size is bounded by the number of <i>currently
/// in-flight</i> sagas across every tracked tree — typically zero
/// in a quiesced cluster, a small constant under sustained
/// atomic-batch load.
/// <para>
/// Tracker state is intentionally process-local: the data has no
/// durability requirement (a silo crash that loses the in-flight
/// tracker is equivalent to "no in-flight sagas observed since
/// last restart" — the snapshot provider just sees an empty
/// in-flight set and produces an empty blacklist, which is the
/// conservative outcome).
/// </para>
/// <para>
/// Defense-in-depth: each row carries a monotonic Stopwatch
/// timestamp of its last observation, and rows older than
/// <see cref="StaleEntryTimeout"/> are opportunistically pruned on
/// every <see cref="ObserveEmission"/> and
/// <see cref="GetInFlightTransactions"/> call. Without this guard
/// a producer that crashes mid-saga (or a misbehaving caller that
/// emits with a wrong <c>batchSize</c> on subsequent siblings)
/// would leak a row that never reaches completion, growing the
/// dictionary unboundedly under sustained partial-saga load. The
/// stale ceiling is generous (<c>10 minutes</c>) so a legitimate
/// long-running saga is never spuriously pruned.
/// </para>
/// </summary>
internal sealed class InMemoryInFlightSagaTracker : IInFlightSagaTracker
{
    /// <summary>
    /// Wall-clock ceiling above which a tracker row is considered
    /// stale and pruned opportunistically on the next observe / get
    /// call. Hard-coded rather than option-bound because the value
    /// only matters as a safety net — the snapshot provider's
    /// quiesce window is the load-bearing knob; this ceiling exists
    /// solely to guarantee bounded memory under producer-crash or
    /// caller-bug pathologies.
    /// </summary>
    internal static readonly TimeSpan StaleEntryTimeout = TimeSpan.FromMinutes(10);

    private static readonly long StaleEntryStopwatchTicks =
        (long)(StaleEntryTimeout.TotalSeconds * Stopwatch.Frequency);

    private readonly ConcurrentDictionary<TransactionKey, SagaEntry> _byTransaction = new();

    /// <inheritdoc />
    public void ObserveEmission(string treeName, Guid transactionId, int batchSize)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        if (transactionId == Guid.Empty)
        {
            throw new ArgumentException(
                "transactionId must be non-empty for atomic-batch tracking.",
                nameof(transactionId));
        }

        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(batchSize),
                batchSize,
                "Atomic-batch size must be strictly positive.");
        }

        var key = new TransactionKey(treeName, transactionId);
        var nowTicks = Stopwatch.GetTimestamp();

        // AddOrUpdate atomically: insert with count = 1 on first
        // observation; otherwise increment the existing count and
        // refresh the last-observed timestamp. The batch-size
        // component is stamped on first observation and preserved
        // across updates (the producer is required to emit every
        // sibling with the same declared batch size; a mismatched
        // sibling is a producer bug and is asserted in Debug builds
        // to fail fast in test runs).
        var updated = _byTransaction.AddOrUpdate(
            key,
            static (_, args) => new SagaEntry(args.BatchSize, 1, args.NowTicks),
            static (_, existing, args) =>
            {
                Debug.Assert(
                    existing.BatchSize == args.BatchSize,
                    $"Producer emitted atomic-batch sibling with batchSize={args.BatchSize} after declaring batchSize={existing.BatchSize}; "
                    + "every sibling of an atomic batch must carry the same declared batch size.");
                return existing with { Count = existing.Count + 1, LastObservedAtTicks = args.NowTicks };
            },
            (BatchSize: batchSize, NowTicks: nowTicks));

        // Removal condition: every sibling has been observed. Use
        // TryRemove with the explicit key/value pair so a
        // concurrent observation that incremented the count is not
        // silently dropped — the row simply remains in the
        // dictionary with a count at-or-above the batch size, which
        // the in-flight query treats as "complete".
        if (updated.Count >= updated.BatchSize)
        {
            _byTransaction.TryRemove(new KeyValuePair<TransactionKey, SagaEntry>(key, updated));
        }

        PruneStaleEntries(nowTicks);
    }

    /// <inheritdoc />
    public IReadOnlyList<Guid> GetInFlightTransactions(string treeName)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);

        if (_byTransaction.IsEmpty)
        {
            return Array.Empty<Guid>();
        }

        var nowTicks = Stopwatch.GetTimestamp();
        PruneStaleEntries(nowTicks);

        if (_byTransaction.IsEmpty)
        {
            return Array.Empty<Guid>();
        }

        List<Guid>? buffer = null;
        foreach (var kvp in _byTransaction)
        {
            if (!string.Equals(kvp.Key.TreeName, treeName, StringComparison.Ordinal))
            {
                continue;
            }

            if (kvp.Value.Count < kvp.Value.BatchSize)
            {
                buffer ??= new List<Guid>(capacity: 4);
                buffer.Add(kvp.Key.TransactionId);
            }
        }

        return (IReadOnlyList<Guid>?)buffer ?? Array.Empty<Guid>();
    }

    /// <inheritdoc />
    /// <remarks>
    /// O(N) override against the in-process tracker dictionary:
    /// performs a single dictionary scan and tests each candidate
    /// id with O(1) hashed lookup, instead of the base
    /// implementation's two-allocation
    /// (<see cref="GetInFlightTransactions"/> + <see cref="HashSet{T}"/>)
    /// fallback. Called by the snapshot provider on every poll-loop
    /// tick during quiesce wait so allocation-free is load-bearing.
    /// </remarks>
    public bool AnyInFlight(string treeName, IReadOnlyCollection<Guid> candidates)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentNullException.ThrowIfNull(candidates);

        if (candidates.Count == 0 || _byTransaction.IsEmpty)
        {
            return false;
        }

        // Adopt the caller's HashSet directly when possible (the
        // snapshot provider hands us a HashSet<Guid> built once
        // up-front), else build a small one for O(1) probing.
        var probe = candidates as HashSet<Guid> ?? new HashSet<Guid>(candidates);

        foreach (var kvp in _byTransaction)
        {
            if (!string.Equals(kvp.Key.TreeName, treeName, StringComparison.Ordinal))
            {
                continue;
            }

            if (kvp.Value.Count < kvp.Value.BatchSize && probe.Contains(kvp.Key.TransactionId))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Opportunistically removes rows whose last observation is
    /// older than <see cref="StaleEntryTimeout"/>. Called from
    /// <see cref="ObserveEmission"/> and
    /// <see cref="GetInFlightTransactions"/> so the dictionary
    /// cannot grow unboundedly when a producer crashes mid-saga or
    /// a caller bug stops emitting siblings before
    /// <c>BatchSize</c> is reached. Removal is conditional on the
    /// snapshot value (TryRemove with KeyValuePair) so a concurrent
    /// observation that just refreshed
    /// <see cref="SagaEntry.LastObservedAtTicks"/> is not silently
    /// dropped — the next prune-pass will re-evaluate.
    /// </summary>
    private void PruneStaleEntries(long nowTicks)
    {
        if (_byTransaction.IsEmpty)
        {
            return;
        }

        foreach (var kvp in _byTransaction)
        {
            if (nowTicks - kvp.Value.LastObservedAtTicks > StaleEntryStopwatchTicks)
            {
                _byTransaction.TryRemove(new KeyValuePair<TransactionKey, SagaEntry>(kvp.Key, kvp.Value));
            }
        }
    }

    /// <summary>
    /// Composite tracker key. Tree name is stamped on the row so a
    /// single dictionary holds tracker state for every replicated
    /// tree the silo observes.
    /// </summary>
    private readonly record struct TransactionKey(string TreeName, Guid TransactionId);

    /// <summary>
    /// Per-transaction tracker row. <see cref="BatchSize"/> is the
    /// declared <see cref="ReplogEntry.AtomicBatchSize"/> stamped on
    /// the first observation; <see cref="Count"/> is the running
    /// emit count; <see cref="LastObservedAtTicks"/> is the
    /// monotonic <see cref="Stopwatch.GetTimestamp"/> reading at the
    /// last observation, consulted by
    /// <see cref="PruneStaleEntries(long)"/> to bound dictionary
    /// growth under producer-crash pathologies.
    /// </summary>
    private readonly record struct SagaEntry(int BatchSize, int Count, long LastObservedAtTicks);
}
