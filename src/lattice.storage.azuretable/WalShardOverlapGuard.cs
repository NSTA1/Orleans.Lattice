namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Per-shard bookkeeping that lets an
/// <see cref="AzureTableWalStorageProvider"/> honour the
/// <see cref="IWalStorageProvider"/> overlap rule without a storage
/// round-trip on the steady-state append path.
/// <para>
/// Every append lands in its own batch partition keyed by its start
/// offset, so two batches with different start offsets never collide in
/// storage: a batch overlapping an already-written one would be
/// accepted, and a read would then yield the shared offsets twice. A
/// re-append at the <i>same</i> start offset does collide (409) and is
/// resolved by the provider's idempotent-replay proof, so this guard
/// only polices overlap between batches whose start offsets differ.
/// </para>
/// <para>
/// The guard keeps an upper bound on the end offset of every batch that
/// may be written for the shard, together with the batches whose append
/// is in motion on this instance. An append starting above the bound
/// cannot overlap anything and is admitted with no I/O, which is the
/// case for every append the WAL grain issues in allocation order. Any
/// other append is checked against the in-motion batches here and
/// against storage by the provider. The bound is only trusted while
/// this instance is the shard's single writer, which the WAL grain's
/// single activation guarantees; reconciliation re-establishes it.
/// </para>
/// </summary>
internal sealed class WalShardOverlapGuard
{
    private readonly object _gate = new();
    private readonly List<(long Start, long EndInclusive)> _inMotion = new();
    private bool _bounded;
    private long _writtenUpperBound = -1L;

    /// <summary>
    /// Outcome of <see cref="Claim"/>.
    /// </summary>
    internal enum ClaimOutcome
    {
        /// <summary>The batch starts above every batch that may be written; no storage check is needed.</summary>
        AboveWritten,

        /// <summary>The batch does not overlap any in-motion batch, but may overlap a written one; storage must be checked.</summary>
        NeedsStorageCheck,

        /// <summary>The batch overlaps an in-motion batch that starts at a different offset.</summary>
        Conflict,
    }

    /// <summary>
    /// True once an upper bound on the shard's written end offsets is known.
    /// </summary>
    internal bool IsBounded
    {
        get
        {
            lock (_gate)
            {
                return _bounded;
            }
        }
    }

    /// <summary>
    /// Synchronous fast path: claims <c>[start, endInclusive]</c> when it
    /// starts above every batch that may be written, and returns
    /// <see langword="false"/> without claiming anything otherwise.
    /// </summary>
    internal bool TryClaimAboveWritten(long start, long endInclusive)
    {
        lock (_gate)
        {
            if (!_bounded || start <= _writtenUpperBound)
            {
                return false;
            }

            _inMotion.Add((start, endInclusive));
            _writtenUpperBound = endInclusive;
            return true;
        }
    }

    /// <summary>
    /// Claims <c>[start, endInclusive]</c> unless it overlaps an
    /// in-motion batch that starts at a different offset. Anything but
    /// <see cref="ClaimOutcome.Conflict"/> leaves the batch claimed, and
    /// the caller must <see cref="Release"/> it.
    /// </summary>
    internal ClaimOutcome Claim(long start, long endInclusive, out long conflictingStart)
    {
        lock (_gate)
        {
            conflictingStart = -1L;
            if (_bounded && start > _writtenUpperBound)
            {
                _inMotion.Add((start, endInclusive));
                _writtenUpperBound = endInclusive;
                return ClaimOutcome.AboveWritten;
            }

            foreach (var (otherStart, otherEnd) in _inMotion)
            {
                if (otherStart != start && otherStart <= endInclusive && otherEnd >= start)
                {
                    conflictingStart = otherStart;
                    return ClaimOutcome.Conflict;
                }
            }

            _inMotion.Add((start, endInclusive));
            if (endInclusive > _writtenUpperBound)
            {
                _writtenUpperBound = endInclusive;
            }

            return ClaimOutcome.NeedsStorageCheck;
        }
    }

    /// <summary>
    /// Releases a claim once the append's entry rows have landed or the
    /// append has failed. The upper bound is left raised: a failed append
    /// can still have written rows, so keeping it only routes a later
    /// append over the same offsets through the storage check.
    /// </summary>
    internal void Release(long start, long endInclusive)
    {
        lock (_gate)
        {
            var index = _inMotion.IndexOf((start, endInclusive));
            if (index >= 0)
            {
                _inMotion.RemoveAt(index);
            }
        }
    }

    /// <summary>
    /// Records an upper bound read from storage. Never lowers the bound,
    /// so a stale read can only make the guard more conservative.
    /// </summary>
    internal void RaiseBound(long writtenUpperBound)
    {
        lock (_gate)
        {
            if (writtenUpperBound > _writtenUpperBound)
            {
                _writtenUpperBound = writtenUpperBound;
            }

            _bounded = true;
        }
    }

    /// <summary>
    /// Replaces the bound after reconciliation has established that no
    /// batch is written above <paramref name="writtenUpperBound"/>, or
    /// forgets it when <paramref name="writtenUpperBound"/> is
    /// <see langword="null"/> so the next append reads it from storage.
    /// Batches still in motion keep the bound at or above their end.
    /// </summary>
    internal void Reset(long? writtenUpperBound)
    {
        lock (_gate)
        {
            var bound = writtenUpperBound ?? -1L;
            foreach (var (_, endInclusive) in _inMotion)
            {
                if (endInclusive > bound)
                {
                    bound = endInclusive;
                }
            }

            _writtenUpperBound = bound;
            _bounded = writtenUpperBound.HasValue;
        }
    }
}
