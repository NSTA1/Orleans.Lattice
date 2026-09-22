using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Orleans.Lattice.Storage.File;

/// <summary>
/// Owns the durable state and in-memory index for a single
/// <c>(treeId, shardIndex)</c> write-ahead log. All public operations
/// serialise through a per-shard async gate so a batch append (buffer +
/// single write + fsync) is atomic with respect to concurrent reads,
/// trims, and compaction. The on-disk representation is a segmented
/// append-only log framed by <see cref="FileWalRecordFormat"/>; the
/// in-memory index maps each live offset to the file position and length
/// of its payload so reads seek directly to the bytes.
/// </summary>
internal sealed class FileWalShard : IDisposable
{
    private readonly record struct IndexEntry(long Offset, long Position, int PayloadLength);

    private readonly string _directory;
    private readonly string _logPath;
    private readonly string _treeId;
    private readonly int _shardIndex;
    private readonly FileWalStorageOptions _options;
    private readonly IWalReadPressureGovernor _governor;
    private readonly SemaphoreSlim _gate = new(1, 1);

    // Entries kept sorted ascending by offset. Out-of-order batch arrival
    // (LatticeOptions.WalMaxPendingBatches > 1) is handled by inserting at
    // the sorted position; a failed flush simply never adds the batch, so
    // a gap in the offset sequence is surfaced honestly on read.
    private readonly List<IndexEntry> _entries = new();

    private FileStream? _stream;
    private bool _loaded;
    private bool _disposed;
    private long _writePosition;
    private long _retainedBytes;
    private long _deadBytes;
    private long _deadEntries;
    private long _trimWatermark = -1;

    // Captured by RecoverFromDisk before it truncates, and published once by
    // RecordRecoveryOutcome. Recovery destroys the bytes these describe, so
    // they cannot be recomputed after the load completes (issue #3366).
    private long _recoveryTornTailBytes;
    private long _recoveryTornTailRecords;

    // Built once per shard rather than per emission. The priming pass and the
    // compaction record site are both on paths that must not allocate to
    // report, so the three tags every compaction measurement carries are
    // cached here instead of being constructed at each call.
    private readonly KeyValuePair<string, object?> _treeTag;
    private readonly KeyValuePair<string, object?> _tenantTag;

    // Compaction is decided per shard - the ratio test reads this instance's
    // own _retainedBytes and _deadBytes - so a tree-scoped measurement is the
    // average of one threshold test per shard and reports a dead fraction no
    // shard holds. Issue #3206 measured that: three of eight shards holding
    // 81% of a 1.6 GB WAL had never compacted while the tree-level counters
    // advanced healthily. LatticeMetrics.TagShard is the correct key rather
    // than TagPartition, which names the producer-side writer partition and is
    // reserved for the writer-layer instruments.
    private readonly KeyValuePair<string, object?> _shardTag;

    internal FileWalShard(string directory, FileWalStorageOptions options)
        : this(directory, options, string.Empty, 0, GcWalReadPressureGovernor.Instance)
    {
    }

    internal FileWalShard(
        string directory,
        FileWalStorageOptions options,
        string treeId,
        int shardIndex,
        IWalReadPressureGovernor governor)
    {
        _directory = directory;
        _logPath = Path.Combine(directory, "wal.log");
        _options = options;
        _treeId = treeId;
        _shardIndex = shardIndex;
        _governor = governor;
        _treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        _tenantTag = LatticeTenantLabel.ForTree(treeId);
        _shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, shardIndex);
    }

    /// <summary>
    /// Counts reads that had to give up window width to complete: once per
    /// narrowing step forced by an allocation failure, and once more when
    /// even a single-entry page was unaffordable. Monotonic. Exposed so the
    /// degradation path is directly observable in a test rather than only
    /// inferable from the absence of a crash.
    /// </summary>
    internal long ReadPressureDegradations => Interlocked.Read(ref _readPressureDegradations);

    private long _readPressureDegradations;

    /// <summary>Appends a dense, non-overlapping batch atomically.</summary>
    internal async Task AppendAsync(IReadOnlyList<PreparedWalRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            ValidateDenseWithinBatch(records);
            RejectOverlap(records);
            WriteBatch(records);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Snapshots up to <paramref name="maxEntries"/> payloads with offset
    /// strictly greater than <paramref name="fromOffsetExclusive"/>, in
    /// ascending offset order, materialising each payload into a
    /// freshly-owned array. The page is additionally bounded to
    /// <paramref name="maxBytes"/> total payload bytes, so a run of large
    /// records cannot materialise an unbounded page (issue #2689).
    /// </summary>
    /// <param name="fromOffsetExclusive">Exclusive lower bound on offset.</param>
    /// <param name="maxEntries">Maximum entries to return; must be at least <c>1</c>.</param>
    /// <param name="maxBytes">
    /// Maximum total payload bytes to materialise; must be at least
    /// <c>1</c>. At least one entry is always returned even when it alone
    /// exceeds this budget, so the bound can never stall a reader. The
    /// value is an upper bound only: it is narrowed further, per read, by
    /// the process's current memory occupancy (issue #2742), and a page
    /// that still cannot be allocated is retried at a quarter of its width
    /// down to a single entry before <see cref="WalReadUnderPressureException"/>
    /// is raised.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    internal async Task<(long[] Offsets, byte[][] Payloads)> SnapshotAsync(
        long fromOffsetExclusive,
        int maxEntries,
        long maxBytes,
        CancellationToken cancellationToken)
    {
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries), maxEntries, "At least one entry must be requested per read.");
        }

        if (maxBytes < 1L)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxBytes), maxBytes, "At least one byte must be budgeted per read.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            if (fromOffsetExclusive == long.MaxValue)
            {
                // No entry can have an offset greater than long.MaxValue;
                // fromOffsetExclusive + 1 would overflow to long.MinValue
                // and wrongly snapshot the whole log from the head.
                return (Array.Empty<long>(), Array.Empty<byte[]>());
            }

            var startIndex = LowerBound(fromOffsetExclusive + 1);
            var available = _entries.Count - startIndex;
            if (available <= 0)
            {
                return (Array.Empty<long>(), Array.Empty<byte[]>());
            }

            var budget = NarrowBudget(maxBytes);
            var take = Narrow(startIndex, Math.Min(available, maxEntries), budget);
            return MaterialiseOwnedPage(startIndex, take);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Snapshots a page like <see cref="SnapshotAsync"/>, but decodes each
    /// payload directly from pooled, non-contiguous chunks instead of
    /// handing the caller an owned <c>byte[]</c> per entry.
    /// </summary>
    /// <remarks>
    /// This is the shape the replay read path uses. The caller wants a
    /// deserialized record, never the bytes, so materialising a contiguous
    /// array per entry only to throw it away is pure cost - and it is the
    /// specific cost that fails first on a nearly-full heap, because a
    /// contiguous request needs a single free block rather than merely
    /// enough free memory. Decoding from a <see cref="ReadOnlySequence{T}"/>
    /// removes the entry-sized contiguous requirement entirely: peak
    /// additional memory for a page becomes the decoded records plus a
    /// handful of pooled 64 KiB chunks, whatever the entry size.
    /// <para>
    /// <paramref name="decode"/> is invoked while the shard gate is held and
    /// must not retain the sequence: the chunks behind it are returned to
    /// the pool as soon as it returns.
    /// </para>
    /// </remarks>
    internal async Task<(long[] Offsets, T[] Values)> SnapshotDecodedAsync<T>(
        long fromOffsetExclusive,
        int maxEntries,
        long maxBytes,
        Func<ReadOnlySequence<byte>, T> decode,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(decode);
        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries), maxEntries, "At least one entry must be requested per read.");
        }

        if (maxBytes < 1L)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxBytes), maxBytes, "At least one byte must be budgeted per read.");
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            if (fromOffsetExclusive == long.MaxValue)
            {
                return (Array.Empty<long>(), Array.Empty<T>());
            }

            var startIndex = LowerBound(fromOffsetExclusive + 1);
            var available = _entries.Count - startIndex;
            if (available <= 0)
            {
                return (Array.Empty<long>(), Array.Empty<T>());
            }

            var budget = NarrowBudget(maxBytes);
            var take = Narrow(startIndex, Math.Min(available, maxEntries), budget);
            return DecodePage(startIndex, take, decode);
        }
        finally
        {
            _gate.Release();
        }
    }

    private (long[] Offsets, T[] Values) DecodePage<T>(int startIndex, int take, Func<ReadOnlySequence<byte>, T> decode)
    {
        while (true)
        {
            using var chunks = new PooledPayloadSequence();
            var entry = default(IndexEntry);
            try
            {
                var offsets = new long[take];
                var values = new T[take];
                for (var i = 0; i < take; i++)
                {
                    entry = _entries[startIndex + i];
                    offsets[i] = entry.Offset;
                    chunks.Fill(_stream!, entry.Position, entry.PayloadLength);
                    values[i] = decode(chunks.Sequence);
                }

                return (offsets, values);
            }
            catch (OutOfMemoryException) when (take > 1)
            {
                take = NarrowAfterAllocationFailure(take);
            }
            catch (OutOfMemoryException ex)
            {
                throw UnaffordableRead(entry, ex);
            }
        }
    }

    private (long[] Offsets, byte[][] Payloads) MaterialiseOwnedPage(int startIndex, int take)
    {
        while (true)
        {
            var entry = default(IndexEntry);
            try
            {
                var offsets = new long[take];
                var payloads = new byte[take][];
                for (var i = 0; i < take; i++)
                {
                    entry = _entries[startIndex + i];
                    offsets[i] = entry.Offset;
                    payloads[i] = ReadPayload(entry);
                }

                return (offsets, payloads);
            }
            catch (OutOfMemoryException) when (take > 1)
            {
                take = NarrowAfterAllocationFailure(take);
            }
            catch (OutOfMemoryException ex)
            {
                throw UnaffordableRead(entry, ex);
            }
        }
    }

    /// <summary>
    /// Shrinks a read window after an allocation failure, quartering it down
    /// to a single entry.
    /// </summary>
    /// <remarks>
    /// The pre-read budget is a prediction; this is the correction when the
    /// prediction was wrong. It has to exist because occupancy is sampled at
    /// the last collection and hundreds of leaves read concurrently, so a
    /// window that was affordable when it was chosen can be unaffordable a
    /// moment later. Quartering rather than halving is deliberate: the
    /// failure says the estimate was not slightly optimistic but
    /// categorically so, and each extra attempt is itself an allocation
    /// burst on an already-failing heap, so converging in four steps from a
    /// 256-entry page costs less than converging in eight. Partially built
    /// arrays are dropped by leaving the try block and become collectable
    /// before the retry allocates.
    /// </remarks>
    private int NarrowAfterAllocationFailure(int take)
    {
        Interlocked.Increment(ref _readPressureDegradations);
        var narrowed = take / 4;
        return narrowed < 1 ? 1 : narrowed;
    }

    private WalReadUnderPressureException UnaffordableRead(in IndexEntry entry, Exception inner)
    {
        Interlocked.Increment(ref _readPressureDegradations);
        return new WalReadUnderPressureException(_treeId, _shardIndex, entry.Offset, entry.PayloadLength, inner);
    }

    /// <summary>
    /// Applies the process-wide memory-pressure narrowing to a configured
    /// per-read byte ceiling.
    /// </summary>
    /// <remarks>
    /// The configured ceiling answers "how large may a page be?", which is a
    /// question about the log. It cannot answer "how large may a page be
    /// <i>here, now</i>?", which is a question about the machine, and that
    /// is the question that matters when a deployment is already at the edge
    /// of its heap: a ceiling chosen for healthy operation is exactly the
    /// wrong one for a process whose reads are failing, because affording it
    /// is what is no longer possible. Narrowing is one-way - the configured
    /// value remains an upper bound and is used unchanged whenever the
    /// machine reports room to work in.
    /// </remarks>
    private long NarrowBudget(long maxBytes)
    {
        var narrowed = _governor.NarrowBudget(maxBytes);
        if (narrowed < 1L)
        {
            narrowed = 1L;
        }

        return narrowed > maxBytes ? maxBytes : narrowed;
    }

    /// <summary>
    /// Narrows a count-bounded take window to the longest prefix whose
    /// payload bytes fit <paramref name="maxBytes"/>, always keeping at
    /// least one entry.
    /// </summary>
    /// <remarks>
    /// The write path bounds a batch by entries AND bytes
    /// (<see cref="LatticeOptions.WalMaxBatchEntries"/> /
    /// <see cref="LatticeOptions.WalMaxBatchBytes"/>); before issue #2689
    /// the read path bounded only entries, so a page of large records was
    /// unbounded in memory and was held twice - once materialised here into
    /// <c>byte[][]</c>, then again as the deserializer re-allocated each
    /// payload.
    /// <para>
    /// The window is computed entirely from <see cref="IndexEntry.PayloadLength"/>
    /// in the in-memory index, so the bound costs no extra I/O: it decides
    /// how much to read before reading any of it.
    /// </para>
    /// <para>
    /// The always-take-one floor is load-bearing, not a rounding
    /// convenience. Every reader on this path treats an empty page as
    /// end-of-stream - <c>WalShardGrain.ReadAsync</c> reports
    /// <c>NextSequence = fromSequence</c> and <c>WalCommitLogReader</c>
    /// yields a break - so a page that returned nothing because its first
    /// entry exceeded the budget would stall replay at that offset
    /// forever, reproducing the very wedge this bound exists to end. A
    /// short (but non-empty) page is instead an already-supported
    /// condition: the same readers resume from the last offset actually
    /// returned, so truncating a page is a resumption and never a skip.
    /// </para>
    /// </remarks>
    private int Narrow(int startIndex, int take, long maxBytes)
    {
        var accumulated = 0L;
        for (var i = 0; i < take; i++)
        {
            var length = _entries[startIndex + i].PayloadLength;
            if (i > 0 && accumulated + length > maxBytes)
            {
                return i;
            }

            accumulated += length;
        }

        return take;
    }

    /// <summary>
    /// Returns the highest offset ever assigned to this shard, or <c>-1</c>
    /// when it has never accepted an entry.
    /// <para>
    /// The trim watermark is folded in deliberately (issue #3366).
    /// <c>_entries</c> holds only LIVE entries - <see cref="RecoverFromDisk"/>
    /// drops everything at or below the watermark - so answering from it alone
    /// returns <c>-1</c> for a fully-trimmed shard. The WAL grain sets
    /// <c>_nextOffset = answer + 1</c>, so that regression restarts allocation
    /// at offset <c>0</c> beneath a durable watermark that survives both
    /// recovery and compaction. Entries appended there commit, acknowledge, and
    /// read back normally, then vanish on the next recovery as already-trimmed.
    /// Returning the watermark keeps allocation strictly above it.
    /// </para>
    /// </summary>
    internal async Task<long> GetHighestOffsetAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            return _entries.Count == 0
                ? _trimWatermark
                : Math.Max(_entries[^1].Offset, _trimWatermark);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Returns the lowest live offset, or <c>-1</c> when empty.</summary>
    internal async Task<long> GetLowestOffsetAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            return _entries.Count == 0 ? -1L : _entries[0].Offset;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Returns the retained payload byte total across live entries.</summary>
    internal async Task<long> GetRetainedByteSizeAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            return _retainedBytes;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Returns the shard's physical on-disk footprint: every byte the log
    /// file occupies, including per-record framing and dead (trimmed but
    /// not yet compacted) payload.
    /// <para>
    /// This is the figure that bounds disk, and it is not the one
    /// <see cref="GetRetainedByteSizeAsync"/> returns. A log-structured
    /// backend reclaims space only by rewriting the file, so dead bytes are a
    /// designed-in component of occupancy - up to the compaction threshold's
    /// share of the file - and a policy that reads only the live payload can
    /// be satisfied while the file is twice the size it believes.
    /// </para>
    /// <para>
    /// The read is O(1) and exact rather than a <c>FileInfo.Length</c> stat:
    /// <c>_writePosition</c> tracks the file length by construction, since
    /// recovery truncates to the last good record end and sets the field to
    /// it, every append advances both together, and compaction rewrites to
    /// exactly the new position.
    /// </para>
    /// </summary>
    internal async Task<long> GetPhysicalByteSizeAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            return _writePosition;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Payload bytes belonging to trimmed entries that compaction has not yet
    /// reclaimed. Exposed so a test can assert on the quantity the ceiling
    /// bounds rather than inferring it from file size alone.
    /// </summary>
    internal long DeadBytes => _deadBytes;

    /// <summary>
    /// Trimmed entries that compaction has not yet reclaimed. The record-count
    /// companion to <see cref="DeadBytes"/>: the two together give the dead
    /// records' mean payload, which is what makes the per-record framing
    /// overhead computable and so the shard's true dead ratio exact rather than
    /// bounded (issue #3206).
    /// </summary>
    internal long DeadEntries => _deadEntries;

    /// <summary>Trims every entry with offset &lt;= <paramref name="throughOffsetInclusive"/>.</summary>
    internal async Task TrimAsync(long throughOffsetInclusive, CancellationToken cancellationToken)
    {
        if (throughOffsetInclusive < 0L)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();

            // Persist the durable trim marker first: if it throws we must
            // not have mutated the in-memory view. Losing a marker to a
            // crash only over-retains (safe); observing one on recovery
            // re-applies the trim idempotently.
            AppendTrimMarker(throughOffsetInclusive);

            var firstSurvivor = 0;
            while (firstSurvivor < _entries.Count && _entries[firstSurvivor].Offset <= throughOffsetInclusive)
            {
                _retainedBytes -= _entries[firstSurvivor].PayloadLength;
                _deadBytes += _entries[firstSurvivor].PayloadLength;
                _deadEntries++;
                firstSurvivor++;
            }

            if (firstSurvivor > 0)
            {
                _entries.RemoveRange(0, firstSurvivor);
            }

            if (throughOffsetInclusive > _trimWatermark)
            {
                _trimWatermark = throughOffsetInclusive;
            }

            CompactIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Evaluates this shard against the compaction policy without trimming
    /// anything, and compacts if the policy admits.
    /// <para>
    /// <see cref="TrimAsync"/> already ends in the same evaluation, and it is
    /// unconditional there - a trim that removes no entry still evaluates. So
    /// the gate on every compaction threshold was never "did we trim", it was
    /// "was <see cref="TrimAsync"/> called at all", and a shard the GC stops
    /// scanning is a shard for which it is not. This method is the same
    /// evaluation reached by a path that does not require a release to have
    /// happened first (issue #3207).
    /// </para>
    /// <para>
    /// <b>What it can and cannot reclaim.</b> A compaction's yield is exactly
    /// <c>_deadBytes</c>, and that quantity rises in only two places: inside
    /// <see cref="TrimAsync"/>, and on recovery when a trim marker that
    /// <see cref="TrimAsync"/> wrote is replayed. No append path marks anything
    /// dead. So this evaluation completes the shard that <i>has</i> trimmed
    /// before and is now held at the retention floor: its accumulated dead
    /// bytes were previously measured against no threshold at all, and are now
    /// measured against the same policy a trim would have applied. It is
    /// structurally inert on a shard that has <i>never</i> trimmed, whose
    /// <c>_deadBytes</c> is pinned at zero in perpetuity, and that is correct
    /// rather than a gap: such a shard's retained bytes are live, not dead, so
    /// a rewrite would return none of them and only the retention floor
    /// advancing can. The shard-attributed
    /// <c>orleans.lattice.wal.gc.trim_stop</c> arm, read against that shard's
    /// flat <c>orleans.lattice.wal.entries_trimmed</c>, is what names that
    /// population; no arm derived from <c>_deadBytes</c> can, because the stop
    /// is what prevents the quantity from ever being written.
    /// </para>
    /// <para>
    /// It moves no watermark and mutates no logical state: the offsets
    /// readable before the call are exactly those readable after it, whether
    /// or not a compaction ran. It is therefore safe to call on every pass.
    /// Note that a threshold-gated evaluation is self-limiting and needs no
    /// separate frequency bound, which is the reason it is preferred here over
    /// the threshold-free <see cref="ReconcileAsync"/>: a compaction zeroes
    /// <c>_deadBytes</c>, so every subsequent evaluation returns at the
    /// minimum-dead floor until further trims accumulate, and an evaluation
    /// that declines rewrites nothing. Calling <see cref="ReconcileAsync"/>
    /// here instead would rewrite the whole shard file on every pass that
    /// reached it, trading unbounded WAL growth for unbounded write
    /// amplification.
    /// </para>
    /// </summary>
    internal async Task EvaluateCompactionAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            CompactIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// Activation-time recovery. Forces a load (which rolls forward every
    /// committed batch and discards any torn/uncommitted tail) and then
    /// reclaims trimmed on-disk space via compaction.
    /// </summary>
    internal async Task ReconcileAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            if (_deadBytes > 0)
            {
                Compact(LatticeMetrics.WalCompactionTriggerReconcile);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    // --- gate-held helpers -------------------------------------------------

    private void EnsureLoaded()
    {
        if (_loaded)
        {
            return;
        }

        System.IO.Directory.CreateDirectory(_directory);
        _stream = new FileStream(_logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        RecoverFromDisk();
        _loaded = true;
        PrimeCompactionCounters();
        RecordRecoveryOutcome();
    }

    /// <summary>
    /// Publishes what activation-time recovery discarded, once per load.
    /// <para>
    /// Both arms are zero-primed before the measurement is added, for the
    /// reason given on <see cref="PrimeCompactionCounters"/>: a shard that
    /// recovered cleanly must be distinguishable from a shard that is not
    /// reporting, and an absent series cannot carry that distinction. Priming
    /// unconditionally and then adding only a non-zero measurement keeps the
    /// clean case at an explicit zero rather than an absence.
    /// </para>
    /// <para>
    /// This is the only opportunity to report the quantity at all.
    /// <see cref="RecoverFromDisk"/> truncates the bytes it describes, so once
    /// the load completes the evidence is gone from the log, and nothing
    /// downstream - not the shard, not a snapshot, not a later scrape - can
    /// reconstruct it (issue #3366).
    /// </para>
    /// </summary>
    private void RecordRecoveryOutcome()
    {
        if (_treeId.Length == 0)
        {
            // A bare shard constructed directly by a test has no tree
            // identity to attribute a measurement to.
            return;
        }

        LatticeMetrics.WalRecoveryTornTailBytes.Add(0, _treeTag, _shardTag, _tenantTag);
        LatticeMetrics.WalRecoveryTornTailRecords.Add(0, _treeTag, _shardTag, _tenantTag);

        if (_recoveryTornTailBytes > 0)
        {
            LatticeMetrics.WalRecoveryTornTailBytes.Add(_recoveryTornTailBytes, _treeTag, _shardTag, _tenantTag);
        }

        if (_recoveryTornTailRecords > 0)
        {
            LatticeMetrics.WalRecoveryTornTailRecords.Add(_recoveryTornTailRecords, _treeTag, _shardTag, _tenantTag);
        }
    }

    /// <summary>
    /// Writes a zero on every arm of the compaction counters the first time a
    /// shard is loaded.
    /// <para>
    /// Without it, a deployment whose WAL has simply never needed compacting
    /// is indistinguishable on a scrape from one where this provider is not
    /// registered at all - both render as an absent series. That was exactly
    /// the reading that made issue #3107 take as long to diagnose as it did,
    /// so the arms are armed before anything can fire them.
    /// </para>
    /// <para>
    /// The zeros carry this shard's own <see cref="LatticeMetrics.TagShard"/>,
    /// so the priming guarantee holds per shard rather than per tree. A
    /// tree-scoped prime would leave "this shard has never compacted"
    /// indistinguishable from "this shard is not reporting" the moment one
    /// sibling compacted, which is the ambiguity issue #3206 measured on a
    /// live estate and exists to remove.
    /// </para>
    /// </summary>
    private void PrimeCompactionCounters()
    {
        if (_treeId.Length == 0)
        {
            // A bare shard constructed directly by a test has no tree
            // identity to attribute a measurement to.
            return;
        }

        LatticeMetrics.WalCompactions.Add(0, _treeTag, _shardTag, LatticeMetrics.WalCompactionTriggerRatio, _tenantTag);
        LatticeMetrics.WalCompactions.Add(0, _treeTag, _shardTag, LatticeMetrics.WalCompactionTriggerCeiling, _tenantTag);
        LatticeMetrics.WalCompactions.Add(0, _treeTag, _shardTag, LatticeMetrics.WalCompactionTriggerReconcile, _tenantTag);
        LatticeMetrics.WalCompactionReclaimedBytes.Add(0, _treeTag, _shardTag, _tenantTag);

        // The gate-input samples are armed from the shard's true post-recovery
        // state rather than a synthetic zero, so the very first scrape after a
        // load already carries this shard's retained and dead figures even if
        // it is never trimmed again.
        RecordCompactionEvaluation();
    }

    private void RecoverFromDisk()
    {
        var stream = _stream!;
        var fileLength = stream.Length;
        _entries.Clear();
        _retainedBytes = 0;
        _deadBytes = 0;
        _deadEntries = 0;
        _trimWatermark = -1;
        _recoveryTornTailBytes = 0;
        _recoveryTornTailRecords = 0;

        var committed = new List<IndexEntry>();
        var pending = new List<IndexEntry>();
        long watermark = -1;
        long lastGoodEnd = 0;

        if (fileLength > 0)
        {
            stream.Seek(0, SeekOrigin.Begin);
            using var reader = new BinaryReaderState(stream, fileLength);
            while (reader.TryReadRecord(out var record))
            {
                switch (record.Type)
                {
                    case FileWalRecordFormat.RecordTypeData:
                        pending.Add(new IndexEntry(record.Offset, record.PayloadPosition, record.PayloadLength));
                        break;
                    case FileWalRecordFormat.RecordTypeCommit:
                        if (record.CommitCount != pending.Count)
                        {
                            // A commit that does not seal exactly the
                            // pending run is corruption: stop and treat
                            // everything from here as a torn tail.
                            goto done;
                        }
                        committed.AddRange(pending);
                        pending.Clear();
                        lastGoodEnd = record.EndPosition;
                        break;
                    case FileWalRecordFormat.RecordTypeTrim:
                        if (record.Offset > watermark)
                        {
                            watermark = record.Offset;
                        }
                        lastGoodEnd = record.EndPosition;
                        break;
                    default:
                        goto done;
                }
            }
        }

    done:
        // Roll back any data records that were not sealed by a commit, plus
        // any torn trailing bytes, by truncating to the last durable
        // boundary.
        //
        // Measure before truncating. The truncation destroys the only record
        // that this happened, so a shard that discarded a tail was previously
        // indistinguishable on every surface from one that had nothing to
        // discard (issue #3366). `pending` holds the complete-but-unsealed
        // records; the byte delta additionally covers the torn trailing bytes
        // of a partially-written record, which is why the two are reported
        // separately rather than derived from one another.
        _recoveryTornTailBytes = fileLength > lastGoodEnd ? fileLength - lastGoodEnd : 0;
        _recoveryTornTailRecords = pending.Count;

        if (fileLength > lastGoodEnd)
        {
            stream.SetLength(lastGoodEnd);
        }

        _writePosition = lastGoodEnd;

        committed.Sort(static (a, b) => a.Offset.CompareTo(b.Offset));
        foreach (var entry in committed)
        {
            if (entry.Offset <= watermark)
            {
                _deadBytes += entry.PayloadLength;
                _deadEntries++;
                continue;
            }

            _entries.Add(entry);
            _retainedBytes += entry.PayloadLength;
        }

        _trimWatermark = watermark;
    }

    private void ValidateDenseWithinBatch(IReadOnlyList<PreparedWalRecord> records)
    {
        for (var i = 1; i < records.Count; i++)
        {
            if (records[i].Offset != records[i - 1].Offset + 1)
            {
                throw new InvalidOperationException(
                    $"Append batch for '{_directory}' is not dense within the batch: entry {i} has offset "
                    + $"{records[i].Offset} but expected {records[i - 1].Offset + 1}. Offsets supplied to a single "
                    + "AppendBatchAsync call must be strictly ascending and gap-free.");
            }
        }
    }

    private void RejectOverlap(IReadOnlyList<PreparedWalRecord> records)
    {
        if (_entries.Count == 0)
        {
            return;
        }

        var first = records[0].Offset;
        var last = records[^1].Offset;
        var insertAt = LowerBound(first);
        if (insertAt < _entries.Count && _entries[insertAt].Offset <= last)
        {
            throw new InvalidOperationException(
                $"Append batch for '{_directory}' overlaps an existing entry: offset "
                + $"{_entries[insertAt].Offset} is already persisted.");
        }
    }

    private void WriteBatch(IReadOnlyList<PreparedWalRecord> records)
    {
        var stream = _stream!;
        var total = FileWalRecordFormat.CommitRecordLength;
        for (var i = 0; i < records.Count; i++)
        {
            total += FileWalRecordFormat.DataRecordLength(records[i].Payload.Length);
        }

        var buffer = ArrayPool<byte>.Shared.Rent(total);
        var newEntries = new IndexEntry[records.Count];
        try
        {
            var cursor = 0;
            for (var i = 0; i < records.Count; i++)
            {
                var payload = records[i].Payload.Span;
                var payloadPosition = _writePosition + cursor
                    + FileWalRecordFormat.FramingOverhead - 4 + FileWalRecordFormat.DataBodyPrefix;
                cursor += FileWalRecordFormat.WriteDataRecord(
                    buffer.AsSpan(cursor), records[i].Offset, payload);
                newEntries[i] = new IndexEntry(records[i].Offset, payloadPosition, payload.Length);
            }

            cursor += FileWalRecordFormat.WriteCommitRecord(buffer.AsSpan(cursor), records.Count);

            stream.Seek(_writePosition, SeekOrigin.Begin);
            stream.Write(buffer, 0, cursor);
            stream.Flush(_options.FlushToDisk);
            _writePosition += cursor;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var insertAt = LowerBound(records[0].Offset);
        _entries.InsertRange(insertAt, newEntries);
        for (var i = 0; i < newEntries.Length; i++)
        {
            _retainedBytes += newEntries[i].PayloadLength;
        }
    }

    private void AppendTrimMarker(long throughOffsetInclusive)
    {
        var stream = _stream!;
        Span<byte> buffer = stackalloc byte[FileWalRecordFormat.TrimRecordLength];
        var written = FileWalRecordFormat.WriteTrimRecord(buffer, throughOffsetInclusive);
        stream.Seek(_writePosition, SeekOrigin.Begin);
        stream.Write(buffer[..written]);
        stream.Flush(_options.FlushToDisk);
        _writePosition += written;
    }

    private byte[] ReadPayload(in IndexEntry entry)
    {
        var stream = _stream!;
        var buffer = _governor.Allocate(entry.PayloadLength);
        if (entry.PayloadLength > 0)
        {
            stream.Seek(entry.Position, SeekOrigin.Begin);
            stream.ReadExactly(buffer, 0, entry.PayloadLength);
        }

        return buffer;
    }

    private void CompactIfNeeded()
    {
        // Sampled before any arm is tested, so every evaluation is reported
        // whichever way it goes. That ordering is the point: an absent sample
        // now means the shard was never evaluated, which is a different fault
        // from a shard that is evaluated every sweep and correctly declines.
        RecordCompactionEvaluation();

        if (_deadBytes < _options.CompactionMinimumDeadBytes)
        {
            return;
        }

        var totalPayload = _retainedBytes + _deadBytes;
        if (totalPayload <= 0)
        {
            return;
        }

        // The absolute ceiling is checked first and independently of the
        // ratio. That ordering is the entire point of the option: the ratio
        // bounds waste only relative to live data, so on a large shard it
        // can sit far below its threshold while holding an amount of dead
        // space that is, in absolute terms, unacceptable.
        var ceiling = _options.CompactionMaximumDeadBytes;
        if (ceiling > 0L && _deadBytes >= ceiling)
        {
            Compact(LatticeMetrics.WalCompactionTriggerCeiling);
            return;
        }

        if ((double)_deadBytes / totalPayload < _options.CompactionThreshold)
        {
            return;
        }

        Compact(LatticeMetrics.WalCompactionTriggerRatio);
    }

    private void Compact(KeyValuePair<string, object?> trigger)
    {
        var reclaimed = _deadBytes;
        var stream = _stream!;
        var tempPath = _logPath + ".compacting";

        var newEntries = new IndexEntry[_entries.Count];
        long newWritePosition;
        using (var temp = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            var writeBuffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
            try
            {
                long position = 0;
                for (var i = 0; i < _entries.Count; i++)
                {
                    var payload = ReadPayload(_entries[i]);
                    var needed = FileWalRecordFormat.DataRecordLength(payload.Length);
                    if (writeBuffer.Length < needed)
                    {
                        ArrayPool<byte>.Shared.Return(writeBuffer);
                        writeBuffer = ArrayPool<byte>.Shared.Rent(needed);
                    }

                    var written = FileWalRecordFormat.WriteDataRecord(writeBuffer, _entries[i].Offset, payload);
                    var payloadPosition = position
                        + FileWalRecordFormat.FramingOverhead - 4 + FileWalRecordFormat.DataBodyPrefix;
                    temp.Write(writeBuffer, 0, written);
                    newEntries[i] = new IndexEntry(_entries[i].Offset, payloadPosition, payload.Length);
                    position += written;
                }

                var commitWritten = FileWalRecordFormat.WriteCommitRecord(writeBuffer, _entries.Count);
                temp.Write(writeBuffer, 0, commitWritten);
                position += commitWritten;

                if (_trimWatermark >= 0)
                {
                    var trimWritten = FileWalRecordFormat.WriteTrimRecord(writeBuffer, _trimWatermark);
                    temp.Write(writeBuffer, 0, trimWritten);
                    position += trimWritten;
                }

                temp.Flush(_options.FlushToDisk);
                newWritePosition = position;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(writeBuffer);
            }
        }

        stream.Dispose();
        System.IO.File.Move(tempPath, _logPath, overwrite: true);
        _stream = new FileStream(_logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        _entries.Clear();
        _entries.AddRange(newEntries);
        _writePosition = newWritePosition;
        _deadBytes = 0;
        _deadEntries = 0;
        RecordCompaction(trigger, reclaimed);
    }

    /// <summary>
    /// Records one completed compaction and the dead bytes it released.
    /// <para>
    /// Both instruments are monotonic counters rather than an
    /// <c>UpDownCounter</c> of outstanding dead bytes, for the reason issue
    /// #2700 established: an up-down counter is process-lifetime state, so a
    /// lost compensating write - a disposed shard, a re-created provider, a
    /// torn activation - ratchets the series permanently and makes real waste
    /// indistinguishable from accumulated drift, which is the one question
    /// the instrument exists to answer. Present occupancy is reported instead
    /// as derived truth, by <see cref="GetPhysicalByteSizeAsync"/>.
    /// </para>
    /// <para>
    /// Both carry this shard's <see cref="LatticeMetrics.TagShard"/>, because
    /// the trigger that produced the measurement is shard-local: the ratio
    /// test compares this instance's own dead bytes against its own payload.
    /// The pre-#3206 tree-scoped tagging forced a reader to sum across shards,
    /// which averages one threshold test per shard and hides a stranded
    /// majority behind an active minority.
    /// </para>
    /// </summary>
    private void RecordCompaction(KeyValuePair<string, object?> trigger, long reclaimedBytes)
    {
        if (_treeId.Length == 0)
        {
            return;
        }

        LatticeMetrics.WalCompactions.Add(1, _treeTag, _shardTag, trigger, _tenantTag);
        if (reclaimedBytes > 0)
        {
            LatticeMetrics.WalCompactionReclaimedBytes.Add(reclaimedBytes, _treeTag, _shardTag, _tenantTag);
        }
    }

    /// <summary>
    /// Samples the four quantities the shard-local compaction gate tests, at
    /// the moment it tests them and before any arm fires.
    /// <para>
    /// Publishing the decision's inputs rather than only its outcome is what
    /// lets a reader reconstruct the gate from outside the process. Issue #3206
    /// established that the outcome counters alone cannot settle the question
    /// they are asked: a shard whose series is flat may be declining correctly
    /// every sweep or may never be reaching the evaluation at all, and those
    /// have opposite remedies. It further established that the byte figures
    /// alone cannot settle it either, because both track <b>payload</b> length
    /// while the file stores <b>framed</b> records, so any ratio derived by
    /// subtracting published byte totals carries the framing overhead in
    /// numerator and denominator alike and is strictly an upper bound. The
    /// entry counts make mean payload observable, and mean payload is what
    /// turns that bound back into the exact figure.
    /// </para>
    /// <para>
    /// Sampled unconditionally, including when every quantity is zero, for the
    /// same reason <see cref="PrimeCompactionCounters"/> arms the outcome
    /// counters: an absent series must mean "this shard is not reporting" and
    /// nothing else.
    /// </para>
    /// </summary>
    private void RecordCompactionEvaluation()
    {
        if (_treeId.Length == 0)
        {
            return;
        }

        LatticeMetrics.WalCompactionEvalRetainedBytes.Record(_retainedBytes, _treeTag, _shardTag, _tenantTag);
        LatticeMetrics.WalCompactionEvalDeadBytes.Record(_deadBytes, _treeTag, _shardTag, _tenantTag);
        LatticeMetrics.WalCompactionEvalRetainedEntries.Record(_entries.Count, _treeTag, _shardTag, _tenantTag);
        LatticeMetrics.WalCompactionEvalDeadEntries.Record(_deadEntries, _treeTag, _shardTag, _tenantTag);
    }

    private int LowerBound(long target)
    {
        var lo = 0;
        var hi = _entries.Count;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            if (_entries[mid].Offset < target)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stream?.Dispose();
        _gate.Dispose();
    }

    /// <summary>
    /// Forward-only record scanner over the shard's segment file. Reads
    /// each framed record, validates its CRC and length against the known
    /// file length, and stops at the first torn or corrupt record so the
    /// caller can treat everything beyond as a crash tail.
    /// </summary>
    private sealed class BinaryReaderState : IDisposable
    {
        private readonly FileStream _stream;
        private readonly long _fileLength;
        private long _position;

        internal BinaryReaderState(FileStream stream, long fileLength)
        {
            _stream = stream;
            _fileLength = fileLength;
            _position = 0;
        }

        internal bool TryReadRecord(out ScannedRecord record)
        {
            record = default;
            // Need at least the type byte + body length prefix.
            if (_position + 5 > _fileLength)
            {
                return false;
            }

            Span<byte> header = stackalloc byte[5];
            _stream.Seek(_position, SeekOrigin.Begin);
            _stream.ReadExactly(header);
            var type = header[0];
            var bodyLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(1, 4));

            // bodyLen is read verbatim from a possibly-torn tail. Validate it in
            // 64-bit against the bytes remaining in the file before trusting it:
            // a negative, int-overflowing, or oversized length is a torn tail and
            // is discarded here rather than used to size a buffer. Computing
            // FramingOverhead + bodyLen in 32-bit would wrap a near-int.MaxValue
            // garbage length to a negative Rent length and hard-fail recovery
            // instead of rolling the torn tail back.
            if (bodyLen < 0
                || bodyLen > int.MaxValue - FileWalRecordFormat.FramingOverhead
                || bodyLen > _fileLength - _position - FileWalRecordFormat.FramingOverhead)
            {
                return false;
            }

            var recordLen = FileWalRecordFormat.FramingOverhead + bodyLen;

            var rented = ArrayPool<byte>.Shared.Rent(recordLen);
            try
            {
                _stream.Seek(_position, SeekOrigin.Begin);
                _stream.ReadExactly(rented, 0, recordLen);

                var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(
                    rented.AsSpan(5 + bodyLen, 4));
                var actualCrc = Crc32.HashToUInt32(rented.AsSpan(0, 5 + bodyLen));
                if (storedCrc != actualCrc)
                {
                    return false;
                }

                var payloadPosition = _position + 5;
                record = type switch
                {
                    FileWalRecordFormat.RecordTypeData => new ScannedRecord
                    {
                        Type = type,
                        Offset = BinaryPrimitives.ReadInt64LittleEndian(rented.AsSpan(5, 8)),
                        PayloadPosition = payloadPosition + FileWalRecordFormat.DataBodyPrefix,
                        PayloadLength = bodyLen - FileWalRecordFormat.DataBodyPrefix,
                        EndPosition = _position + recordLen,
                    },
                    FileWalRecordFormat.RecordTypeCommit => new ScannedRecord
                    {
                        Type = type,
                        CommitCount = BinaryPrimitives.ReadInt32LittleEndian(rented.AsSpan(5, 4)),
                        EndPosition = _position + recordLen,
                    },
                    FileWalRecordFormat.RecordTypeTrim => new ScannedRecord
                    {
                        Type = type,
                        Offset = BinaryPrimitives.ReadInt64LittleEndian(rented.AsSpan(5, 8)),
                        EndPosition = _position + recordLen,
                    },
                    _ => new ScannedRecord { Type = type, EndPosition = _position + recordLen },
                };

                // A Data record with a negative payload length is corrupt.
                if (type == FileWalRecordFormat.RecordTypeData && record.PayloadLength < 0)
                {
                    return false;
                }

                _position += recordLen;
                return true;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        public void Dispose()
        {
        }
    }

    private struct ScannedRecord
    {
        public byte Type;
        public long Offset;
        public long PayloadPosition;
        public int PayloadLength;
        public int CommitCount;
        public long EndPosition;
    }
}
