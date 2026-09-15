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
    private long _trimWatermark = -1;

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

    /// <summary>Returns the highest live offset, or <c>-1</c> when empty.</summary>
    internal async Task<long> GetHighestOffsetAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureLoaded();
            return _entries.Count == 0 ? -1L : _entries[^1].Offset;
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
                Compact();
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
    }

    private void RecoverFromDisk()
    {
        var stream = _stream!;
        var fileLength = stream.Length;
        _entries.Clear();
        _retainedBytes = 0;
        _deadBytes = 0;
        _trimWatermark = -1;

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
        if (_deadBytes < _options.CompactionMinimumDeadBytes)
        {
            return;
        }

        var totalPayload = _retainedBytes + _deadBytes;
        if (totalPayload <= 0)
        {
            return;
        }

        if ((double)_deadBytes / totalPayload < _options.CompactionThreshold)
        {
            return;
        }

        Compact();
    }

    private void Compact()
    {
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
