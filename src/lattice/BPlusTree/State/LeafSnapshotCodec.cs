using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Versioned, allocation-lean binary codec for the row set of a
/// <see cref="LeafSnapshotBlob"/>.
/// <para>
/// The legacy encoding persists <see cref="LeafSnapshotBlob.Rows"/> as an
/// object graph through the Orleans grain-storage serializer, which for the
/// default JSON serializer means a per-row envelope of property names plus a
/// base64 string for every value. This codec replaces that with a single
/// length-prefixed binary frame carrying raw value bytes: the per-row JSON
/// envelope disappears entirely, and both encode and decode run over
/// <see cref="Span{T}"/> with no intermediate <see cref="string"/> and no
/// per-row scratch array.
/// </para>
/// <para>
/// Frame layout (all multi-byte integers little-endian):
/// <code>
/// [0..4)                        magic 4C 53 4E 01 ("LSN" + a control byte)
/// [4]                           format version
/// [5]                           frame flags (reserved, currently 0)
/// [6..8)                        reserved, zero
/// [8..12)                       row count            (int32)
/// [12..16)                      index-table offset   (int32, absolute)
/// [16..20)                      total frame length   (int32, incl. trailer)
/// [20..24)                      reserved, zero
/// [24..indexOffset)             row records, in ascending key order
/// [indexOffset..len-8)          index table: one absolute int32 start offset per row
/// [len-8..len)                  XxHash64 of [0, len-8)
/// </code>
/// Each row record is:
/// <code>
/// int32  key byte length
/// bytes  key, UTF-8
/// byte   row flags (value present / tombstone / origin / vector clock / migrated / merge mode)
/// int64  HLC wall-clock ticks
/// int32  HLC counter
/// int64  expires-at ticks
/// int32  merge mode                    (only when the merge-mode flag is set)
/// int32+bytes origin cluster id, UTF-8 (only when the origin flag is set)
/// int32 entry count, then per entry int32+bytes replica id, int64 ticks, int32 counter
///                                      (only when the vector-clock flag is set)
/// int32  value byte length, then the raw value bytes
///                                      (only when the value flag is set)
/// </code>
/// </para>
/// <para>
/// Two properties of that layout are load-bearing beyond the size win. The
/// value bytes are written last in each record, so a reader that only wants a
/// row's key or metadata never touches the payload. And the trailing index
/// table gives every row an absolute start offset, so a reader holding the
/// frame can seek directly to row <c>i</c> without decoding rows
/// <c>0..i-1</c> - the seam a bounded, key-range-scoped partial hydration
/// needs. <see cref="TryFindFirstRowAtOrAfter"/> is the binary search built on
/// it; <see cref="CompareKeysUtf8"/> is the comparison that keeps that search
/// consistent with the ordinal string order the leaf cache is sorted by.
/// </para>
/// <para>
/// The magic prefix is the dual-read discriminator. It is only ever produced
/// by <see cref="Encode"/>, so a payload that does not start with it is
/// treated as a legacy blob and read from <see cref="LeafSnapshotBlob.Rows"/>
/// instead. Legacy blobs therefore stay readable indefinitely, which is a
/// durability requirement and not a convenience: the coverage-gated WAL GC
/// trims a checkpointed prefix precisely because a snapshot covers it, so a
/// snapshot that stopped being readable over an already-trimmed prefix would
/// be real data loss.
/// </para>
/// </summary>
internal static class LeafSnapshotCodec
{
    /// <summary>
    /// Frame format version written into, and required by, every frame this
    /// codec produces and accepts. A frame carrying any other version is
    /// rejected by <see cref="Validate"/> and therefore treated as "no
    /// snapshot" rather than decoded on a guess.
    /// </summary>
    internal const byte FormatVersion = 1;

    /// <summary>Byte length of the fixed frame header.</summary>
    internal const int HeaderLength = 24;

    /// <summary>Byte length of the trailing checksum.</summary>
    internal const int TrailerLength = 8;

    /// <summary>
    /// Smallest byte length a structurally valid frame can have: a header and
    /// a trailer with no rows and an empty index table.
    /// </summary>
    internal const int MinimumFrameLength = HeaderLength + TrailerLength;

    // "LSN" plus a 0x01 control byte. The control byte is what makes the
    // prefix unforgeable by the legacy encoding: a JSON document is text and
    // never begins with a C0 control character in this position.
    private const byte Magic0 = 0x4C;
    private const byte Magic1 = 0x53;
    private const byte Magic2 = 0x4E;
    private const byte Magic3 = 0x01;

    private const byte RowFlagHasValue = 0x01;
    private const byte RowFlagTombstone = 0x02;
    private const byte RowFlagHasOriginClusterId = 0x04;
    private const byte RowFlagHasVectorClock = 0x08;
    private const byte RowFlagMigrated = 0x10;
    private const byte RowFlagHasMergeMode = 0x20;

    /// <summary>
    /// Encodes <paramref name="rows"/> into a single self-describing frame.
    /// <para>
    /// The caller is expected to supply the rows in ascending ordinal key
    /// order (which is the order the leaf entry cache enumerates in). The
    /// codec preserves the order verbatim and records it in the index table;
    /// <see cref="TryFindFirstRowAtOrAfter"/> is only meaningful for a frame
    /// whose rows were supplied sorted.
    /// </para>
    /// <para>
    /// Exactly one heap allocation happens here: the returned frame. The
    /// required length is measured first, so the buffer is sized exactly and
    /// never grown, and every string is transcoded straight into the frame
    /// with no intermediate array.
    /// </para>
    /// </summary>
    /// <param name="rows">Rows to encode, in ascending ordinal key order.</param>
    /// <returns>The encoded frame.</returns>
    /// <exception cref="ArgumentException">A row carries a <see langword="null"/> key.</exception>
    /// <exception cref="InvalidOperationException">The frame would exceed <see cref="int.MaxValue"/> bytes.</exception>
    internal static byte[] Encode(ReadOnlySpan<LeafSnapshotRow> rows)
    {
        long measured = HeaderLength;
        for (var i = 0; i < rows.Length; i++)
        {
            measured += MeasureRow(in rows[i]);
        }

        var indexOffset = measured;
        measured += (long)rows.Length * sizeof(int);
        var total = measured + TrailerLength;
        if (total > int.MaxValue)
        {
            throw new InvalidOperationException(
                "Leaf snapshot frame exceeds the maximum encodable length of int.MaxValue bytes.");
        }

        var frame = new byte[(int)total];
        var span = frame.AsSpan();

        span[0] = Magic0;
        span[1] = Magic1;
        span[2] = Magic2;
        span[3] = Magic3;
        span[4] = FormatVersion;
        span[5] = 0;
        span[6] = 0;
        span[7] = 0;
        BinaryPrimitives.WriteInt32LittleEndian(span[8..], rows.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span[12..], (int)indexOffset);
        BinaryPrimitives.WriteInt32LittleEndian(span[16..], (int)total);
        BinaryPrimitives.WriteInt32LittleEndian(span[20..], 0);

        var indexBase = (int)indexOffset;
        var pos = HeaderLength;
        for (var i = 0; i < rows.Length; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[(indexBase + (i * sizeof(int)))..], pos);
            pos = WriteRow(span, pos, in rows[i]);
        }

        // The measure pass and the write pass walk the same rows, so a
        // mismatch means the caller mutated the row source between them. Fail
        // loudly rather than persist a frame whose index table lies: a capture
        // that throws is dropped by its best-effort caller and simply retried,
        // whereas a silently malformed frame would later be rejected as "no
        // snapshot" over a prefix the WAL GC may already have trimmed.
        if (pos != indexBase)
        {
            throw new InvalidOperationException(
                "Leaf snapshot row set changed between the measure and write passes.");
        }

        var body = span[..((int)total - TrailerLength)];
        BinaryPrimitives.WriteUInt64LittleEndian(
            span[((int)total - TrailerLength)..],
            XxHash64.HashToUInt64(body));

        return frame;
    }

    /// <summary>
    /// <see langword="true"/> when <paramref name="payload"/> begins with the
    /// frame magic, i.e. when it claims to be a binary frame. This is the
    /// dual-read sniff: a payload that fails it is not a frame at all and the
    /// blob's legacy row list is authoritative. Claiming to be a frame is not
    /// the same as being a valid one - see <see cref="Validate"/>.
    /// </summary>
    /// <param name="payload">Candidate frame bytes.</param>
    internal static bool HasFrameMagic(ReadOnlySpan<byte> payload)
        => payload.Length >= 4
            && payload[0] == Magic0
            && payload[1] == Magic1
            && payload[2] == Magic2
            && payload[3] == Magic3;

    /// <summary>
    /// Fully validates <paramref name="frame"/>: magic, format version,
    /// self-consistent header lengths, checksum over the whole frame, and a
    /// structural walk asserting that every row parses inside the row region
    /// and that the index table matches the layout exactly.
    /// <para>
    /// This is the gate that turns a truncated or corrupt blob into "no
    /// snapshot" instead of a partially decoded cache. A caller must run it
    /// before enumerating, and must treat <see langword="false"/> as an absent
    /// snapshot rather than as coverage.
    /// </para>
    /// </summary>
    /// <param name="frame">Candidate frame bytes.</param>
    internal static bool Validate(ReadOnlySpan<byte> frame)
    {
        if (!TryReadHeader(frame, out var rowCount, out var indexOffset))
        {
            return false;
        }

        var bodyLength = frame.Length - TrailerLength;
        var expected = BinaryPrimitives.ReadUInt64LittleEndian(frame[bodyLength..]);
        if (XxHash64.HashToUInt64(frame[..bodyLength]) != expected)
        {
            return false;
        }

        var pos = HeaderLength;
        for (var i = 0; i < rowCount; i++)
        {
            var declared = BinaryPrimitives.ReadInt32LittleEndian(frame[(indexOffset + (i * sizeof(int)))..]);
            if (declared != pos)
            {
                return false;
            }

            if (!TrySkipRow(frame, indexOffset, ref pos))
            {
                return false;
            }
        }

        return pos == indexOffset;
    }

    /// <summary>
    /// Reads the number of rows a frame carries without decoding any of them.
    /// Returns <see langword="false"/> when the header is not readable.
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="rowCount">Receives the row count on success.</param>
    internal static bool TryGetRowCount(ReadOnlySpan<byte> frame, out int rowCount)
        => TryReadHeader(frame, out rowCount, out _);

    /// <summary>
    /// Sums the logical payload footprint of every row - UTF-8 key length plus
    /// stored value length, zero for a tombstone - using the same formula as
    /// <c>LeafEntryCache.EntryBytes</c>, without materialising a single key
    /// string or value array. Returns <see langword="false"/> when the frame
    /// is not structurally readable.
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="stateBytes">Receives the summed footprint on success.</param>
    internal static bool TryComputeStateBytes(ReadOnlySpan<byte> frame, out long stateBytes)
    {
        stateBytes = 0;
        if (!TryReadHeader(frame, out var rowCount, out var indexOffset))
        {
            return false;
        }

        var pos = HeaderLength;
        long total = 0;
        for (var i = 0; i < rowCount; i++)
        {
            if (!TryMeasureRowFootprint(frame, indexOffset, ref pos, out var rowBytes))
            {
                return false;
            }

            total += rowBytes;
        }

        if (pos != indexOffset)
        {
            return false;
        }

        stateBytes = total;
        return true;
    }

    /// <summary>
    /// Decodes the row at <paramref name="index"/> directly, seeking through
    /// the index table rather than walking the rows before it. This is the
    /// bounded random-access primitive a partial, key-range-scoped hydration
    /// is built from.
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="index">Zero-based row index.</param>
    /// <param name="row">Receives the decoded row on success.</param>
    internal static bool TryReadRowAt(ReadOnlySpan<byte> frame, int index, out LeafSnapshotRow row)
    {
        row = default;
        if (!TryGetRowStart(frame, index, out var start, out var indexOffset))
        {
            return false;
        }

        return TryReadRow(frame, indexOffset, ref start, out row);
    }

    /// <summary>
    /// Returns the UTF-8 key bytes of the row at <paramref name="index"/> as a
    /// slice of <paramref name="frame"/>, allocating nothing at all. Used to
    /// probe keys during a seek without paying for the key string or the row's
    /// payload.
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="index">Zero-based row index.</param>
    /// <param name="keyUtf8">Receives the key slice on success.</param>
    internal static bool TryReadRowKeyUtf8At(ReadOnlySpan<byte> frame, int index, out ReadOnlySpan<byte> keyUtf8)
    {
        keyUtf8 = default;
        if (!TryGetRowStart(frame, index, out var start, out var indexOffset))
        {
            return false;
        }

        if (!TryReadInt32(frame, indexOffset, ref start, out var keyLength)
            || keyLength < 0
            || indexOffset - start < keyLength)
        {
            return false;
        }

        keyUtf8 = frame.Slice(start, keyLength);
        return true;
    }

    /// <summary>
    /// Binary-searches the index table for the first row whose key is greater
    /// than or equal to <paramref name="keyUtf8"/>, comparing with
    /// <see cref="CompareKeysUtf8"/>. Returns <see langword="false"/> only when
    /// the frame is unreadable; a key beyond the last row yields
    /// <see langword="true"/> with <paramref name="index"/> equal to the row
    /// count, exactly as a lower-bound search should.
    /// <para>
    /// Meaningful only for a frame whose rows were encoded in ascending
    /// ordinal key order, which is what <see cref="Encode"/> is documented to
    /// require and what the leaf entry cache always yields.
    /// </para>
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="keyUtf8">Inclusive lower-bound key, UTF-8 encoded.</param>
    /// <param name="index">Receives the zero-based lower-bound row index.</param>
    internal static bool TryFindFirstRowAtOrAfter(ReadOnlySpan<byte> frame, ReadOnlySpan<byte> keyUtf8, out int index)
    {
        index = 0;
        if (!TryReadHeader(frame, out var rowCount, out _))
        {
            return false;
        }

        var low = 0;
        var high = rowCount;
        while (low < high)
        {
            var mid = low + ((high - low) / 2);
            if (!TryReadRowKeyUtf8At(frame, mid, out var probe))
            {
                return false;
            }

            if (CompareKeysUtf8(probe, keyUtf8) < 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        index = low;
        return true;
    }

    /// <summary>
    /// Compares two UTF-8 keys under the same total order that
    /// <see cref="StringComparer.Ordinal"/> imposes on the decoded strings,
    /// without decoding either into a <see cref="string"/>.
    /// <para>
    /// A plain byte-wise compare is <em>not</em> equivalent: ordinal string
    /// comparison orders UTF-16 code units, so a supplementary character
    /// (U+10000 and above, encoded as a surrogate pair) sorts <em>below</em>
    /// U+E000..U+FFFF, whereas its UTF-8 bytes sort above them. Each code
    /// point is therefore mapped to a rank that reproduces the UTF-16 order
    /// before comparing. Malformed UTF-8 on either side falls back to a raw
    /// byte compare so the result stays a total order instead of throwing.
    /// </para>
    /// </summary>
    /// <param name="left">Left key, UTF-8 encoded.</param>
    /// <param name="right">Right key, UTF-8 encoded.</param>
    /// <returns>Negative, zero, or positive as <paramref name="left"/> sorts before, with, or after <paramref name="right"/>.</returns>
    internal static int CompareKeysUtf8(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var leftRemaining = left;
        var rightRemaining = right;
        while (!leftRemaining.IsEmpty && !rightRemaining.IsEmpty)
        {
            if (Rune.DecodeFromUtf8(leftRemaining, out var leftRune, out var leftConsumed) != OperationStatus.Done
                || Rune.DecodeFromUtf8(rightRemaining, out var rightRune, out var rightConsumed) != OperationStatus.Done)
            {
                return leftRemaining.SequenceCompareTo(rightRemaining);
            }

            var cmp = Utf16OrdinalRank(leftRune.Value).CompareTo(Utf16OrdinalRank(rightRune.Value));
            if (cmp != 0)
            {
                return cmp;
            }

            leftRemaining = leftRemaining[leftConsumed..];
            rightRemaining = rightRemaining[rightConsumed..];
        }

        return leftRemaining.IsEmpty ? (rightRemaining.IsEmpty ? 0 : -1) : 1;
    }

    /// <summary>
    /// Reads the header fields, rejecting anything whose declared lengths are
    /// not self-consistent with the supplied buffer. Every other read path
    /// goes through this first, so a truncated buffer can never be walked.
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="rowCount">Receives the declared row count.</param>
    /// <param name="indexOffset">Receives the absolute index-table offset, which is also the exclusive end of the row region.</param>
    internal static bool TryReadHeader(ReadOnlySpan<byte> frame, out int rowCount, out int indexOffset)
    {
        rowCount = 0;
        indexOffset = 0;

        if (frame.Length < MinimumFrameLength || !HasFrameMagic(frame) || frame[4] != FormatVersion)
        {
            return false;
        }

        var declaredRows = BinaryPrimitives.ReadInt32LittleEndian(frame[8..]);
        var declaredIndex = BinaryPrimitives.ReadInt32LittleEndian(frame[12..]);
        var declaredTotal = BinaryPrimitives.ReadInt32LittleEndian(frame[16..]);

        if (declaredTotal != frame.Length || declaredRows < 0 || declaredIndex < HeaderLength)
        {
            return false;
        }

        // The index table must fill the gap between the row region and the
        // trailer exactly. This single equality rejects a truncated frame, a
        // frame with trailing junk, and a row count that disagrees with the
        // table it indexes.
        if (declaredIndex + ((long)declaredRows * sizeof(int)) != declaredTotal - TrailerLength)
        {
            return false;
        }

        rowCount = declaredRows;
        indexOffset = declaredIndex;
        return true;
    }

    /// <summary>
    /// Decodes the row starting at <paramref name="pos"/>, advancing
    /// <paramref name="pos"/> past it. Every read is bounds-checked against
    /// <paramref name="limit"/>, so a malformed frame returns
    /// <see langword="false"/> rather than reading out of the row region. This
    /// is the streaming primitive <see cref="LeafSnapshotRowSequence"/> walks.
    /// </summary>
    /// <param name="frame">Frame bytes.</param>
    /// <param name="limit">Exclusive end of the row region (the index-table offset).</param>
    /// <param name="pos">Row start on entry; the next row's start on success.</param>
    /// <param name="row">Receives the decoded row on success.</param>
    internal static bool TryReadRow(ReadOnlySpan<byte> frame, int limit, ref int pos, out LeafSnapshotRow row)
        => TryReadRowCore(frame, limit, ref pos, materialize: true, out row);

    private static bool TrySkipRow(ReadOnlySpan<byte> frame, int limit, ref int pos)
        => TryReadRowCore(frame, limit, ref pos, materialize: false, out _);

    // Single parser for both the materialising and the skipping walk so the
    // field order can never drift between them.
    private static bool TryReadRowCore(
        ReadOnlySpan<byte> frame,
        int limit,
        ref int pos,
        bool materialize,
        out LeafSnapshotRow row)
    {
        row = default;

        if (!TryReadSpan(frame, limit, ref pos, out var keyUtf8)
            || !TryReadByte(frame, limit, ref pos, out var flags)
            || !TryReadInt64(frame, limit, ref pos, out var wallClockTicks)
            || !TryReadInt32(frame, limit, ref pos, out var counter)
            || !TryReadInt64(frame, limit, ref pos, out var expiresAtTicks))
        {
            return false;
        }

        LatticeMergeMode? mergeMode = null;
        if ((flags & RowFlagHasMergeMode) != 0)
        {
            if (!TryReadInt32(frame, limit, ref pos, out var rawMode))
            {
                return false;
            }

            mergeMode = (LatticeMergeMode)rawMode;
        }

        string? originClusterId = null;
        if ((flags & RowFlagHasOriginClusterId) != 0)
        {
            if (!TryReadSpan(frame, limit, ref pos, out var originUtf8))
            {
                return false;
            }

            if (materialize)
            {
                originClusterId = Encoding.UTF8.GetString(originUtf8);
            }
        }

        VersionVector? vectorClock = null;
        if ((flags & RowFlagHasVectorClock) != 0)
        {
            if (!TryReadInt32(frame, limit, ref pos, out var entryCount) || entryCount < 0)
            {
                return false;
            }

            // Each entry costs at least a length prefix, a tick field, and a
            // counter, so a count that could not possibly fit in the remaining
            // row region is rejected before anything is allocated for it.
            if ((long)entryCount * (sizeof(int) + sizeof(long) + sizeof(int)) > limit - pos)
            {
                return false;
            }

            if (materialize)
            {
                vectorClock = new VersionVector();
            }

            for (var i = 0; i < entryCount; i++)
            {
                if (!TryReadSpan(frame, limit, ref pos, out var replicaUtf8)
                    || !TryReadInt64(frame, limit, ref pos, out var entryTicks)
                    || !TryReadInt32(frame, limit, ref pos, out var entryCounter))
                {
                    return false;
                }

                if (materialize)
                {
                    vectorClock!.Entries[Encoding.UTF8.GetString(replicaUtf8)] =
                        new HybridLogicalClock { WallClockTicks = entryTicks, Counter = entryCounter };
                }
            }
        }

        byte[]? value = null;
        if ((flags & RowFlagHasValue) != 0)
        {
            if (!TryReadSpan(frame, limit, ref pos, out var valueBytes))
            {
                return false;
            }

            if (materialize)
            {
                // The one unavoidable per-row allocation, and it is the
                // rehydrated payload itself: the entry cache stores byte[].
                // No intermediate string, buffer, or copy precedes it.
                value = valueBytes.ToArray();
            }
        }

        if (!materialize)
        {
            return true;
        }

        row = new LeafSnapshotRow(
            Encoding.UTF8.GetString(keyUtf8),
            new LwwValue<byte[]>
            {
                Value = value,
                Timestamp = new HybridLogicalClock { WallClockTicks = wallClockTicks, Counter = counter },
                IsTombstone = (flags & RowFlagTombstone) != 0,
                ExpiresAtTicks = expiresAtTicks,
                OriginClusterId = originClusterId,
                VectorClock = vectorClock,
                IsMigrated = (flags & RowFlagMigrated) != 0,
            },
            mergeMode);
        return true;
    }

    // Walks a row far enough to total its logical footprint, skipping every
    // field that does not contribute to it.
    private static bool TryMeasureRowFootprint(ReadOnlySpan<byte> frame, int limit, ref int pos, out long rowBytes)
    {
        rowBytes = 0;

        if (!TryReadSpan(frame, limit, ref pos, out var keyUtf8)
            || !TryReadByte(frame, limit, ref pos, out var flags)
            || !TryReadInt64(frame, limit, ref pos, out _)
            || !TryReadInt32(frame, limit, ref pos, out _)
            || !TryReadInt64(frame, limit, ref pos, out _))
        {
            return false;
        }

        if ((flags & RowFlagHasMergeMode) != 0 && !TryReadInt32(frame, limit, ref pos, out _))
        {
            return false;
        }

        if ((flags & RowFlagHasOriginClusterId) != 0 && !TryReadSpan(frame, limit, ref pos, out _))
        {
            return false;
        }

        if ((flags & RowFlagHasVectorClock) != 0)
        {
            if (!TryReadInt32(frame, limit, ref pos, out var entryCount) || entryCount < 0)
            {
                return false;
            }

            for (var i = 0; i < entryCount; i++)
            {
                if (!TryReadSpan(frame, limit, ref pos, out _)
                    || !TryReadInt64(frame, limit, ref pos, out _)
                    || !TryReadInt32(frame, limit, ref pos, out _))
                {
                    return false;
                }
            }
        }

        long valueBytes = 0;
        if ((flags & RowFlagHasValue) != 0)
        {
            if (!TryReadSpan(frame, limit, ref pos, out var value))
            {
                return false;
            }

            valueBytes = value.Length;
        }

        rowBytes = keyUtf8.Length + ((flags & RowFlagTombstone) != 0 ? 0 : valueBytes);
        return true;
    }

    private static bool TryGetRowStart(ReadOnlySpan<byte> frame, int index, out int start, out int indexOffset)
    {
        start = 0;
        if (!TryReadHeader(frame, out var rowCount, out indexOffset))
        {
            return false;
        }

        if ((uint)index >= (uint)rowCount)
        {
            return false;
        }

        start = BinaryPrimitives.ReadInt32LittleEndian(frame[(indexOffset + (index * sizeof(int)))..]);
        return start >= HeaderLength && start < indexOffset;
    }

    private static long MeasureRow(in LeafSnapshotRow row)
    {
        var key = row.Key
            ?? throw new ArgumentException("A leaf snapshot row cannot carry a null key.", nameof(row));

        long size = sizeof(int) + Encoding.UTF8.GetByteCount(key);
        size += sizeof(byte) + sizeof(long) + sizeof(int) + sizeof(long);

        if (row.MergeMode.HasValue)
        {
            size += sizeof(int);
        }

        var value = row.Value;
        if (value.OriginClusterId is { } origin)
        {
            size += sizeof(int) + Encoding.UTF8.GetByteCount(origin);
        }

        if (value.VectorClock is { } vectorClock)
        {
            size += sizeof(int);
            foreach (var entry in vectorClock.Entries)
            {
                size += sizeof(int) + Encoding.UTF8.GetByteCount(entry.Key) + sizeof(long) + sizeof(int);
            }
        }

        if (value.Value is { } payload)
        {
            size += sizeof(int) + payload.Length;
        }

        return size;
    }

    private static int WriteRow(Span<byte> span, int pos, in LeafSnapshotRow row)
    {
        pos = WriteString(span, pos, row.Key);

        var value = row.Value;
        byte flags = 0;
        if (value.Value is not null)
        {
            flags |= RowFlagHasValue;
        }

        if (value.IsTombstone)
        {
            flags |= RowFlagTombstone;
        }

        if (value.OriginClusterId is not null)
        {
            flags |= RowFlagHasOriginClusterId;
        }

        if (value.VectorClock is not null)
        {
            flags |= RowFlagHasVectorClock;
        }

        if (value.IsMigrated)
        {
            flags |= RowFlagMigrated;
        }

        if (row.MergeMode.HasValue)
        {
            flags |= RowFlagHasMergeMode;
        }

        span[pos++] = flags;
        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], value.Timestamp.WallClockTicks);
        pos += sizeof(long);
        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], value.Timestamp.Counter);
        pos += sizeof(int);
        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], value.ExpiresAtTicks);
        pos += sizeof(long);

        if (row.MergeMode is { } mode)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], (int)mode);
            pos += sizeof(int);
        }

        if (value.OriginClusterId is { } origin)
        {
            pos = WriteString(span, pos, origin);
        }

        if (value.VectorClock is { } vectorClock)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], vectorClock.Entries.Count);
            pos += sizeof(int);
            foreach (var entry in vectorClock.Entries)
            {
                pos = WriteString(span, pos, entry.Key);
                BinaryPrimitives.WriteInt64LittleEndian(span[pos..], entry.Value.WallClockTicks);
                pos += sizeof(long);
                BinaryPrimitives.WriteInt32LittleEndian(span[pos..], entry.Value.Counter);
                pos += sizeof(int);
            }
        }

        if (value.Value is { } payload)
        {
            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], payload.Length);
            pos += sizeof(int);
            payload.CopyTo(span[pos..]);
            pos += payload.Length;
        }

        return pos;
    }

    private static int WriteString(Span<byte> span, int pos, string value)
    {
        // Transcodes straight into the frame: no intermediate byte[] and no
        // second GetByteCount pass, since GetBytes reports what it wrote.
        var written = Encoding.UTF8.GetBytes(value, span[(pos + sizeof(int))..]);
        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], written);
        return pos + sizeof(int) + written;
    }

    private static bool TryReadByte(ReadOnlySpan<byte> frame, int limit, ref int pos, out byte value)
    {
        value = 0;
        if (pos < 0 || limit - pos < sizeof(byte))
        {
            return false;
        }

        value = frame[pos];
        pos += sizeof(byte);
        return true;
    }

    private static bool TryReadInt32(ReadOnlySpan<byte> frame, int limit, ref int pos, out int value)
    {
        value = 0;
        if (pos < 0 || limit - pos < sizeof(int))
        {
            return false;
        }

        value = BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(pos, sizeof(int)));
        pos += sizeof(int);
        return true;
    }

    private static bool TryReadInt64(ReadOnlySpan<byte> frame, int limit, ref int pos, out long value)
    {
        value = 0;
        if (pos < 0 || limit - pos < sizeof(long))
        {
            return false;
        }

        value = BinaryPrimitives.ReadInt64LittleEndian(frame.Slice(pos, sizeof(long)));
        pos += sizeof(long);
        return true;
    }

    private static bool TryReadSpan(ReadOnlySpan<byte> frame, int limit, ref int pos, out ReadOnlySpan<byte> value)
    {
        value = default;
        if (!TryReadInt32(frame, limit, ref pos, out var length) || length < 0 || limit - pos < length)
        {
            return false;
        }

        value = frame.Slice(pos, length);
        pos += length;
        return true;
    }

    // Maps a Unicode code point onto the order UTF-16 ordinal comparison
    // imposes. Code points below U+10000 keep their natural order; a
    // supplementary code point is ranked immediately after U+D7FF, mirroring
    // the high surrogate its UTF-16 form starts with, and monotonically within
    // that band so supplementary code points still order among themselves.
    private static long Utf16OrdinalRank(int codePoint)
        => codePoint >= 0x10000
            ? (0xD800L << 16) + (codePoint - 0x10000)
            : (long)codePoint << 16;
}
