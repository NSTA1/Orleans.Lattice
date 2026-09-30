using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;
using FoldMember = Orleans.Lattice.Views.AggregationRowCodec.FoldMember;
using MemberEntry = Orleans.Lattice.Views.AggregationRowCodec.MemberEntry;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the block-copy fast path added to the aggregation row splices in
/// <c>Orleans.Lattice.Views.AggregationRowCodec</c>.
/// <para>
/// <b>What was there.</b> <c>SpliceInverse</c> and <c>SpliceFoldInverse</c> walk
/// an encoded shard row twice. Pass one validates the row and measures the
/// spliced result; pass two then re-walked the row <i>entry by entry</i> -
/// re-reading each length prefix, re-comparing each source key against the
/// spliced key, re-parsing each field - purely to re-derive the byte spans pass
/// one had already measured, and copied each surviving entry through with its
/// own <c>WriteRaw</c>.
/// </para>
/// <para>
/// <b>What ships.</b> A row this codec produces carries a source key <i>at most
/// once</i>, so the surviving entries form at most two contiguous runs: those
/// before the matched entry and those after it. Pass one now records that
/// entry's start and end offsets as it goes, and pass two collapses to at most
/// two block copies plus the replacement. The entry-by-entry walk is retained
/// verbatim for the hostile row that repeats a key, so the change removes work
/// without narrowing what the splice accepts.
/// </para>
/// <para>
/// <b>Baselines are verbatim.</b> <see cref="Legacy"/> holds the pre-change
/// method bodies, byte for byte, together with private copies of the codec's own
/// <c>RowWriter</c> / <c>RowReader</c> cursors that those bodies need. Each
/// baseline lane therefore pays exactly what the shipped lane pays minus the
/// trim, and nothing else.
/// </para>
/// <para>
/// <b>Controls.</b> <see cref="Entries"/> sweeps the shard from a single entry
/// upward: the removed pass scales with the row, so the saving must appear as a
/// slope, and the one-entry lane is the control in which a fast path that merely
/// added a branch would show as a loss. A <b>duplicate-key</b> row additionally
/// drives the retained general walk, proving the hostile case is still served
/// and measuring what it now costs.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggblock</c> (or <c>--suite aggblock</c>).
/// No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationBlockSpliceBenchmarks
{
    /// <summary>Entries on the shard row each lane splices. 1 is the control.</summary>
    [Params(1, 16, 64)]
    public int Entries { get; set; }

    private byte[] _inverseShard = null!;
    private byte[] _foldShard = null!;
    private byte[] _inverseDuplicate = null!;

    private string _presentKey = null!;
    private string _absentKey = null!;
    private string _duplicateKey = null!;
    private MemberEntry _entry;
    private FoldMember _foldEntry;

    /// <summary>
    /// Builds the rows each lane splices, then asserts that every shipped splice
    /// is <b>byte-identical</b> to the verbatim body it replaced across all four
    /// mutation shapes, both positions of <c>moveToEnd</c>, the drain-to-empty
    /// and seed-from-empty edges, and the duplicate-key row that forces the
    /// retained walk - and that a truncated row is rejected identically by both.
    /// A pair that disagrees is timing two different computations, so fail the
    /// setup rather than publish the run.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _presentKey = "tenant-a/orders/2024/src-000000";
        _absentKey = "tenant-a/orders/2024/src-999999";
        _duplicateKey = "tenant-a/orders/2024/src-000000";
        _entry = new MemberEntry(4242.5, "member-spliced");
        _foldEntry = new FoldMember(
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 11 });

        var inverse = new Dictionary<string, MemberEntry>(StringComparer.Ordinal);
        var fold = new Dictionary<string, FoldMember>(StringComparer.Ordinal);
        for (var m = 0; m < Entries; m++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"tenant-a/orders/2024/src-{m:D6}");
            inverse[key] = new MemberEntry(m * 1.5, string.Create(CultureInfo.InvariantCulture, $"member-{m}"));
            fold[key] = new FoldMember(
                [(byte)m, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70],
                new HybridLogicalClock { WallClockTicks = 1_000 + m, Counter = m });
        }

        _inverseShard = AggregationRowCodec.EncodeInverse(inverse);
        _foldShard = AggregationRowCodec.EncodeFoldInverse(fold);
        _inverseDuplicate = BuildDuplicateKeyInverseRow(inverse);

        AssertEquivalence();
    }

    /// <summary>
    /// Builds a row carrying <see cref="_duplicateKey"/> twice. The codec never
    /// emits one, so it can only reach a splice from a corrupt or hostile peer -
    /// exactly the case the retained entry-by-entry walk exists to serve.
    /// Assembled by concatenating a one-entry row's payload onto a full row and
    /// bumping the count, which is the encoding the walk would have produced.
    /// </summary>
    private byte[] BuildDuplicateKeyInverseRow(Dictionary<string, MemberEntry> inverse)
    {
        var single = AggregationRowCodec.EncodeInverse(new Dictionary<string, MemberEntry>(StringComparer.Ordinal)
        {
            [_duplicateKey] = inverse[_duplicateKey],
        });

        var payload = single.AsSpan(sizeof(int));
        var buffer = new byte[_inverseShard.Length + payload.Length];
        _inverseShard.CopyTo(buffer, 0);
        payload.CopyTo(buffer.AsSpan(_inverseShard.Length));
        BinaryPrimitives.WriteInt32LittleEndian(buffer, Entries + 1);
        return buffer;
    }

    private void AssertEquivalence()
    {
        foreach (var moveToEnd in new[] { false, true })
        {
            var tag = moveToEnd ? " (moveToEnd)" : string.Empty;
            AssertSameRow(
                $"inverse replace{tag}",
                Legacy.SpliceInverse(_inverseShard, _presentKey, _entry, moveToEnd),
                AggregationRowCodec.SpliceInverse(_inverseShard, _presentKey, _entry, moveToEnd));
            AssertSameRow(
                $"inverse append{tag}",
                Legacy.SpliceInverse(_inverseShard, _absentKey, _entry, moveToEnd),
                AggregationRowCodec.SpliceInverse(_inverseShard, _absentKey, _entry, moveToEnd));
            AssertSameRow(
                $"inverse duplicate-key replace{tag}",
                Legacy.SpliceInverse(_inverseDuplicate, _duplicateKey, _entry, moveToEnd),
                AggregationRowCodec.SpliceInverse(_inverseDuplicate, _duplicateKey, _entry, moveToEnd));
            AssertSameRow(
                $"fold replace{tag}",
                Legacy.SpliceFoldInverse(_foldShard, _presentKey, _foldEntry, moveToEnd),
                AggregationRowCodec.SpliceFoldInverse(_foldShard, _presentKey, _foldEntry, moveToEnd));
            AssertSameRow(
                $"fold append{tag}",
                Legacy.SpliceFoldInverse(_foldShard, _absentKey, _foldEntry, moveToEnd),
                AggregationRowCodec.SpliceFoldInverse(_foldShard, _absentKey, _foldEntry, moveToEnd));
        }

        // Removals, including the no-op removal of a key the row never held and
        // the removal that drains the row to nothing (which must collapse to
        // "delete the row" on both sides, not to a resurrectable zero-entry row).
        AssertSameRow(
            "inverse remove",
            Legacy.SpliceInverse(_inverseShard, _presentKey, null),
            AggregationRowCodec.SpliceInverse(_inverseShard, _presentKey, null));
        AssertSameRow(
            "inverse remove absent",
            Legacy.SpliceInverse(_inverseShard, _absentKey, null),
            AggregationRowCodec.SpliceInverse(_inverseShard, _absentKey, null));
        AssertSameRow(
            "inverse duplicate-key remove",
            Legacy.SpliceInverse(_inverseDuplicate, _duplicateKey, null),
            AggregationRowCodec.SpliceInverse(_inverseDuplicate, _duplicateKey, null));
        AssertSameRow(
            "fold remove",
            Legacy.SpliceFoldInverse(_foldShard, _presentKey, null),
            AggregationRowCodec.SpliceFoldInverse(_foldShard, _presentKey, null));
        AssertSameRow(
            "fold remove absent",
            Legacy.SpliceFoldInverse(_foldShard, _absentKey, null),
            AggregationRowCodec.SpliceFoldInverse(_foldShard, _absentKey, null));

        // Seeding a new shard splices against the zero-entry row.
        AssertSameRow(
            "inverse seed",
            Legacy.SpliceInverse(AggregationRowCodec.EmptyEntryRow, _presentKey, _entry),
            AggregationRowCodec.SpliceInverse(AggregationRowCodec.EmptyEntryRow, _presentKey, _entry));
        AssertSameRow(
            "fold seed",
            Legacy.SpliceFoldInverse(AggregationRowCodec.EmptyEntryRow, _presentKey, _foldEntry),
            AggregationRowCodec.SpliceFoldInverse(AggregationRowCodec.EmptyEntryRow, _presentKey, _foldEntry));

        AssertHostileParity();
    }

    /// <summary>
    /// A row can arrive from a remote peer under <c>ShipView</c> replication, so
    /// a fast path that validated less than the walk it bypasses would have moved
    /// a validation boundary rather than removed work. Feed both sides the same
    /// truncated rows and require both to reject.
    /// </summary>
    private void AssertHostileParity()
    {
        for (var cut = 1; cut < _inverseShard.Length; cut++)
        {
            var truncated = _inverseShard[..cut];
            var legacyRejected = Rejects(() => Legacy.SpliceInverse(truncated, _presentKey, _entry));
            var shippedRejected = Rejects(() => AggregationRowCodec.SpliceInverse(truncated, _presentKey, _entry));
            if (legacyRejected != shippedRejected)
            {
                throw new InvalidOperationException(
                    $"inverse row truncated to {cut} byte(s): legacy rejected={legacyRejected}, shipped rejected={shippedRejected}.");
            }
        }

        for (var cut = 1; cut < _foldShard.Length; cut++)
        {
            var truncated = _foldShard[..cut];
            var legacyRejected = Rejects(() => Legacy.SpliceFoldInverse(truncated, _presentKey, _foldEntry));
            var shippedRejected = Rejects(() => AggregationRowCodec.SpliceFoldInverse(truncated, _presentKey, _foldEntry));
            if (legacyRejected != shippedRejected)
            {
                throw new InvalidOperationException(
                    $"fold row truncated to {cut} byte(s): legacy rejected={legacyRejected}, shipped rejected={shippedRejected}.");
            }
        }
    }

    private static bool Rejects(Action action)
    {
        try
        {
            action();
            return false;
        }
        catch (InvalidDataException)
        {
            return true;
        }
    }

    private static void AssertSameRow(string lane, byte[]? expected, byte[]? actual)
    {
        if (expected is null || actual is null)
        {
            if (expected is null && actual is null)
            {
                return;
            }

            throw new InvalidOperationException(
                $"{lane}: one lane produced a deleted row and the other did not (legacy null={expected is null}).");
        }

        if (!expected.AsSpan().SequenceEqual(actual))
        {
            throw new InvalidOperationException(
                $"{lane}: the shipped row differs from the verbatim baseline ({expected.Length} vs {actual.Length} bytes).");
        }
    }

    // ---- (1) inverse row splice ----

    [Benchmark(Baseline = true, Description = "Inverse replace: entry-by-entry pass two (baseline)")]
    public int LegacyInverseReplace() => Legacy.SpliceInverse(_inverseShard, _presentKey, _entry)!.Length;

    [Benchmark(Description = "Inverse replace: block-copy pass two (shipped)")]
    public int ShippedInverseReplace() => AggregationRowCodec.SpliceInverse(_inverseShard, _presentKey, _entry)!.Length;

    [Benchmark(Description = "Inverse remove: entry-by-entry pass two (baseline)")]
    public int LegacyInverseRemove() => Legacy.SpliceInverse(_inverseShard, _presentKey, null)?.Length ?? 0;

    [Benchmark(Description = "Inverse remove: block-copy pass two (shipped)")]
    public int ShippedInverseRemove() => AggregationRowCodec.SpliceInverse(_inverseShard, _presentKey, null)?.Length ?? 0;

    /// <summary>
    /// The append shape: no entry matches, so the whole entry region is one
    /// block. This is where the trim removes the most per-entry work.
    /// </summary>
    [Benchmark(Description = "Inverse append: entry-by-entry pass two (baseline)")]
    public int LegacyInverseAppend() => Legacy.SpliceInverse(_inverseShard, _absentKey, _entry)!.Length;

    [Benchmark(Description = "Inverse append: block-copy pass two (shipped)")]
    public int ShippedInverseAppend() => AggregationRowCodec.SpliceInverse(_inverseShard, _absentKey, _entry)!.Length;

    // ---- (2) fold-inverse row splice ----

    [Benchmark(Description = "Fold replace: entry-by-entry pass two (baseline)")]
    public int LegacyFoldReplace() => Legacy.SpliceFoldInverse(_foldShard, _presentKey, _foldEntry)!.Length;

    [Benchmark(Description = "Fold replace: block-copy pass two (shipped)")]
    public int ShippedFoldReplace() => AggregationRowCodec.SpliceFoldInverse(_foldShard, _presentKey, _foldEntry)!.Length;

    [Benchmark(Description = "Fold append: entry-by-entry pass two (baseline)")]
    public int LegacyFoldAppend() => Legacy.SpliceFoldInverse(_foldShard, _absentKey, _foldEntry)!.Length;

    [Benchmark(Description = "Fold append: block-copy pass two (shipped)")]
    public int ShippedFoldAppend() => AggregationRowCodec.SpliceFoldInverse(_foldShard, _absentKey, _foldEntry)!.Length;

    // ---- control: the hostile duplicate-key row still takes the general walk ----

    /// <summary>
    /// Control: a row repeating the spliced key cannot take the fast path, so
    /// this pair must show parity. A gap here would mean the branch itself, not
    /// the block copy, is what the other lanes are measuring.
    /// </summary>
    [Benchmark(Description = "Inverse replace, duplicate-key row: entry-by-entry (baseline control)")]
    public int LegacyInverseDuplicate() => Legacy.SpliceInverse(_inverseDuplicate, _duplicateKey, _entry)!.Length;

    [Benchmark(Description = "Inverse replace, duplicate-key row: retained walk (control)")]
    public int ShippedInverseDuplicate() => AggregationRowCodec.SpliceInverse(_inverseDuplicate, _duplicateKey, _entry)!.Length;

    /// <summary>
    /// The splice bodies exactly as they stood before the block-copy fast path,
    /// together with the private cursors they depend on, copied verbatim from
    /// <c>AggregationRowCodec</c>. Nothing here is production code; it exists so
    /// each baseline lane pays precisely what the shipped lane pays minus the
    /// trim under measurement.
    /// </summary>
    private static class Legacy
    {
        private const int MinimumInverseEntrySize = 1 + sizeof(bool) + sizeof(double);

        private const int MinimumFoldInverseEntrySize = 1 + sizeof(long) + sizeof(int) + sizeof(int);

        private static int Utf8Size(string value)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            return SevenBitSize(byteCount) + byteCount;
        }

        private static int SevenBitSize(int value)
        {
            var v = (uint)value;
            var size = 1;
            while (v >= 0x80)
            {
                size++;
                v >>= 7;
            }

            return size;
        }

        internal static byte[]? SpliceInverse(ReadOnlySpan<byte> row, string sourceKey, MemberEntry? add, bool moveToEnd = false)
        {
            byte[]? rented = null;
            var maxKeyBytes = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
            Span<byte> keyBuffer = maxKeyBytes <= 256
                ? stackalloc byte[256]
                : (rented = ArrayPool<byte>.Shared.Rent(maxKeyBytes));
            try
            {
                var key = keyBuffer[..Encoding.UTF8.GetBytes(sourceKey, keyBuffer)];

                // Pass one: validate the row exactly as DecodeInverse would, and
                // measure the spliced result without writing anything.
                var reader = new RowReader(row);
                var count = reader.ReadBoundedCount(MinimumInverseEntrySize);
                var entriesStart = reader.Position;
                var matchCount = 0;
                var matchBytes = 0;
                for (var i = 0; i < count; i++)
                {
                    var start = reader.Position;
                    var matched = reader.ReadStringBytes().SequenceEqual(key);
                    var hasMember = reader.ReadBool();
                    reader.ReadDouble();
                    if (hasMember)
                    {
                        reader.SkipString();
                    }

                    if (matched)
                    {
                        matchCount++;
                        matchBytes += reader.Position - start;
                    }
                }

                var entriesEnd = reader.Position;
                var hasMember2 = add is { Member: not null };
                var addedSize = add is { } entry
                    ? Utf8Size(sourceKey) + sizeof(bool) + sizeof(double) + (hasMember2 ? Utf8Size(entry.Member!) : 0)
                    : 0;
                var newCount = count - matchCount + (add is null ? 0 : 1);
                if (newCount == 0)
                {
                    return null;
                }

                var buffer = new byte[sizeof(int) + (entriesEnd - entriesStart) - matchBytes + addedSize];
                var writer = new RowWriter(buffer);
                writer.WriteInt32(newCount);

                // Pass two: copy every surviving entry through as raw bytes, writing
                // the replacement in the first matched entry's place.
                reader = new RowReader(row);
                reader.ReadBoundedCount(MinimumInverseEntrySize);
                var written = false;
                for (var i = 0; i < count; i++)
                {
                    var start = reader.Position;
                    var matched = reader.ReadStringBytes().SequenceEqual(key);
                    var hasMember = reader.ReadBool();
                    reader.ReadDouble();
                    if (hasMember)
                    {
                        reader.SkipString();
                    }

                    if (!matched)
                    {
                        writer.WriteRaw(row[start..reader.Position]);
                        continue;
                    }

                    if (add is { } replacement && !written && !moveToEnd)
                    {
                        WriteInverseEntry(ref writer, sourceKey, replacement);
                        written = true;
                    }
                }

                if (add is { } appended && !written)
                {
                    WriteInverseEntry(ref writer, sourceKey, appended);
                }

                return buffer;
            }
            finally
            {
                if (rented is not null)
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
        }

        internal static byte[]? SpliceFoldInverse(ReadOnlySpan<byte> row, string sourceKey, FoldMember? add, bool moveToEnd = false)
        {
            byte[]? rented = null;
            var maxKeyBytes = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
            Span<byte> keyBuffer = maxKeyBytes <= 256
                ? stackalloc byte[256]
                : (rented = ArrayPool<byte>.Shared.Rent(maxKeyBytes));
            try
            {
                var key = keyBuffer[..Encoding.UTF8.GetBytes(sourceKey, keyBuffer)];

                var reader = new RowReader(row);
                var count = reader.ReadBoundedCount(MinimumFoldInverseEntrySize);
                var entriesStart = reader.Position;
                var matchCount = 0;
                var matchBytes = 0;
                for (var i = 0; i < count; i++)
                {
                    var start = reader.Position;
                    var matched = reader.ReadStringBytes().SequenceEqual(key);
                    reader.ReadInt64();
                    reader.ReadInt32();
                    reader.SkipBytes(reader.ReadInt32());
                    if (matched)
                    {
                        matchCount++;
                        matchBytes += reader.Position - start;
                    }
                }

                var entriesEnd = reader.Position;
                var addedSize = add is { } entry
                    ? Utf8Size(sourceKey) + sizeof(long) + sizeof(int) + sizeof(int) + entry.Value.Length
                    : 0;
                var newCount = count - matchCount + (add is null ? 0 : 1);
                if (newCount == 0)
                {
                    return null;
                }

                var buffer = new byte[sizeof(int) + (entriesEnd - entriesStart) - matchBytes + addedSize];
                var writer = new RowWriter(buffer);
                writer.WriteInt32(newCount);

                reader = new RowReader(row);
                reader.ReadBoundedCount(MinimumFoldInverseEntrySize);
                var written = false;
                for (var i = 0; i < count; i++)
                {
                    var start = reader.Position;
                    var matched = reader.ReadStringBytes().SequenceEqual(key);
                    reader.ReadInt64();
                    reader.ReadInt32();
                    reader.SkipBytes(reader.ReadInt32());
                    if (!matched)
                    {
                        writer.WriteRaw(row[start..reader.Position]);
                        continue;
                    }

                    if (add is { } replacement && !written && !moveToEnd)
                    {
                        WriteFoldInverseEntry(ref writer, sourceKey, replacement);
                        written = true;
                    }
                }

                if (add is { } appended && !written)
                {
                    WriteFoldInverseEntry(ref writer, sourceKey, appended);
                }

                return buffer;
            }
            finally
            {
                if (rented is not null)
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
        }

        private static void WriteInverseEntry(ref RowWriter writer, string sourceKey, in MemberEntry entry)
        {
            writer.WriteString(sourceKey);
            var hasMember = entry.Member is not null;
            writer.WriteBool(hasMember);
            writer.WriteDouble(entry.Numeric);
            if (hasMember)
            {
                writer.WriteString(entry.Member!);
            }
        }

        private static void WriteFoldInverseEntry(ref RowWriter writer, string sourceKey, in FoldMember entry)
        {
            writer.WriteString(sourceKey);
            writer.WriteInt64(entry.Timestamp.WallClockTicks);
            writer.WriteInt32(entry.Timestamp.Counter);
            writer.WriteInt32(entry.Value.Length);
            writer.WriteRaw(entry.Value);
        }


        /// <summary>
        /// A forward-only cursor that writes the same byte layout as
        /// <see cref="BinaryWriter"/> with <see cref="Encoding.UTF8"/> (7-bit
        /// length-prefixed UTF-8 strings, single-byte bools, little-endian numerics,
        /// raw byte spans) directly into a caller-owned span, so a row can be
        /// encoded into an exact-size array with no intermediate stream or writer.
        /// </summary>
        private ref struct RowWriter(Span<byte> buffer)
        {
            /// <summary>
            /// Longest UTF-16 length whose UTF-8 encoding is provably under the
            /// one-byte 7-bit prefix bound. A UTF-16 code unit encodes to at most
            /// three UTF-8 bytes, and a surrogate pair is two code units for four
            /// bytes, so three times the length is an upper bound on the encoded
            /// size for every string. Written as a constant rather than asked of
            /// <see cref="Encoding.GetMaxByteCount(int)"/>, which is a virtual call
            /// on <see cref="Encoding"/> and was being paid once per string purely
            /// to recompute this same product.
            /// </summary>
            private const int MaxSingleBytePrefixChars = 0x7F / 3;

            private readonly Span<byte> _buffer = buffer;
            private int _pos;

            public void WriteBool(bool value) => _buffer[_pos++] = value ? (byte)1 : (byte)0;

            public void WriteInt32(int value)
            {
                BinaryPrimitives.WriteInt32LittleEndian(_buffer[_pos..], value);
                _pos += sizeof(int);
            }

            public void WriteInt64(long value)
            {
                BinaryPrimitives.WriteInt64LittleEndian(_buffer[_pos..], value);
                _pos += sizeof(long);
            }

            public void WriteDouble(double value)
            {
                BinaryPrimitives.WriteDoubleLittleEndian(_buffer[_pos..], value);
                _pos += sizeof(double);
            }

            public void WriteRaw(ReadOnlySpan<byte> value)
            {
                value.CopyTo(_buffer[_pos..]);
                _pos += value.Length;
            }

            /// <summary>
            /// Writes a 7-bit-encoded UTF-8 byte count followed by the UTF-8 bytes,
            /// exactly as <see cref="BinaryWriter.Write(string)"/> does.
            /// <para>
            /// A string whose <i>worst case</i> UTF-8 length is already below
            /// <c>0x80</c> must encode to fewer than <c>0x80</c> bytes, so its
            /// 7-bit prefix is provably exactly one byte wide. That is the only
            /// thing the count pass was needed for, so the body is encoded straight
            /// past the reserved prefix slot and the prefix is back-filled from the
            /// encoder's own written count - which is by definition the number the
            /// count pass would have returned. That removes a full UTF-8 scan of
            /// every string on the row-encode path, which runs once per source key
            /// on every aggregation fold and re-encode.
            /// </para>
            /// <para>
            /// A longer string keeps the two-pass shape, because its prefix width is
            /// not known before the count and the body cannot be placed without it.
            /// The sizing pass that allocated this buffer measured the same string
            /// with <c>Utf8Size</c>, so the fast path's one-byte prefix and the
            /// space reserved for it agree by construction.
            /// </para>
            /// </summary>
            public void WriteString(string value)
            {
                if (value.Length <= MaxSingleBytePrefixChars)
                {
                    var written = Encoding.UTF8.GetBytes(value, _buffer[(_pos + 1)..]);
                    _buffer[_pos] = (byte)written;
                    _pos += written + 1;
                    return;
                }

                var byteCount = Encoding.UTF8.GetByteCount(value);
                Write7BitEncodedInt(byteCount);
                Encoding.UTF8.GetBytes(value, _buffer[_pos..]);
                _pos += byteCount;
            }

            private void Write7BitEncodedInt(int value)
            {
                var v = (uint)value;
                while (v >= 0x80)
                {
                    _buffer[_pos++] = (byte)(v | 0x80);
                    v >>= 7;
                }

                _buffer[_pos++] = (byte)v;
            }
        }

        /// <summary>
        /// A forward-only cursor that reads the same byte layout
        /// <see cref="RowWriter"/> emits (7-bit length-prefixed UTF-8 strings,
        /// single-byte bools, little-endian numerics, raw byte slices) directly from
        /// a caller-owned span, so a row can be decoded with no intermediate
        /// <see cref="MemoryStream"/> or <see cref="BinaryReader"/> (and no reader
        /// decode buffer) per call. It is the exact inverse of <see cref="RowWriter"/>
        /// and parses the identical format <see cref="BinaryReader"/> with
        /// <see cref="Encoding.UTF8"/> produced, so previously persisted rows read
        /// back unchanged.
        /// </summary>
        private ref struct RowReader(ReadOnlySpan<byte> buffer)
        {
            private readonly ReadOnlySpan<byte> _buffer = buffer;
            private int _pos;

            /// <summary>The number of bytes left to read, never negative.</summary>
            public readonly int Remaining => _buffer.Length - _pos;

            /// <summary>
            /// The cursor's current byte offset into the row. A splice takes it
            /// either side of an entry to delimit that entry's raw bytes, so the
            /// entry can be copied through without being materialised.
            /// </summary>
            public readonly int Position => _pos;

            public bool ReadBool()
            {
                Demand(sizeof(byte));
                return _buffer[_pos++] != 0;
            }

            public int ReadInt32()
            {
                Demand(sizeof(int));
                var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer[_pos..]);
                _pos += sizeof(int);
                return value;
            }

            public long ReadInt64()
            {
                Demand(sizeof(long));
                var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer[_pos..]);
                _pos += sizeof(long);
                return value;
            }

            public double ReadDouble()
            {
                Demand(sizeof(double));
                var value = BinaryPrimitives.ReadDoubleLittleEndian(_buffer[_pos..]);
                _pos += sizeof(double);
                return value;
            }

            public byte[] ReadBytes(int count)
            {
                // The length prefix is attacker-controlled on a ShipView row, so it is
                // validated against what the row can actually hold before the copy is
                // sized from it. Without this a negative or oversized length reaches
                // Slice and raises ArgumentOutOfRangeException, which the drain loop
                // does not recognise as a framing fault.
                if (count < 0 || count > Remaining)
                {
                    throw new InvalidDataException(
                        $"An aggregation row declares a {count}-byte value but only {Remaining} byte(s) remain; the row is truncated or corrupt.");
                }

                var value = _buffer.Slice(_pos, count).ToArray();
                _pos += count;
                return value;
            }

            public string ReadString()
            {
                var byteCount = ReadStringLength();
                var value = Encoding.UTF8.GetString(_buffer.Slice(_pos, byteCount));
                _pos += byteCount;
                return value;
            }

            /// <summary>
            /// Steps over a length-prefixed UTF-8 string without transcoding it.
            /// Validates the prefix exactly as <see cref="ReadString"/> does, so a
            /// corrupt row is rejected at the same byte whether the caller wanted the
            /// string or not - a skip must not be a weaker gate than a read.
            /// </summary>
            public void SkipString()
            {
                // The byte count must land in a local first. Written as
                // `_pos += ReadStringLength()`, C# loads `_pos` BEFORE the call, so
                // the cursor advance that call makes over the length prefix is
                // overwritten by the store - leaving the reader one prefix short and
                // parsing the following fields from inside the previous string.
                var byteCount = ReadStringLength();
                _pos += byteCount;
            }

            /// <summary>
            /// Returns a length-prefixed UTF-8 string's raw bytes without
            /// transcoding them, so a caller comparing against a known key can do so
            /// on bytes rather than by decoding every candidate. Validates the prefix
            /// exactly as <see cref="ReadString"/> does.
            /// </summary>
            public ReadOnlySpan<byte> ReadStringBytes()
            {
                var byteCount = ReadStringLength();
                var value = _buffer.Slice(_pos, byteCount);

                // See SkipString: the count lands in a local before the store.
                _pos += byteCount;
                return value;
            }

            /// <summary>
            /// Steps over a raw byte run, bounding its declared length against the
            /// row exactly as <see cref="ReadBytes"/> does, so a hostile length is
            /// rejected identically whether the caller wanted the bytes or not.
            /// </summary>
            public void SkipBytes(int count)
            {
                if (count < 0 || count > Remaining)
                {
                    throw new InvalidDataException(
                        $"An aggregation row declares a {count}-byte value but only {Remaining} byte(s) remain; the row is truncated or corrupt.");
                }

                _pos += count;
            }

            /// <summary>
            /// Reads and bounds a string's 7-bit-encoded UTF-8 byte-count prefix,
            /// leaving the cursor on the first content byte.
            /// </summary>
            private int ReadStringLength()
            {
                var byteCount = Read7BitEncodedInt();
                if (byteCount < 0 || byteCount > Remaining)
                {
                    throw new InvalidDataException(
                        $"An aggregation row declares a {byteCount}-byte string but only {Remaining} byte(s) remain; the row is truncated or corrupt.");
                }

                return byteCount;
            }

            /// <summary>
            /// Reads a leading entry count and bounds it against the bytes that remain,
            /// so a caller can pre-size a collection from it without a hostile or
            /// corrupt row turning four bytes into a multi-gigabyte allocation. Each
            /// entry costs at least <paramref name="minimumEntrySize"/> bytes, so a
            /// count above that ceiling is necessarily a lie.
            /// </summary>
            public int ReadBoundedCount(int minimumEntrySize)
            {
                var count = ReadInt32();
                var maxPossible = Remaining / minimumEntrySize;
                if (count < 0 || count > maxPossible)
                {
                    throw new InvalidDataException(
                        $"An aggregation row reports {count} entries but its {Remaining} remaining byte(s) can hold at most {maxPossible}; the row is truncated or corrupt.");
                }

                return count;
            }

            private void Demand(int bytes)
            {
                if (Remaining < bytes)
                {
                    throw new InvalidDataException(
                        $"An aggregation row needs {bytes} more byte(s) but only {Remaining} remain; the row is truncated or corrupt.");
                }
            }

            private int Read7BitEncodedInt()
            {
                // Mirrors BinaryReader.Read7BitEncodedInt: low-order 7 bits per byte,
                // continuation flag in the high bit, at most five bytes for a 32-bit
                // value. A malformed prefix is rejected exactly as BinaryReader does.
                var result = 0;
                var shift = 0;
                while (shift < 5 * 7)
                {
                    Demand(sizeof(byte));
                    var b = _buffer[_pos++];
                    result |= (b & 0x7F) << shift;
                    if ((b & 0x80) == 0)
                    {
                        return result;
                    }

                    shift += 7;
                }

                throw new FormatException("The 7-bit encoded length prefix is malformed.");
            }
        }
    }
}
