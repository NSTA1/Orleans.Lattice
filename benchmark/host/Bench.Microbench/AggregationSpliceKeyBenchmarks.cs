using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;
using FoldMember = Orleans.Lattice.Views.AggregationRowCodec.FoldMember;
using MemberEntry = Orleans.Lattice.Views.AggregationRowCodec.MemberEntry;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the source-key handling trimmed from the aggregation row splices in
/// <c>Orleans.Lattice.Views.AggregationRowCodec</c>.
/// <para>
/// <b>What was there.</b> <c>SpliceInverse</c> and <c>SpliceFoldInverse</c> both
/// transcode the spliced source key to UTF-8 once up front, so that each stored
/// key can be compared as raw bytes without decoding it. On the branch that adds
/// or replaces an entry, that transcoded span was then thrown away twice over:
/// the sizing pass called <c>Utf8Size(sourceKey)</c>, a full second scan of the
/// string, to recover a byte count the span's own <c>Length</c> already held
/// exactly; and the entry writer was handed the <i>string</i>, so
/// <c>WriteString</c> encoded the very same bytes a third time. The scratch
/// buffer those bytes land in was also a fixed <c>stackalloc byte[256]</c>,
/// zeroed in full on every call however short the key.
/// </para>
/// <para>
/// <b>What ships.</b> The sizing pass derives the added entry's key cost from
/// the transcoded span (<c>SevenBitSize(key.Length) + key.Length</c>), the entry
/// writers take the span and emit it through a new
/// <c>RowWriter.WriteStringBytes</c>, and the scratch buffer is sized to the key
/// actually being transcoded. The encoded row is byte-identical; only the number
/// of passes over the key changes.
/// </para>
/// <para>
/// <b>Baselines are verbatim.</b> <see cref="Legacy"/> holds the pre-change
/// bodies of both splices and both entry writers, byte for byte, together with
/// private copies of the codec's own <c>RowWriter</c> / <c>RowReader</c> cursors
/// and its <c>Utf8Size</c> / <c>SevenBitSize</c> helpers. The baseline lane
/// therefore pays exactly what the shipped lane pays plus the removed work, and
/// nothing else.
/// </para>
/// <para>
/// <b>Controls.</b> Only the add/replace branch pays the removed passes, so the
/// <b>remove</b> lanes are the control: they take the same splice with
/// <c>add: null</c>, where the trim provably cannot help, and must show parity.
/// <see cref="Entries"/> sweeps the shard so that the fixed per-call key work is
/// measured against a growing row, and the one-entry lane is where a trim that
/// merely moved cost would show as a loss.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggkey</c> (or <c>--suite aggkey</c>).
/// No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationSpliceKeyBenchmarks
{
    /// <summary>The number of source keys already present in the spliced row.</summary>
    [Params(1, 16, 64)]
    public int Entries { get; set; }

    private const string SpliceKey = "tenant-7/customer-00429/orders";

    private byte[] _inverseRow = [];
    private byte[] _foldRow = [];
    private MemberEntry _inverseAdd;
    private FoldMember _foldAdd;

    [GlobalSetup]
    public void Setup()
    {
        var inverse = new Dictionary<string, MemberEntry>(StringComparer.Ordinal);
        var fold = new Dictionary<string, FoldMember>(StringComparer.Ordinal);
        for (var i = 0; i < Entries; i++)
        {
            var key = i == 0 ? SpliceKey : $"tenant-7/customer-{i:D5}/orders";
            inverse[key] = new MemberEntry(i * 1.5, i % 3 == 0 ? $"member-{i}" : null);
            fold[key] = new FoldMember(Encoding.UTF8.GetBytes($"value-{i}"), new HybridLogicalClock { WallClockTicks = 1000 + i, Counter = i });
        }

        _inverseRow = AggregationRowCodec.EncodeInverse(inverse);
        _foldRow = AggregationRowCodec.EncodeFoldInverse(fold);
        _inverseAdd = new MemberEntry(42.5, "member-spliced");
        _foldAdd = new FoldMember(Encoding.UTF8.GetBytes("value-spliced"), new HybridLogicalClock { WallClockTicks = 9999, Counter = 7 });

        AssertEquivalence();
    }

    /// <summary>
    /// Proves the shipped splices emit byte-identical rows to the verbatim
    /// pre-change bodies across every branch the trim touches - replace in place,
    /// replace moved to the end, append an absent key, and remove - and that both
    /// reject the same malformed rows at the same byte. Byte identity is the bar
    /// rather than decode equality, because the trim changes how the key's length
    /// prefix and bytes are produced.
    /// </summary>
    private void AssertEquivalence()
    {
        var absent = "tenant-7/customer-99999/orders";

        AssertSame("inverse replace", Legacy.SpliceInverse(_inverseRow, SpliceKey, _inverseAdd), AggregationRowCodec.SpliceInverse(_inverseRow, SpliceKey, _inverseAdd));
        AssertSame("inverse replace moved", Legacy.SpliceInverse(_inverseRow, SpliceKey, _inverseAdd, moveToEnd: true), AggregationRowCodec.SpliceInverse(_inverseRow, SpliceKey, _inverseAdd, moveToEnd: true));
        AssertSame("inverse append", Legacy.SpliceInverse(_inverseRow, absent, _inverseAdd), AggregationRowCodec.SpliceInverse(_inverseRow, absent, _inverseAdd));
        AssertSame("inverse remove", Legacy.SpliceInverse(_inverseRow, SpliceKey, add: null), AggregationRowCodec.SpliceInverse(_inverseRow, SpliceKey, add: null));

        AssertSame("fold replace", Legacy.SpliceFoldInverse(_foldRow, SpliceKey, _foldAdd), AggregationRowCodec.SpliceFoldInverse(_foldRow, SpliceKey, _foldAdd));
        AssertSame("fold replace moved", Legacy.SpliceFoldInverse(_foldRow, SpliceKey, _foldAdd, moveToEnd: true), AggregationRowCodec.SpliceFoldInverse(_foldRow, SpliceKey, _foldAdd, moveToEnd: true));
        AssertSame("fold append", Legacy.SpliceFoldInverse(_foldRow, absent, _foldAdd), AggregationRowCodec.SpliceFoldInverse(_foldRow, absent, _foldAdd));
        AssertSame("fold remove", Legacy.SpliceFoldInverse(_foldRow, SpliceKey, add: null), AggregationRowCodec.SpliceFoldInverse(_foldRow, SpliceKey, add: null));

        // A key long enough to leave the stack buffer exercises the rented path
        // the sizing change also touches, and a non-ASCII key proves the byte
        // count taken from the transcoded span still matches the encoder's.
        var longKey = new string('k', 400);
        AssertSame("inverse long key", Legacy.SpliceInverse(_inverseRow, longKey, _inverseAdd), AggregationRowCodec.SpliceInverse(_inverseRow, longKey, _inverseAdd));
        var wideKey = "tenant-\u00e9\u00e8/customer-\u4e2d\u6587/orders";
        AssertSame("inverse wide key", Legacy.SpliceInverse(_inverseRow, wideKey, _inverseAdd), AggregationRowCodec.SpliceInverse(_inverseRow, wideKey, _inverseAdd));
        AssertSame("fold wide key", Legacy.SpliceFoldInverse(_foldRow, wideKey, _foldAdd), AggregationRowCodec.SpliceFoldInverse(_foldRow, wideKey, _foldAdd));

        // The spliced row must still decode, so the trim cannot have produced a
        // self-consistent but unreadable layout.
        _ = AggregationRowCodec.DecodeInverse(AggregationRowCodec.SpliceInverse(_inverseRow, absent, _inverseAdd)!);
        _ = AggregationRowCodec.DecodeFoldInverse(AggregationRowCodec.SpliceFoldInverse(_foldRow, absent, _foldAdd)!);

        // Malformed input must be rejected identically: a truncated row fails in
        // both lanes rather than being admitted by the reordered sizing.
        var truncated = _inverseRow[..(_inverseRow.Length / 2)];
        AssertThrowsBoth(truncated);
    }

    private static void AssertSame(string what, byte[]? expected, byte[]? actual)
    {
        if (expected is null || actual is null)
        {
            if (!ReferenceEquals(expected, actual) && (expected is null) != (actual is null))
            {
                throw new InvalidOperationException($"Splice equivalence failed ({what}): one lane returned null.");
            }

            return;
        }

        if (!expected.AsSpan().SequenceEqual(actual))
        {
            throw new InvalidOperationException($"Splice equivalence failed ({what}): rows differ.");
        }
    }

    private void AssertThrowsBoth(byte[] malformed)
    {
        var legacyThrew = false;
        var shippedThrew = false;
        try
        {
            _ = Legacy.SpliceInverse(malformed, SpliceKey, _inverseAdd);
        }
        catch (Exception)
        {
            legacyThrew = true;
        }

        try
        {
            _ = AggregationRowCodec.SpliceInverse(malformed, SpliceKey, _inverseAdd);
        }
        catch (Exception)
        {
            shippedThrew = true;
        }

        if (legacyThrew != shippedThrew)
        {
            throw new InvalidOperationException("Malformed-row rejection differs between the baseline and shipped splices.");
        }
    }

    /// <summary>The pre-change inverse splice, replacing an entry already on the row.</summary>
    [Benchmark(Baseline = true, Description = "Inverse splice, replace (baseline)")]
    public byte[]? InverseReplaceLegacy() => Legacy.SpliceInverse(_inverseRow, SpliceKey, _inverseAdd);

    /// <summary>The shipped inverse splice, replacing an entry already on the row.</summary>
    [Benchmark(Description = "Inverse splice, replace (shipped)")]
    public byte[]? InverseReplaceShipped() => AggregationRowCodec.SpliceInverse(_inverseRow, SpliceKey, _inverseAdd);

    /// <summary>The pre-change fold splice, replacing an entry already on the row.</summary>
    [Benchmark(Description = "Fold splice, replace (baseline)")]
    public byte[]? FoldReplaceLegacy() => Legacy.SpliceFoldInverse(_foldRow, SpliceKey, _foldAdd);

    /// <summary>The shipped fold splice, replacing an entry already on the row.</summary>
    [Benchmark(Description = "Fold splice, replace (shipped)")]
    public byte[]? FoldReplaceShipped() => AggregationRowCodec.SpliceFoldInverse(_foldRow, SpliceKey, _foldAdd);

    /// <summary>Control: the pre-change inverse splice on the remove branch, which pays none of the removed work.</summary>
    [Benchmark(Description = "Inverse splice, remove (control, baseline)")]
    public byte[]? InverseRemoveLegacy() => Legacy.SpliceInverse(_inverseRow, SpliceKey, add: null);

    /// <summary>Control: the shipped inverse splice on the remove branch; must show parity.</summary>
    [Benchmark(Description = "Inverse splice, remove (control, shipped)")]
    public byte[]? InverseRemoveShipped() => AggregationRowCodec.SpliceInverse(_inverseRow, SpliceKey, add: null);

    /// <summary>
    /// Isolation, baseline: only the key handling the add/replace branch changed.
    /// The end-to-end lanes above are dominated by copying the surrounding row,
    /// which the trim does not touch, so this lane is what makes the saving
    /// attributable. Both isolation lanes drive the same writer and differ only
    /// in the method called.
    /// </summary>
    [Benchmark(Description = "Key handling only (isolated, baseline)")]
    public int KeyHandlingLegacy()
    {
        Span<byte> keyBuffer = stackalloc byte[256];
        Span<byte> destination = stackalloc byte[256];
        return Legacy.KeyHandlingLegacy(SpliceKey, keyBuffer, destination);
    }

    /// <summary>Isolation, shipped: one transcode, an exact size, and a copy of the bytes it produced.</summary>
    [Benchmark(Description = "Key handling only (isolated, shipped)")]
    public int KeyHandlingShipped()
    {
        Span<byte> keyBuffer = stackalloc byte[256];
        Span<byte> destination = stackalloc byte[256];
        return Legacy.KeyHandlingShipped(SpliceKey, keyBuffer, destination);
    }

    /// <summary>
    /// Verbatim pre-change bodies of both splices and both entry writers,
    /// together with the private cursors and size helpers they call, copied from
    /// <c>AggregationRowCodec</c> so the baseline lane pays exactly the removed
    /// work and nothing else.
    /// </summary>
    private static class Legacy
    {
        private const int MinimumInverseEntrySize = 1 + sizeof(bool) + sizeof(double);

        private const int MinimumFoldInverseEntrySize = 1 + sizeof(long) + sizeof(int) + sizeof(int);
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
                var firstMatchStart = 0;
                var firstMatchEnd = 0;
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
                        if (matchCount == 1)
                        {
                            firstMatchStart = start;
                            firstMatchEnd = reader.Position;
                        }
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

                // Pass two, block form. A row this codec produced carries a key at
                // most once, so the surviving entries are at most two contiguous
                // runs - those before the match and those after it - and pass one
                // already delimited both. Copying the runs as blocks emits exactly
                // the bytes the entry-by-entry walk below emits, without re-reading
                // a length prefix, re-comparing a key, or re-parsing a field. The
                // general walk is kept for the hostile row that repeats the key.
                if (matchCount <= 1)
                {
                    if (matchCount == 0)
                    {
                        writer.WriteRaw(row[entriesStart..entriesEnd]);
                    }
                    else
                    {
                        writer.WriteRaw(row[entriesStart..firstMatchStart]);
                        if (add is { } inPlace && !moveToEnd)
                        {
                            WriteInverseEntry(ref writer, sourceKey, inPlace);
                        }

                        writer.WriteRaw(row[firstMatchEnd..entriesEnd]);
                    }

                    if (add is { } tail && (matchCount == 0 || moveToEnd))
                    {
                        WriteInverseEntry(ref writer, sourceKey, tail);
                    }

                    return buffer;
                }

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
                var firstMatchStart = 0;
                var firstMatchEnd = 0;
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
                        if (matchCount == 1)
                        {
                            firstMatchStart = start;
                            firstMatchEnd = reader.Position;
                        }
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

                // Pass two, block form - see SpliceInverse. The fold row's saving is
                // strictly larger: every skipped entry also carries an opaque value
                // payload whose length prefix the walk would read only to step over
                // it, and whose bytes it would copy one entry at a time.
                if (matchCount <= 1)
                {
                    if (matchCount == 0)
                    {
                        writer.WriteRaw(row[entriesStart..entriesEnd]);
                    }
                    else
                    {
                        writer.WriteRaw(row[entriesStart..firstMatchStart]);
                        if (add is { } inPlace && !moveToEnd)
                        {
                            WriteFoldInverseEntry(ref writer, sourceKey, inPlace);
                        }

                        writer.WriteRaw(row[firstMatchEnd..entriesEnd]);
                    }

                    if (add is { } tail && (matchCount == 0 || moveToEnd))
                    {
                        WriteFoldInverseEntry(ref writer, sourceKey, tail);
                    }

                    return buffer;
                }

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

        private static void WriteFoldInverseEntry(ref RowWriter writer, string sourceKey, in FoldMember entry)
        {
            writer.WriteString(sourceKey);
            writer.WriteInt64(entry.Timestamp.WallClockTicks);
            writer.WriteInt32(entry.Timestamp.Counter);
            writer.WriteInt32(entry.Value.Length);
            writer.WriteRaw(entry.Value);
        }

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

        /// <summary>
        /// The key handling the add/replace branch performed, in isolation: the
        /// source key is transcoded once into the comparison buffer, scanned a
        /// second time to size the entry, and transcoded a third time to write it.
        /// </summary>
        internal static int KeyHandlingLegacy(string sourceKey, Span<byte> keyBuffer, Span<byte> destination)
        {
            var written = Encoding.UTF8.GetBytes(sourceKey, keyBuffer);
            var key = keyBuffer[..written];
            var addedSize = Utf8Size(sourceKey) + MinimumInverseEntrySize;
            var writer = new RowWriter(destination);
            writer.WriteString(sourceKey);
            return addedSize + key.Length;
        }

        /// <summary>
        /// The same handling as it ships: one transcode, an exact size derived
        /// from the bytes it produced, and a copy of those bytes.
        /// </summary>
        internal static int KeyHandlingShipped(string sourceKey, Span<byte> keyBuffer, Span<byte> destination)
        {
            var written = Encoding.UTF8.GetBytes(sourceKey, keyBuffer);
            var key = keyBuffer[..written];
            var addedSize = SevenBitSize(key.Length) + key.Length + MinimumInverseEntrySize;
            var writer = new RowWriter(destination);
            writer.WriteStringBytes(key);
            return addedSize + key.Length;
        }

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

            /// <summary>
            /// A mirror of the shipped <c>RowWriter.WriteStringBytes</c>, so the
            /// isolation lanes differ only in the method called on the same writer.
            /// </summary>
            public void WriteStringBytes(scoped ReadOnlySpan<byte> utf8)
            {
                Write7BitEncodedInt(utf8.Length);
                utf8.CopyTo(_buffer[_pos..]);
                _pos += utf8.Length;
            }
        }

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
