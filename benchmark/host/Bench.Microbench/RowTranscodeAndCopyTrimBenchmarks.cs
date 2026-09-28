using System.Globalization;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Arbitrates three independent trims on the aggregation row encoder and the
/// CRDT provenance decoders. Every group carries a baseline lane holding a
/// verbatim copy of the replaced body, an optimized lane running the shipped
/// body, and a control lane on the input shape where the trim is expected to
/// buy nothing - so a win that is really measurement noise has somewhere to
/// show itself.
/// <para>
/// <b>(1) rowtranscode - the aggregation row writer transcoded every string
/// twice.</b> <c>RowWriter.WriteString</c> ran
/// <see cref="Encoding.GetByteCount(string)"/> purely to learn how wide the
/// 7-bit length prefix had to be, and then <c>GetBytes</c> to fill the body.
/// When a string's own worst case is already below <c>0x80</c> bytes its prefix
/// is provably exactly one byte wide, which is the only thing the count pass
/// was needed for: the body is encoded straight past the reserved slot and the
/// prefix back-filled from the encoder's written count. Longer strings keep the
/// two-pass shape. This runs once per source key on every aggregation fold and
/// re-encode, which is the hottest view-maintenance cycle.
/// </para>
/// <para>
/// <b>(2) mvcopy - the multi-value register projected through an intermediate
/// list.</b> <c>DecodeCurrentValue</c> copied the entries into a second list to
/// sort, then projected the sorted copy. Projecting first and sorting the
/// projection is order-equivalent - the projection is one-to-one and carries
/// both ordering keys through unchanged, and a dot is unique per
/// <c>(replica, counter)</c> so there are no ties for an unstable sort to order
/// differently - and drops a whole intermediate list plus the copy that filled
/// it.
/// </para>
/// <para>
/// <b>(3) constantfold - a fixed conversion recomputed per call.</b>
/// <c>FlagProvenance.CurrentValue</c> ran the UTF-8 encoder over the string
/// constant <c>"enabled"</c> or <c>"disabled"</c> on every flag read, which is
/// a fixed answer; a UTF-8 literal makes it a memcpy out of the assembly's
/// data section.
/// </para>
/// <para>
/// <b>How to read the columns.</b> Time is the arbiter; allocation is reported
/// because only one lane is allowed to move it. Group (1) writes the same row
/// bytes in both arms and group (3) allocates the same array, so a byte delta
/// there is a defect, not a result. Bytes may legitimately fall in exactly one
/// place: <c>MvCurrentValue_*_Multi</c>, which is the intermediate list the
/// reordering removed.
/// </para>
/// <para>
/// <b>Controls.</b> <c>EncodeInverse_*_LongKey</c> uses source keys whose worst
/// case overflows the one-byte prefix bound, so both arms take the identical
/// two-pass path. <c>MvCurrentValue_*_Single</c> is a single-entry register,
/// which both arms answer through the same untouched fast path.
/// <c>FlagCurrentValue_*_Untouched</c> is a flag that has never been written,
/// which both arms answer with the shared empty singleton before reaching the
/// conversion.
/// </para>
/// <para>
/// <b>The two single-call controls carry a harness confound, and it is why
/// they are read for direction only.</b> A baseline lane calls a static local
/// to this assembly, which the JIT can inline outright; the matching optimized
/// lane calls across an assembly boundary into <c>Orleans.Lattice</c>, which it
/// generally cannot. On a lane whose whole measurement is tens of nanoseconds
/// that fixed difference is the measurement, so
/// <c>MvCurrentValue_*_Single</c> reads as a large regression on code neither
/// arm changed. It is absent from the lanes that matter, where real work
/// dominates: <c>FlagCurrentValue_*_Untouched</c> amortises the call over
/// <see cref="FlagIterations"/> iterations and comes out flat, and
/// <c>MvCurrentValue_*_Multi</c> pays the same fixed overhead while still
/// winning, so its result is understated rather than inflated.
/// </para>
/// <para>
/// The nanosecond-scale surface in group (3) is driven in a fixed-length loop
/// rather than one call per invocation, so the measurement sits well clear of
/// the timer floor; divide by <see cref="FlagIterations"/> for a per-call
/// figure.
/// </para>
/// <para>
/// Run with <c>--suite rowtranscodecopytrims</c> (or
/// <c>BENCH_MICROBENCH_SUITE=rowtranscodecopytrims</c>). This machine is noisy:
/// treat smoke fidelity as an equivalence and direction check only and take
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> as the arbiter.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RowTranscodeAndCopyTrimBenchmarks
{
    private const string ReplicaA = "replica-a";

    /// <summary>Calls per flag lane invocation.</summary>
    public const int FlagIterations = 256;

    // Group 1 - aggregation row transcode.
    private Dictionary<string, AggregationRowCodec.MemberEntry> _shortRow = null!;
    private Dictionary<string, AggregationRowCodec.MemberEntry> _longKeyRow = null!;
    private Dictionary<string, AggregationRowCodec.MemberEntry> _multibyteRow = null!;

    // Group 2 - multi-value register projection.
    private MvRegister _multiRegister = null!;
    private MvRegister _singleRegister = null!;

    // Group 3 needs no corpus; its whole input is two booleans.

    [GlobalSetup]
    public void Setup()
    {
        _shortRow = MakeRow(24, 18, multibyte: false);
        _longKeyRow = MakeRow(24, 64, multibyte: false);
        _multibyteRow = MakeRow(24, 20, multibyte: true);

        _multiRegister = MakeRegister(6);
        _singleRegister = MakeRegister(1);

        AssertRowTranscodeEquivalence();
        AssertMvCopyEquivalence();
        AssertConstantFoldEquivalence();
    }

    // ------------------------------------------------------------ equivalence

    private void AssertRowTranscodeEquivalence()
    {
        AssertSameBytes(
            BaselineEncodeInverse(_shortRow),
            AggregationRowCodec.EncodeInverse(_shortRow),
            "EncodeInverse (short keys)");
        AssertSameBytes(
            BaselineEncodeInverse(_longKeyRow),
            AggregationRowCodec.EncodeInverse(_longKeyRow),
            "EncodeInverse (long keys)");
        AssertSameBytes(
            BaselineEncodeInverse(_multibyteRow),
            AggregationRowCodec.EncodeInverse(_multibyteRow),
            "EncodeInverse (multi-byte keys)");
        AssertSameBytes(
            BaselineEncodeInverse(new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)),
            AggregationRowCodec.EncodeInverse(new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)),
            "EncodeInverse (empty row)");

        // The degenerate inputs a length-gated transcode is most likely to get
        // wrong: an empty key, keys sitting either side of the one-byte prefix
        // bound in UTF-16 length, and multi-byte text whose character count and
        // byte count disagree across that bound - including one whose encoded
        // size overflows the bound while its character count does not.
        var edge = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
        {
            [string.Empty] = new(1.5, string.Empty),
            [new string('a', 41)] = new(2.5, new string('b', 42)),
            [new string('c', 42)] = new(3.5, null),
            [new string('d', 43)] = new(4.5, new string('e', 44)),
            [new string('\u00e9', 40)] = new(5.5, new string('\u4e2d', 30)),
            [new string('\u4e2d', 42)] = new(6.5, new string('\u00e9', 64)),
            [new string('\u4e2d', 43)] = new(7.5, new string('\u4e2d', 128)),
            [new string('\ud83d', 1) + new string('\ude00', 1)] = new(8.5, "\ud83d\ude00 tail"),
        };
        AssertSameBytes(
            BaselineEncodeInverse(edge),
            AggregationRowCodec.EncodeInverse(edge),
            "EncodeInverse (prefix-boundary keys)");

        // The row has to survive its own decoder, not merely match byte for
        // byte, because the prefix is what the reader frames on.
        var decoded = AggregationRowCodec.DecodeInverse(AggregationRowCodec.EncodeInverse(edge));
        if (decoded.Count != edge.Count)
        {
            throw new InvalidOperationException(
                $"EncodeInverse round trip lost entries: expected {edge.Count}, got {decoded.Count}.");
        }
    }

    private void AssertMvCopyEquivalence()
    {
        AssertSameValues(
            BaselineMvDecodeCurrentValue(_multiRegister),
            MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(_multiRegister),
            "MvRegister DecodeCurrentValue (multi)");
        AssertSameValues(
            BaselineMvDecodeCurrentValue(_singleRegister),
            MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(_singleRegister),
            "MvRegister DecodeCurrentValue (single)");
        AssertSameValues(
            BaselineMvDecodeCurrentValue(new MvRegister()),
            MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(new MvRegister()),
            "MvRegister DecodeCurrentValue (empty)");

        // Two replicas whose entries arrive out of replica order, so the sort is
        // load-bearing and a reordering that skipped it would be caught.
        var reversed = new MvRegister();
        reversed.Entries.Add(new MvRegisterEntry { ReplicaId = "replica-z", Counter = 9, Value = [9] });
        reversed.Entries.Add(new MvRegisterEntry { ReplicaId = ReplicaA, Counter = 3, Value = [3] });
        reversed.Entries.Add(new MvRegisterEntry { ReplicaId = ReplicaA, Counter = 1, Value = [1] });
        AssertSameValues(
            BaselineMvDecodeCurrentValue(reversed),
            MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(reversed),
            "MvRegister DecodeCurrentValue (unordered)");

        // A null value has to keep mapping to the shared empty singleton, not to
        // a fresh zero-length array, or the projection reorder would have
        // changed what the caller receives.
        var withNull = new MvRegister();
        withNull.Entries.Add(new MvRegisterEntry { ReplicaId = "replica-y", Counter = 2, Value = null! });
        withNull.Entries.Add(new MvRegisterEntry { ReplicaId = ReplicaA, Counter = 4, Value = [4] });
        AssertSameValues(
            BaselineMvDecodeCurrentValue(withNull),
            MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(withNull),
            "MvRegister DecodeCurrentValue (null value)");
    }

    private void AssertConstantFoldEquivalence()
    {
        foreach (var (hasAnyDot, isEnabled) in new[]
                 {
                     (true, true), (true, false), (false, true), (false, false),
                 })
        {
            AssertSameValues(
                BaselineFlagCurrentValue(hasAnyDot, isEnabled),
                FlagProvenance.CurrentValue(hasAnyDot, isEnabled),
                $"FlagProvenance CurrentValue (hasAnyDot={hasAnyDot}, isEnabled={isEnabled})");
        }

        // The flag element is the contract the projection renders, so assert the
        // literal really is the same text the encoder used to produce.
        AssertSameBytes(
            Encoding.UTF8.GetBytes("enabled"),
            FlagProvenance.CurrentValue(true, true)[0].Element,
            "FlagProvenance enabled element");
        AssertSameBytes(
            Encoding.UTF8.GetBytes("disabled"),
            FlagProvenance.CurrentValue(true, false)[0].Element,
            "FlagProvenance disabled element");
    }

    // ---------------------------------------------------------------- corpora

    private static Dictionary<string, AggregationRowCodec.MemberEntry> MakeRow(
        int entries, int keyLength, bool multibyte)
    {
        var map = new Dictionary<string, AggregationRowCodec.MemberEntry>(entries, StringComparer.Ordinal);
        var fill = multibyte ? '\u00e9' : 'k';
        for (var i = 0; i < entries; i++)
        {
            var suffix = i.ToString("D3", CultureInfo.InvariantCulture);
            var key = new string(fill, Math.Max(1, keyLength - suffix.Length)) + suffix;
            map[key] = new AggregationRowCodec.MemberEntry(
                i * 1.5,
                (i % 3) == 0 ? null : new string(fill, 12) + suffix);
        }

        return map;
    }

    private static MvRegister MakeRegister(int entries)
    {
        var register = new MvRegister();
        for (var i = 0; i < entries; i++)
        {
            register.Entries.Add(new MvRegisterEntry
            {
                ReplicaId = $"replica-{(entries - i):D2}",
                Counter = i + 1,
                Value = Encoding.UTF8.GetBytes($"register-value-{i:D2}"),
            });
            register.Context[$"replica-{(entries - i):D2}"] = i + 1;
        }

        return register;
    }

    // ------------------------------------------------------------- assertions

    private static void AssertSameValues(
        IReadOnlyList<CrdtMemberValue> baseline,
        IReadOnlyList<CrdtMemberValue> optimized,
        string what)
    {
        if (baseline.Count != optimized.Count)
        {
            throw new InvalidOperationException(
                $"{what}: baseline emitted {baseline.Count} value(s), optimized emitted {optimized.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            var b = baseline[i];
            var o = optimized[i];
            if (!string.Equals(b.ReplicaId, o.ReplicaId, StringComparison.Ordinal)
                || b.Ordinal != o.Ordinal
                || !b.Element.AsSpan().SequenceEqual(o.Element))
            {
                throw new InvalidOperationException($"{what}: value {i} differs between the arms.");
            }
        }
    }

    private static void AssertSameBytes(byte[] baseline, byte[] optimized, string what)
    {
        if (!baseline.AsSpan().SequenceEqual(optimized))
        {
            throw new InvalidOperationException(
                $"{what}: the arms produced different bytes ({baseline.Length} vs {optimized.Length}).");
        }
    }

    // ------------------------------------------------------- group 1 baseline

    /// <summary>
    /// Verbatim copy of <c>AggregationRowCodec.EncodeInverse</c> together with
    /// the <c>RowWriter</c> and sizing helpers as they stood before the
    /// transcode was fused, so the baseline pays the identical sizing pass and
    /// per-field writes and differs only in how a string is written.
    /// </summary>
    private static byte[] BaselineEncodeInverse(
        IReadOnlyDictionary<string, AggregationRowCodec.MemberEntry> entries)
    {
        var size = sizeof(int);
        foreach (var (sourceKey, entry) in entries)
        {
            size += BaselineUtf8Size(sourceKey) + sizeof(bool) + sizeof(double)
                + (entry.Member is not null ? BaselineUtf8Size(entry.Member) : 0);
        }

        var buffer = new byte[size];
        var writer = new BaselineRowWriter(buffer);
        writer.WriteInt32(entries.Count);
        foreach (var (sourceKey, entry) in entries)
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

        return buffer;
    }

    private static int BaselineUtf8Size(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        return BaselineSevenBitSize(byteCount) + byteCount;
    }

    private static int BaselineSevenBitSize(int value)
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

    private ref struct BaselineRowWriter(Span<byte> buffer)
    {
        private readonly Span<byte> _buffer = buffer;
        private int _pos;

        public void WriteBool(bool value) => _buffer[_pos++] = value ? (byte)1 : (byte)0;

        public void WriteInt32(int value)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(_buffer[_pos..], value);
            _pos += sizeof(int);
        }

        public void WriteDouble(double value)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteDoubleLittleEndian(_buffer[_pos..], value);
            _pos += sizeof(double);
        }

        public void WriteString(string value)
        {
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

    // ------------------------------------------------------- group 2 baseline

    /// <summary>
    /// Verbatim copy of <c>MvRegisterProvenanceDecoder.DecodeCurrentValue</c> as
    /// it stood before the intermediate list was removed, including the
    /// single-entry fast path both arms still share.
    /// </summary>
    private static IReadOnlyList<CrdtMemberValue> BaselineMvDecodeCurrentValue(object state)
    {
        ArgumentNullException.ThrowIfNull(state);
        var register = (MvRegister)state;
        var entries = register.Entries;
        if (entries.Count == 0) return Array.Empty<CrdtMemberValue>();

        if (entries.Count == 1)
        {
            var only = entries[0];
            return new CrdtMemberValue[]
            {
                new()
                {
                    Element = only.Value is null ? Array.Empty<byte>() : only.Value.AsSpan().ToArray(),
                    ReplicaId = only.ReplicaId,
                    Ordinal = only.Counter,
                },
            };
        }

        var ordered = new List<MvRegisterEntry>(entries);
        ordered.Sort(static (a, b) =>
        {
            var byReplica = string.CompareOrdinal(a.ReplicaId, b.ReplicaId);
            return byReplica != 0 ? byReplica : a.Counter.CompareTo(b.Counter);
        });

        var result = new List<CrdtMemberValue>(ordered.Count);
        for (var i = 0; i < ordered.Count; i++)
        {
            var e = ordered[i];
            result.Add(new CrdtMemberValue
            {
                Element = e.Value is null ? Array.Empty<byte>() : e.Value.AsSpan().ToArray(),
                ReplicaId = e.ReplicaId,
                Ordinal = e.Counter,
            });
        }

        return result;
    }

    // ------------------------------------------------------ group 3 baselines

    /// <summary>
    /// Verbatim copy of <c>FlagProvenance.CurrentValue</c> as it stood before
    /// the UTF-8 literals replaced the encoder call, including the untouched
    /// short circuit both arms still share.
    /// </summary>
    private static IReadOnlyList<CrdtMemberValue> BaselineFlagCurrentValue(bool hasAnyDot, bool isEnabled)
    {
        if (!hasAnyDot) return Array.Empty<CrdtMemberValue>();
        return new[]
        {
            new CrdtMemberValue
            {
                Element = Encoding.UTF8.GetBytes(isEnabled ? "enabled" : "disabled"),
                ReplicaId = string.Empty,
                Ordinal = 0,
            },
        };
    }

    // ------------------------------------------------------------- group 1

    [Benchmark]
    [BenchmarkCategory("rowtranscode")]
    public int EncodeInverse_Baseline_Short() => BaselineEncodeInverse(_shortRow).Length;

    [Benchmark]
    [BenchmarkCategory("rowtranscode")]
    public int EncodeInverse_Optimized_Short() => AggregationRowCodec.EncodeInverse(_shortRow).Length;

    [Benchmark]
    [BenchmarkCategory("rowtranscode")]
    public int EncodeInverse_Baseline_LongKey() => BaselineEncodeInverse(_longKeyRow).Length;

    [Benchmark]
    [BenchmarkCategory("rowtranscode")]
    public int EncodeInverse_Optimized_LongKey() => AggregationRowCodec.EncodeInverse(_longKeyRow).Length;

    [Benchmark]
    [BenchmarkCategory("rowtranscode")]
    public int EncodeInverse_Baseline_Multibyte() => BaselineEncodeInverse(_multibyteRow).Length;

    [Benchmark]
    [BenchmarkCategory("rowtranscode")]
    public int EncodeInverse_Optimized_Multibyte() => AggregationRowCodec.EncodeInverse(_multibyteRow).Length;

    // ------------------------------------------------------------- group 2

    [Benchmark]
    [BenchmarkCategory("mvcopy")]
    public int MvCurrentValue_Baseline_Multi() => BaselineMvDecodeCurrentValue(_multiRegister).Count;

    [Benchmark]
    [BenchmarkCategory("mvcopy")]
    public int MvCurrentValue_Optimized_Multi()
        => MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(_multiRegister).Count;

    [Benchmark]
    [BenchmarkCategory("mvcopy")]
    public int MvCurrentValue_Baseline_Single() => BaselineMvDecodeCurrentValue(_singleRegister).Count;

    [Benchmark]
    [BenchmarkCategory("mvcopy")]
    public int MvCurrentValue_Optimized_Single()
        => MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(_singleRegister).Count;

    // ------------------------------------------------------------- group 3

    [Benchmark]
    [BenchmarkCategory("constantfold")]
    public int FlagCurrentValue_Baseline_Enabled()
    {
        var total = 0;
        for (var i = 0; i < FlagIterations; i++)
        {
            total += BaselineFlagCurrentValue(true, (i & 1) == 0)[0].Element.Length;
        }

        return total;
    }

    [Benchmark]
    [BenchmarkCategory("constantfold")]
    public int FlagCurrentValue_Optimized_Enabled()
    {
        var total = 0;
        for (var i = 0; i < FlagIterations; i++)
        {
            total += FlagProvenance.CurrentValue(true, (i & 1) == 0)[0].Element.Length;
        }

        return total;
    }

    [Benchmark]
    [BenchmarkCategory("constantfold")]
    public int FlagCurrentValue_Baseline_Untouched()
    {
        var total = 0;
        for (var i = 0; i < FlagIterations; i++)
        {
            total += BaselineFlagCurrentValue(false, (i & 1) == 0).Count;
        }

        return total;
    }

    [Benchmark]
    [BenchmarkCategory("constantfold")]
    public int FlagCurrentValue_Optimized_Untouched()
    {
        var total = 0;
        for (var i = 0; i < FlagIterations; i++)
        {
            total += FlagProvenance.CurrentValue(false, (i & 1) == 0).Count;
        }

        return total;
    }
}
