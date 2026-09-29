using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three trims on the leaf snapshot frame codec - the read primitives
/// every bounded hydration goes through - so their time and byte deltas are
/// measurable in the clear rather than buried under a silo, a grain call and a
/// storage provider.
/// <para>
/// All three remove the same wasted work: <c>TryReadHeader</c>, called over
/// and over for constants that cannot change within a frame.
/// <c>TryReadHeader</c> is not a field load - it re-checks the 4-byte magic,
/// the format version, the declared total length against the real buffer
/// length, that the index-table offset lies inside the frame, and that the
/// table fills the gap to the trailer exactly. Paying that per row, or per
/// binary-search probe, is an <c>O(n)</c> or <c>O(log n)</c> revalidation of
/// 24 fixed bytes.
/// </para>
/// <para>
/// (1) <b>Hydration admission asked the same question three times, and its
/// order check re-asked it per row.</b>
/// <c>LeafSnapshotHydrationSource.TryCreate</c> called <c>TryGetRowCount</c>,
/// then <c>TryComputeCacheAggregates</c>, then <c>IsAscendingByKey</c> - three
/// independent header reads - and the third re-read the header once more
/// <em>per row</em>, so a 4096-row frame paid 4099 revalidations to answer
/// three questions about one 24-byte header. It now pays one, and hands the
/// resolved constants back to the caller.
/// </para>
/// <para>
/// A fusion of admission's <em>two traversals</em> (the sequential aggregate
/// walk and the index-table order walk) into a single pass was also built and
/// measured here, and <b>rejected</b>: it reads one index-table slot per row
/// from the far end of the frame, replacing two well-localised streams with
/// one that thrashes between two distant regions, and made admission ~57%
/// slower on a 570 KB frame. The header hoist is kept; the walk fusion is not.
/// </para>
/// <para>
/// (2) <b>Every lower-bound probe re-validated the header.</b> The binary
/// search resolves a row start per probe, and each resolution re-read the
/// header, so a seek over a 4096-row frame paid 12 revalidations instead of
/// one. The header is now read once per seek and threaded through.
/// </para>
/// <para>
/// (3) <b>Every hydrated row re-validated the header.</b> A block hydration
/// decodes 64 rows and re-read the header once per row. It is now read once
/// per block.
/// </para>
/// <para>
/// Read all three for <b>time</b>: none moves a byte of managed heap, and the
/// <c>Allocated</c> column is reported precisely so that can be checked rather
/// than asserted.
/// </para>
/// <para>
/// Every group carries a control lane where the trim is expected to buy little
/// or nothing - a two-row frame for (1), a single probe for (2), a single row
/// read for (3) - because a trim that taxes the case it cannot help is not a
/// trim. Group (1) additionally reports an <b>isolated</b> lane beside the
/// end-to-end one, because admission is dominated by the aggregate walk - work
/// the trim does not remove - so the end-to-end lane alone would understate
/// the removed work and misattribute the remainder.
/// </para>
/// <para>
/// A fourth candidate was measured here and <b>rejected</b>: fusing
/// <c>Encode</c>'s two scans per string (a <c>GetByteCount</c> sizing pass
/// followed by a <c>GetBytes</c> write pass) into a single transcode through a
/// pooled staging buffer. It produced byte-identical frames but ran ~23%
/// <em>slower</em> and allocated <em>more</em>, because <c>GetByteCount</c>
/// over short ASCII is already vectorised to roughly the cost of the
/// <c>memcpy</c> that replaced it, while the staging buffer added rent, growth
/// and return. It is not in this file; the negative result is recorded so the
/// candidate is not re-tried blind.
/// </para>
/// <para>
/// Every baseline lane is a verbatim copy of the code the trim replaced, so it
/// pays exactly the overhead its shipped counterpart pays, and each is
/// asserted in <see cref="Setup"/> to produce exactly the answer its
/// counterpart produces - over corpora that include a two-row frame, an
/// unsorted frame, a truncated frame, ASCII and multi-byte-UTF-8 and
/// surrogate-pair keys, rows with and without an origin cluster id, a merge
/// mode, a value and a multi-entry vector clock, a tombstone, and lower-bound
/// probes below every key, above every key, exactly on a key and between two
/// keys. A lane that answers differently is measuring different work, and the
/// comparison would be void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=leafsnapshotframetrims</c> (or
/// <c>--suite leafsnapshotframetrims</c>); see <c>Program.cs</c>. No Orleans
/// silo is involved, so it runs cheaply at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LeafSnapshotFrameTrimBenchmarks
{
    /// <summary>
    /// Rows in the simulated leaf snapshot. A leaf splits well below this, so
    /// it is a generous upper bound on a real per-capture row set.
    /// </summary>
    private const int RowCount = 4096;

    /// <summary>
    /// Rows a single hydration block admits, mirroring
    /// <c>LeafSnapshotHydrationSource.BlockRows</c>.
    /// </summary>
    private const int BlockRows = 64;

    /// <summary>Lower-bound probes issued per seek lane.</summary>
    private const int SeekProbes = 512;

    /// <summary>Key pairs compared per key-comparison lane.</summary>
    private const int ComparePairs = 512;

    private byte[] _frame = null!;
    private byte[] _tinyFrame = null!;

    private byte[][] _probeKeys = null!;

    // Group 4 corpora. Each is a flat array of 2 * ComparePairs keys, compared
    // pairwise, so a lane's cost is ComparePairs comparisons of one shape.
    private byte[][] _asciiPairs = null!;
    private byte[][] _tinyAsciiPairs = null!;
    private byte[][] _earlyDivergePairs = null!;
    private byte[][] _multibytePairs = null!;

    // Group 5 corpus: keys as the decoder yields them (a string plus the exact
    // UTF-8 length the frame already stated), and the values they account for.
    private string[] _accountingKeys = null!;
    private int[] _accountingKeyUtf8Lengths = null!;
    private byte[][] _accountingValues = null!;

    /// <summary>
    /// Builds the corpora and asserts every optimised lane answers exactly
    /// what its baseline answers, including over the cases where the trims are
    /// expected to buy nothing.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _frame = LeafSnapshotCodec.Encode(BuildRows(RowCount, valueBytes: 32, wide: false));
        _tinyFrame = LeafSnapshotCodec.Encode(BuildRows(2, valueBytes: 32, wide: false));

        _probeKeys = BuildProbeKeys();

        _asciiPairs = BuildAsciiPairs(keyBytes: 24);
        _tinyAsciiPairs = BuildAsciiPairs(keyBytes: 3);
        _earlyDivergePairs = BuildEarlyDivergePairs();
        _multibytePairs = BuildMultibytePairs();
        BuildAccountingCorpus(out _accountingKeys, out _accountingKeyUtf8Lengths, out _accountingValues);

        // The frames every lane reads must themselves validate, or every later
        // assertion is checking a malformed corpus.
        if (!LeafSnapshotCodec.Validate(_frame)
            || !LeafSnapshotCodec.Validate(_tinyFrame)
            || !LeafSnapshotCodec.Validate(LeafSnapshotCodec.Encode(BuildEdgeCaseRows())))
        {
            throw new InvalidOperationException("A benchmark corpus frame does not validate.");
        }

        AssertAdmissionEquivalence();
        AssertSeekEquivalence();
        AssertKeyCompareEquivalence();
        AssertAccountingEquivalence();
    }

    // ---------------------------------------------------------------- group 1

    /// <summary>Admission as three independent probes, the order check re-reading the header per row.</summary>
    [Benchmark(Description = "(1) hydration admission - baseline (three probes, header per row)")]
    public long Admit_Baseline() => BaselineAdmit(_frame);

    /// <summary>Admission as one header read threaded through both passes.</summary>
    [Benchmark(Description = "(1) hydration admission - optimised (one header read)")]
    public long Admit_Optimised()
        => LeafSnapshotCodec.TryAdmitForHydration(_frame, out _, out _, out var stateBytes, out _)
            ? stateBytes
            : -1;

    /// <summary>Isolated: the order check alone, which admission's aggregate walk otherwise masks.</summary>
    [Benchmark(Description = "(1) isolated - ascending-order check - baseline (header per row)")]
    public bool AscendingCheck_Baseline() => BaselineIsAscendingByKey(_frame);

    /// <summary>Isolated: the order check alone, which admission's aggregate walk otherwise masks.</summary>
    [Benchmark(Description = "(1) isolated - ascending-order check - optimised (header hoisted)")]
    public bool AscendingCheck_Optimised() => LeafSnapshotCodec.IsAscendingByKey(_frame);

    /// <summary>
    /// The aggregate walk in isolation. Its baseline / optimised pair is a
    /// cross-run A/B on the <c>[MethodImpl(NoInlining)]</c> attribute alone -
    /// the lane cannot host both shapes at once, because the attribute is a
    /// property of the shipped method rather than of the call site.
    /// </summary>
    [Benchmark(Description = "(1) isolated - aggregate walk (row parser stays inlined)")]
    public long AggregatesOnly()
        => LeafSnapshotCodec.TryComputeCacheAggregates(_frame, out var stateBytes, out _) ? stateBytes : -1;

    /// <summary>Control: a two-row frame, where the per-row header re-read barely exists.</summary>
    [Benchmark(Description = "(1) control - admission over a 2-row frame - baseline")]
    public long AdmitTiny_Baseline() => BaselineAdmit(_tinyFrame);

    /// <summary>Control: a two-row frame, where the per-row header re-read barely exists.</summary>
    [Benchmark(Description = "(1) control - admission over a 2-row frame - optimised")]
    public long AdmitTiny_Optimised()
        => LeafSnapshotCodec.TryAdmitForHydration(_tinyFrame, out _, out _, out var stateBytes, out _)
            ? stateBytes
            : -1;

    // ---------------------------------------------------------------- group 2

    /// <summary>512 lower-bound seeks, re-reading the header on every probe.</summary>
    [Benchmark(Description = "(2) 512 lower-bound seeks - baseline (header per probe)")]
    public int Seek_Baseline()
    {
        var total = 0;
        for (var i = 0; i < _probeKeys.Length; i++)
        {
            if (BaselineTryFindFirstRowAtOrAfter(_frame, _probeKeys[i], out var index))
            {
                total += index;
            }
        }

        return total;
    }

    /// <summary>512 lower-bound seeks against one hoisted header read.</summary>
    [Benchmark(Description = "(2) 512 lower-bound seeks - optimised (header hoisted)")]
    public int Seek_Optimised()
    {
        if (!LeafSnapshotCodec.TryReadHeader(_frame, out var rowCount, out var indexOffset))
        {
            return -1;
        }

        var total = 0;
        for (var i = 0; i < _probeKeys.Length; i++)
        {
            if (LeafSnapshotCodec.TryFindFirstRowAtOrAfter(
                    _frame, _probeKeys[i], rowCount, indexOffset, out var index))
            {
                total += index;
            }
        }

        return total;
    }

    /// <summary>Control: one seek, where the hoist amortises over a single search.</summary>
    [Benchmark(Description = "(2) control - one lower-bound seek - baseline")]
    public int SeekSingle_Baseline()
        => BaselineTryFindFirstRowAtOrAfter(_frame, _probeKeys[3], out var index) ? index : -1;

    /// <summary>Control: one seek, where the hoist amortises over a single search.</summary>
    [Benchmark(Description = "(2) control - one lower-bound seek - optimised")]
    public int SeekSingle_Optimised()
    {
        if (!LeafSnapshotCodec.TryReadHeader(_frame, out var rowCount, out var indexOffset))
        {
            return -1;
        }

        return LeafSnapshotCodec.TryFindFirstRowAtOrAfter(
            _frame, _probeKeys[3], rowCount, indexOffset, out var index)
            ? index
            : -1;
    }

    // ---------------------------------------------------------------- group 3

    /// <summary>A 64-row block hydration, re-reading the header per row.</summary>
    [Benchmark(Description = "(3) hydrate a 64-row block - baseline (header per row)")]
    public long BlockRead_Baseline()
    {
        long bytes = 0;
        for (var i = 0; i < BlockRows; i++)
        {
            if (LeafSnapshotCodec.TryReadRowAt(_frame, i, out _, out var consumed))
            {
                bytes += consumed;
            }
        }

        return bytes;
    }

    /// <summary>A 64-row block hydration against one hoisted header read.</summary>
    [Benchmark(Description = "(3) hydrate a 64-row block - optimised (header hoisted)")]
    public long BlockRead_Optimised()
    {
        if (!LeafSnapshotCodec.TryReadHeader(_frame, out var rowCount, out var indexOffset))
        {
            return -1;
        }

        long bytes = 0;
        for (var i = 0; i < BlockRows; i++)
        {
            if (LeafSnapshotCodec.TryReadRowAt(_frame, i, rowCount, indexOffset, out _, out var consumed))
            {
                bytes += consumed;
            }
        }

        return bytes;
    }

    /// <summary>Control: a single-row read, where one hoist amortises over one row.</summary>
    [Benchmark(Description = "(3) control - single row read - baseline")]
    public long SingleRowRead_Baseline()
        => LeafSnapshotCodec.TryReadRowAt(_frame, RowCount / 2, out _, out var consumed) ? consumed : -1;

    /// <summary>Control: a single-row read, where one hoist amortises over one row.</summary>
    [Benchmark(Description = "(3) control - single row read - optimised")]
    public long SingleRowRead_Optimised()
    {
        if (!LeafSnapshotCodec.TryReadHeader(_frame, out var rowCount, out var indexOffset))
        {
            return -1;
        }

        return LeafSnapshotCodec.TryReadRowAt(_frame, RowCount / 2, rowCount, indexOffset, out _, out var consumed)
            ? consumed
            : -1;
    }

    // ---------------------------------------------------------------- group 4

    /// <summary>
    /// 512 key comparisons over all-ASCII keys, walking runes. This is the
    /// dominant cost of a leaf seek: the group 2 lanes call this once per
    /// binary-search step, so the trim is isolated here rather than inferred
    /// from a lane where the walk is mixed with frame reads.
    /// </summary>
    [Benchmark(Description = "(4) 512 ASCII key compares - baseline (rune walk)")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompare_Baseline() => WalkPairsBaseline(_asciiPairs);

    /// <summary>512 key comparisons over all-ASCII keys through the ASCII fast path.</summary>
    [Benchmark(Description = "(4) 512 ASCII key compares - optimised (divergence-first)")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompare_Optimised() => WalkPairsOptimised(_asciiPairs);

    /// <summary>
    /// Control: 3-byte ASCII keys, where the whole comparison is so short that
    /// the trim has the least to amortise over. A whole-key ASCII validity
    /// gate loses outright here; the divergence scan must not.
    /// </summary>
    [Benchmark(Description = "(4) control - 512 tiny (3-byte) ASCII key compares - baseline")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompareTiny_Baseline() => WalkPairsBaseline(_tinyAsciiPairs);

    /// <summary>Control: 3-byte ASCII keys through the divergence scan.</summary>
    [Benchmark(Description = "(4) control - 512 tiny (3-byte) ASCII key compares - optimised")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompareTiny_Optimised() => WalkPairsOptimised(_tinyAsciiPairs);

    /// <summary>
    /// Control: ASCII keys that differ at byte 0, so the comparison is decided
    /// immediately and there is no shared prefix to amortise a scan over. This
    /// is the lane a whole-key validity gate regresses hardest, because it
    /// must read both keys to the end before it can answer.
    /// </summary>
    [Benchmark(Description = "(4) control - 512 early-diverging ASCII key compares - baseline")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompareEarly_Baseline() => WalkPairsBaseline(_earlyDivergePairs);

    /// <summary>Control: ASCII keys differing at byte 0, through the divergence scan.</summary>
    [Benchmark(Description = "(4) control - 512 early-diverging ASCII key compares - optimised")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompareEarly_Optimised() => WalkPairsOptimised(_earlyDivergePairs);

    /// <summary>
    /// Control: multibyte keys whose difference falls on a non-ASCII byte, so
    /// the trim cannot answer and the rune walk runs anyway. This lane pays
    /// the scan for nothing and is the honest cost of the trim.
    /// </summary>
    [Benchmark(Description = "(4) control - 512 multibyte key compares - baseline")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompareMultibyte_Baseline() => WalkPairsBaseline(_multibytePairs);

    /// <summary>Control: multibyte keys, where the divergence scan declines and falls through.</summary>
    [Benchmark(Description = "(4) control - 512 multibyte key compares - optimised")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public int KeyCompareMultibyte_Optimised() => WalkPairsOptimised(_multibytePairs);

    // ---------------------------------------------------------------- group 5

    /// <summary>
    /// Cache accounting for a hydrated 64-row block, recomputing each key's
    /// UTF-8 length with <c>Encoding.UTF8.GetByteCount</c> - a second scan of
    /// every key byte, to recover a length the frame already stated and the
    /// decoder already used to slice the key out.
    /// </summary>
    [Benchmark(Description = "(5) 64-row block accounting - baseline (GetByteCount per key)")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public long Accounting_Baseline()
    {
        long bytes = 0;
        for (var i = 0; i < BlockRows; i++)
        {
            bytes += LeafEntryCache.EntryBytes(_accountingKeys[i], _accountingValues[i]);
        }

        return bytes;
    }

    /// <summary>Cache accounting carrying the decoder's exact UTF-8 key length.</summary>
    [Benchmark(Description = "(5) 64-row block accounting - optimised (carried UTF-8 length)")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public long Accounting_Optimised()
    {
        long bytes = 0;
        for (var i = 0; i < BlockRows; i++)
        {
            bytes += LeafEntryCache.EntryBytes(_accountingKeyUtf8Lengths[i], _accountingValues[i]);
        }

        return bytes;
    }

    /// <summary>
    /// Control: one row's accounting, where the saved scan cannot amortise
    /// over a block. A block hydration is the real shape, but a single-entry
    /// path exists too and must not be reported as if it moved with the block.
    /// </summary>
    [Benchmark(Description = "(5) control - single row accounting - baseline")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public long AccountingSingle_Baseline()
        => LeafEntryCache.EntryBytes(_accountingKeys[0], _accountingValues[0]);

    /// <summary>Control: one row's accounting through the carried length.</summary>
    [Benchmark(Description = "(5) control - single row accounting - optimised")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    public long AccountingSingle_Optimised()
        => LeafEntryCache.EntryBytes(_accountingKeyUtf8Lengths[0], _accountingValues[0]);

    // ------------------------------------------------------------- equivalence

    private void AssertAdmissionEquivalence()
    {
        foreach (var (frame, label) in new[] { (_frame, "primary frame"), (_tinyFrame, "two-row frame") })
        {
            var baseline = BaselineAdmit(frame);
            var admitted = LeafSnapshotCodec.TryAdmitForHydration(
                frame, out var rowCount, out var indexOffset, out var stateBytes, out var liveRows);
            if (!admitted || baseline != stateBytes)
            {
                throw new InvalidOperationException(
                    $"Admission lanes disagree over the {label}; the comparison would be void.");
            }

            if (!LeafSnapshotCodec.TryGetRowCount(frame, out var expectedRows)
                || expectedRows != rowCount
                || liveRows <= 0
                || indexOffset <= 0)
            {
                throw new InvalidOperationException(
                    $"Fused admission reported inconsistent metadata over the {label}.");
            }

            if (BaselineIsAscendingByKey(frame) != LeafSnapshotCodec.IsAscendingByKey(frame))
            {
                throw new InvalidOperationException(
                    $"Ascending-order lanes disagree over the {label}; the comparison would be void.");
            }
        }

        // A frame whose rows are not ascending must be refused identically by
        // both shapes, or the fused probe has quietly widened admission.
        var unsorted = LeafSnapshotCodec.Encode(
        [
            new LeafSnapshotRow("b", LwwValue<byte[]>.Create([1], HybridLogicalClock.Tick(HybridLogicalClock.Zero))),
            new LeafSnapshotRow("a", LwwValue<byte[]>.Create([2], HybridLogicalClock.Tick(HybridLogicalClock.Zero))),
        ]);
        if (BaselineIsAscendingByKey(unsorted)
            || LeafSnapshotCodec.IsAscendingByKey(unsorted)
            || LeafSnapshotCodec.TryAdmitForHydration(unsorted, out _, out _, out _, out _))
        {
            throw new InvalidOperationException("An unsorted frame was admitted for bounded hydration.");
        }

        // A truncated frame must be refused by both shapes rather than read.
        var truncated = _frame.AsSpan(0, _frame.Length - 1).ToArray();
        if (BaselineAdmit(truncated) != -1
            || LeafSnapshotCodec.TryAdmitForHydration(truncated, out _, out _, out _, out _))
        {
            throw new InvalidOperationException("A truncated frame was admitted for bounded hydration.");
        }
    }

    private void AssertSeekEquivalence()
    {
        if (!LeafSnapshotCodec.TryReadHeader(_frame, out var rowCount, out var indexOffset))
        {
            throw new InvalidOperationException("The benchmark corpus frame carries an unreadable header.");
        }

        foreach (var probe in _probeKeys)
        {
            var baselineFound = BaselineTryFindFirstRowAtOrAfter(_frame, probe, out var baselineIndex);
            var hoistedFound = LeafSnapshotCodec.TryFindFirstRowAtOrAfter(
                _frame, probe, rowCount, indexOffset, out var hoistedIndex);
            if (baselineFound != hoistedFound || baselineIndex != hoistedIndex)
            {
                throw new InvalidOperationException(
                    "Seek lanes disagree on a lower bound; the comparison would be void.");
            }
        }

        for (var i = 0; i < BlockRows; i++)
        {
            var baselineRead = LeafSnapshotCodec.TryReadRowAt(_frame, i, out var baselineRow, out var baselineBytes);
            var hoistedRead = LeafSnapshotCodec.TryReadRowAt(
                _frame, i, rowCount, indexOffset, out var hoistedRow, out var hoistedBytes);
            if (baselineRead != hoistedRead || baselineBytes != hoistedBytes || baselineRow.Key != hoistedRow.Key)
            {
                throw new InvalidOperationException(
                    "Row-read lanes disagree; the comparison would be void.");
            }
        }

        // Out-of-range indices must be refused identically by both shapes.
        if (LeafSnapshotCodec.TryReadRowAt(_frame, rowCount, out _, out _)
            || LeafSnapshotCodec.TryReadRowAt(_frame, rowCount, rowCount, indexOffset, out _, out _)
            || LeafSnapshotCodec.TryReadRowAt(_frame, -1, rowCount, indexOffset, out _, out _))
        {
            throw new InvalidOperationException("An out-of-range row index was accepted.");
        }
    }

    // ------------------------------------------------------------------ corpora

    private static LeafSnapshotRow[] BuildRows(int count, int valueBytes, bool wide)
    {
        var rows = new LeafSnapshotRow[count];
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        for (var i = 0; i < count; i++)
        {
            var ordinal = i.ToString("D6", CultureInfo.InvariantCulture);
            var key = wide ? "\u00e9\u4e2d\ud83d\ude80key:" + ordinal : "key:" + ordinal;

            var value = LwwValue<byte[]>.Create(valueBytes == 0 ? [] : new byte[valueBytes], clock) with
            {
                OriginClusterId = "cluster-" + (i % 4).ToString(CultureInfo.InvariantCulture),
                VectorClock = BuildClock(i),
            };

            rows[i] = new LeafSnapshotRow(key, value, i % 8 == 0 ? LatticeMergeMode.LwwRegister : null);
        }

        return rows;
    }

    private static LeafSnapshotRow[] BuildEdgeCaseRows()
    {
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

        // Ordinal order, because Encode documents ascending keys: the empty
        // string sorts first, then ASCII, then the multi-byte forms.
        return
        [
            new LeafSnapshotRow(string.Empty, LwwValue<byte[]>.Create([], clock)),
            new LeafSnapshotRow("ascii", LwwValue<byte[]>.Create([1, 2, 3], clock) with
            {
                OriginClusterId = "cluster-a",
            }),
            new LeafSnapshotRow("tombstone", LwwValue<byte[]>.Tombstone(clock), LatticeMergeMode.LwwRegister),
            new LeafSnapshotRow("\u00e9-multibyte", LwwValue<byte[]>.Create([4], clock) with
            {
                VectorClock = BuildClock(3),
            }),
            new LeafSnapshotRow("\ud83d\ude80-surrogate", LwwValue<byte[]>.Create(new byte[7], clock) with
            {
                OriginClusterId = "\u4e2d\u6587",
                VectorClock = BuildClock(1),
            }),
        ];
    }

    private static VersionVector BuildClock(int seed)
    {
        var vector = new VersionVector();
        var replicas = (seed % 3) + 1;
        for (var r = 0; r < replicas; r++)
        {
            vector.Tick("replica-" + r.ToString(CultureInfo.InvariantCulture));
        }

        return vector;
    }

    private static byte[][] BuildProbeKeys()
    {
        var probes = new byte[SeekProbes][];
        for (var i = 0; i < SeekProbes; i++)
        {
            probes[i] = i switch
            {
                // Below every key, above every key, and a between-keys probe:
                // the three shapes a lower-bound search must get right and the
                // ones a mid-search bug is most likely to survive.
                0 => Encoding.UTF8.GetBytes("aaa"),
                1 => Encoding.UTF8.GetBytes("zzz"),
                2 => Encoding.UTF8.GetBytes("key:000001a"),
                _ => Encoding.UTF8.GetBytes(
                    "key:" + ((i * 7) % RowCount).ToString("D6", CultureInfo.InvariantCulture)),
            };
        }

        return probes;
    }

    // ------------------------------------------------------------- baselines
    //
    // Verbatim copies of the code each trim replaced. They call the same
    // helpers the shipped code calls wherever those are unchanged, so each
    // baseline pays exactly the overhead its counterpart pays and the delta is
    // attributable to the trim rather than to a paraphrase.

    private static long BaselineAdmit(byte[] frame)
    {
        if (!LeafSnapshotCodec.TryGetRowCount(frame, out _)
            || !LeafSnapshotCodec.TryComputeCacheAggregates(frame, out var stateBytes, out _)
            || !BaselineIsAscendingByKey(frame))
        {
            return -1;
        }

        return stateBytes;
    }

    /// <summary>
    /// The ascending-order check as it stood: the header read once up front,
    /// and then again inside every per-row key probe.
    /// </summary>
    private static bool BaselineIsAscendingByKey(ReadOnlySpan<byte> frame)
    {
        if (!LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out _))
        {
            return false;
        }

        if (rowCount <= 1)
        {
            return true;
        }

        if (!LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, 0, out var previous))
        {
            return false;
        }

        for (var i = 1; i < rowCount; i++)
        {
            if (!LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, i, out var current)
                || LeafSnapshotCodec.CompareKeysUtf8(previous, current) >= 0)
            {
                return false;
            }

            previous = current;
        }

        return true;
    }

    /// <summary>
    /// The lower-bound search as it stood: the header read once up front, and
    /// then again inside every binary-search probe.
    /// </summary>
    private static bool BaselineTryFindFirstRowAtOrAfter(
        ReadOnlySpan<byte> frame, ReadOnlySpan<byte> keyUtf8, out int index)
    {
        index = 0;
        if (!LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out _))
        {
            return false;
        }

        var low = 0;
        var high = rowCount;
        while (low < high)
        {
            var mid = low + ((high - low) / 2);
            if (!LeafSnapshotCodec.TryReadRowKeyUtf8At(frame, mid, out var probe))
            {
                return false;
            }

            if (LeafSnapshotCodec.CompareKeysUtf8(probe, keyUtf8) < 0)
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

    // ------------------------------------------------- group 4/5 harness

    // The two walkers are deliberately identical in signature and shape, and
    // both are NoInlining. An asymmetry here (a differing parameter count, or
    // one side inlineable and the other not) makes the JIT's inlining decision
    // the thing being measured rather than the trim.

    /// <summary>Compares every pair with the pre-trim rune walk.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int WalkPairsBaseline(byte[][] pairs)
    {
        var acc = 0;
        for (var i = 0; i < pairs.Length; i += 2)
        {
            acc += BaselineCompareKeysUtf8(pairs[i], pairs[i + 1]);
        }

        return acc;
    }

    /// <summary>Compares every pair with the shipped comparator.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int WalkPairsOptimised(byte[][] pairs)
    {
        var acc = 0;
        for (var i = 0; i < pairs.Length; i += 2)
        {
            acc += LeafSnapshotCodec.CompareKeysUtf8(pairs[i], pairs[i + 1]);
        }

        return acc;
    }

    /// <summary>
    /// The key comparison as it stood before the ASCII gate: a rune walk over
    /// both sides, ranking each code point into UTF-16 ordinal order. Copied
    /// rather than called so the baseline cannot drift when the shipped
    /// comparator changes.
    /// </summary>
    private static int BaselineCompareKeysUtf8(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
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

            var cmp = BaselineUtf16OrdinalRank(leftRune.Value).CompareTo(BaselineUtf16OrdinalRank(rightRune.Value));
            if (cmp != 0)
            {
                return cmp;
            }

            leftRemaining = leftRemaining[leftConsumed..];
            rightRemaining = rightRemaining[rightConsumed..];
        }

        return leftRemaining.IsEmpty ? (rightRemaining.IsEmpty ? 0 : -1) : 1;
    }

    private static long BaselineUtf16OrdinalRank(int codePoint)
        => codePoint >= 0x10000
            ? (0xD800L << 16) + (codePoint - 0x10000)
            : (long)codePoint << 16;

    // ------------------------------------------------- group 4/5 corpora

    /// <summary>
    /// Builds <see cref="ComparePairs"/> ASCII key pairs that share a long
    /// common prefix, which is the realistic shape for a keyspace and the one
    /// that forces the comparison to run to the tail rather than settling on
    /// the first byte.
    /// </summary>
    private static byte[][] BuildAsciiPairs(int keyBytes)
    {
        var pairs = new byte[ComparePairs * 2][];
        for (var i = 0; i < ComparePairs; i++)
        {
            pairs[(i * 2) + 0] = MakeAsciiKey(i * 2, keyBytes);
            pairs[(i * 2) + 1] = MakeAsciiKey((i * 2) + 1, keyBytes);
        }

        return pairs;
    }

    private static byte[] MakeAsciiKey(int seed, int keyBytes)
    {
        var text = string.Create(
            CultureInfo.InvariantCulture,
            $"tenant/shard/key-{seed:D8}");

        if (text.Length > keyBytes)
        {
            text = text[^keyBytes..];
        }
        else if (text.Length < keyBytes)
        {
            text = text.PadRight(keyBytes, 'x');
        }

        return Encoding.UTF8.GetBytes(text);
    }

    /// <summary>
    /// Builds 24-byte ASCII pairs whose very first byte differs, so the
    /// comparison is decided immediately and no shared prefix exists.
    /// </summary>
    private static byte[][] BuildEarlyDivergePairs()
    {
        var pairs = new byte[ComparePairs * 2][];
        for (var i = 0; i < ComparePairs; i++)
        {
            var left = (char)('a' + (i % 13));
            var right = (char)('n' + (i % 13));
            pairs[(i * 2) + 0] = Encoding.UTF8.GetBytes(
                string.Create(CultureInfo.InvariantCulture, $"{left}enant/shard/key-{i:D6}"));
            pairs[(i * 2) + 1] = Encoding.UTF8.GetBytes(
                string.Create(CultureInfo.InvariantCulture, $"{right}enant/shard/key-{i:D6}"));
        }

        return pairs;
    }

    /// <summary>
    /// Multibyte counterparts to <see cref="BuildAsciiPairs"/>, mixing a
    /// 2-byte (Cyrillic), a 3-byte (CJK) and a 4-byte (supplementary) code
    /// point. Each pair is built so the <em>first differing byte</em> falls
    /// inside a multi-byte sequence, which is what forces the trim to decline
    /// and the rune walk to run - a pair differing only in a trailing ASCII
    /// digit would take the fast path and measure nothing.
    /// </summary>
    private static byte[][] BuildMultibytePairs()
    {
        // Two CJK characters that differ, placed after a shared multibyte
        // prefix, so divergence lands on a 3-byte sequence.
        const string Prefix = "\u0442\u0435\u0441\u0442/\U0001F600/";
        var pairs = new byte[ComparePairs * 2][];
        for (var i = 0; i < ComparePairs; i++)
        {
            pairs[(i * 2) + 0] = Encoding.UTF8.GetBytes(
                string.Create(CultureInfo.InvariantCulture, $"{Prefix}\u6f22\u5b57-{i:D6}"));
            pairs[(i * 2) + 1] = Encoding.UTF8.GetBytes(
                string.Create(CultureInfo.InvariantCulture, $"{Prefix}\u6f22\u5b50-{i:D6}"));
        }

        return pairs;
    }

    /// <summary>
    /// Decodes the first <see cref="BlockRows"/> rows of the benchmark frame
    /// exactly as a hydration block does, keeping both the decoded string and
    /// the UTF-8 length the frame stated, so the accounting lanes compare the
    /// two ways of obtaining that same figure.
    /// </summary>
    private static void BuildAccountingCorpus(
        out string[] keys, out int[] keyUtf8Lengths, out byte[][] values)
    {
        var rows = BuildRows(RowCount, valueBytes: 32, wide: false);
        keys = new string[BlockRows];
        keyUtf8Lengths = new int[BlockRows];
        values = new byte[BlockRows][];

        for (var i = 0; i < BlockRows; i++)
        {
            keys[i] = rows[i].Key;
            keyUtf8Lengths[i] = Encoding.UTF8.GetByteCount(rows[i].Key);
            values[i] = rows[i].Value.Value ?? [];
        }
    }

    // --------------------------------------------- group 4/5 equivalence

    /// <summary>
    /// Asserts the ASCII gate is a pure fast path: it must agree with the rune
    /// walk in sign on every corpus, including the mixed case (one side ASCII,
    /// the other not) where the gate must decline, and on malformed UTF-8,
    /// where the trim must not have moved the validity fallback.
    /// </summary>
    private void AssertKeyCompareEquivalence()
    {
        AssertPairsAgree(_asciiPairs, nameof(_asciiPairs));
        AssertPairsAgree(_tinyAsciiPairs, nameof(_tinyAsciiPairs));
        AssertPairsAgree(_earlyDivergePairs, nameof(_earlyDivergePairs));
        AssertPairsAgree(_multibytePairs, nameof(_multibytePairs));

        // The multibyte corpus only measures the fall-through if its pairs
        // really do diverge on a non-ASCII byte. Assert that, or the lane
        // silently becomes a second copy of the ASCII lane.
        for (var i = 0; i < _multibytePairs.Length; i += 2)
        {
            var left = _multibytePairs[i];
            var right = _multibytePairs[i + 1];
            var common = left.AsSpan().CommonPrefixLength(right);
            if (common == left.Length || common == right.Length || (left[common] < 0x80 && right[common] < 0x80))
            {
                throw new InvalidOperationException(
                    $"Multibyte pair {i / 2} diverges on an ASCII byte, so it does not exercise the rune walk.");
            }
        }

        // Mixed: the gate must decline whenever either side is non-ASCII, or
        // the two orders diverge exactly where the rune walk exists to correct.
        var ascii = Encoding.UTF8.GetBytes("key-000001");
        var supplementary = Encoding.UTF8.GetBytes("key-\U0001F600");
        var bmpHigh = Encoding.UTF8.GetBytes("key-\uF8FF");
        AssertSameSign(ascii, supplementary, "ascii vs supplementary");
        AssertSameSign(supplementary, ascii, "supplementary vs ascii");
        AssertSameSign(supplementary, bmpHigh, "supplementary vs private-use BMP");
        AssertSameSign(bmpHigh, supplementary, "private-use BMP vs supplementary");

        // Malformed input: a lone continuation byte and a truncated sequence
        // must still produce the baseline's answer rather than throwing.
        var malformed = new byte[] { 0x6B, 0x80, 0x41 };
        var truncated = new byte[] { 0x6B, 0xE6, 0xBC };
        AssertSameSign(malformed, ascii, "malformed vs ascii");
        AssertSameSign(ascii, malformed, "ascii vs malformed");
        AssertSameSign(truncated, malformed, "truncated vs malformed");
        AssertSameSign(malformed, truncated, "malformed vs truncated");
    }

    private static void AssertPairsAgree(byte[][] pairs, string corpus)
    {
        for (var i = 0; i < pairs.Length; i += 2)
        {
            AssertSameSign(pairs[i], pairs[i + 1], corpus);
        }
    }

    private static void AssertSameSign(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, string what)
    {
        var baseline = Math.Sign(BaselineCompareKeysUtf8(left, right));
        var optimised = Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(left, right));
        if (baseline != optimised)
        {
            throw new InvalidOperationException(
                $"CompareKeysUtf8 disagrees with the rune walk on {what}: "
                + $"baseline {baseline}, optimised {optimised}.");
        }
    }

    /// <summary>
    /// Asserts the carried UTF-8 length is the same figure
    /// <c>Encoding.UTF8.GetByteCount</c> recomputes, per entry and in total.
    /// </summary>
    private void AssertAccountingEquivalence()
    {
        for (var i = 0; i < BlockRows; i++)
        {
            var baseline = LeafEntryCache.EntryBytes(_accountingKeys[i], _accountingValues[i]);
            var optimised = LeafEntryCache.EntryBytes(_accountingKeyUtf8Lengths[i], _accountingValues[i]);
            if (baseline != optimised)
            {
                throw new InvalidOperationException(
                    $"Entry accounting diverges at row {i}: baseline {baseline}, optimised {optimised}.");
            }
        }

        if (Accounting_Baseline() != Accounting_Optimised()
            || AccountingSingle_Baseline() != AccountingSingle_Optimised())
        {
            throw new InvalidOperationException("Block accounting totals diverge.");
        }
    }
}
