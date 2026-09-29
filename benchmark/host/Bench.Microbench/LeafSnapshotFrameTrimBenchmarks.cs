using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO.Hashing;
using System.Text;

using BenchmarkDotNet.Attributes;

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

    private byte[] _frame = null!;
    private byte[] _tinyFrame = null!;

    private byte[][] _probeKeys = null!;

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
}
