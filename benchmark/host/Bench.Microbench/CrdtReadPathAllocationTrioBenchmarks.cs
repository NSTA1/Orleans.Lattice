using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three allocation and work reductions on CRDT <em>read</em> paths -
/// the projection each collection CRDT runs every time a caller materialises it
/// - so the byte and time deltas are measurable in the clear rather than buried
/// under a silo, a transport, and a storage provider.
/// <para>
/// (1) <c>GSetAccessor.ToListAsync</c>, the whole-set read for a grow-only set,
/// run once per read. The prior shape was <c>[.. set.Values()]</c>.
/// <c>GSet.Values()</c> is a <c>yield return</c> iterator, so it hides its
/// element count from the collection expression: the builder cannot size the
/// destination, and instead fills a chain of segments and copies the whole
/// projection once more into the final array, on top of the iterator state
/// machine itself - even though <c>GSet.Count</c> is exact and was sitting right
/// there. The shipped shape is <c>GSet.SnapshotValues()</c>: one exactly-sized
/// destination, no iterator. A third contrast lane measures sizing from
/// <c>Count</c> but still filling through the iterator (the shape the file's own
/// <c>FlattenElements</c> used to have), so the builder's contribution is
/// separated from the iterator's.
/// </para>
/// <para>
/// (2) <c>RwSetAccessor.ToListAsync</c>, the whole-set read for a remove-wins
/// set. Same hidden-count problem via <c>set.Elements().ToArray()</c>, but with
/// a twist that makes the obvious fix wrong: sizing from <c>RwSet.Count</c>
/// would re-run the identical survivor scan - the per-key remove and tombstone
/// probe - so the set would be walked twice to avoid one builder pass. The
/// shipped shape is <c>RwSet.SnapshotElements()</c>: <b>one</b> survivor scan
/// feeding one exactly-sized destination. A third contrast lane measures the
/// double-scan presize that was deliberately not shipped, so "derive the bound,
/// but never by repeating the work" is evidenced rather than asserted.
/// </para>
/// <para>
/// (3) <c>SequenceProvenanceDecoder.DecodeCurrentValue</c>, the provenance
/// projection behind an entry-level sequence read. It called
/// <c>Rga.ToList()</c>, which allocates a fresh <c>(OrSetDot, byte[])</c> tuple
/// array purely to hand the projection over - and the decoder then re-projects
/// every entry into a <c>CrdtMemberValue</c> on the next line, so that tuple
/// array is discarded immediately. The shipped shape reads the cached
/// <c>Rga.MaterializeShared()</c> view instead and writes straight into an
/// exactly-sized result array. The per-value copy <c>ToList</c> also performs is
/// <b>not</b> dropped - it is the buffer-ownership guard for a value that
/// escapes to a public caller - it simply moves to where the member is
/// populated. The optimized lane calls the <b>real production code</b>.
/// </para>
/// <para>
/// Read the two set groups for <b>time</b>, not bytes. Both prior shapes fill
/// their intermediate segments from <c>ArrayPool</c>, so what the shipped shape
/// removes is the segment bookkeeping, the extra whole-projection copy, and the
/// iterator state machine - real work, but nearly invisible on the heap next to
/// the decoded elements themselves, which dominate every lane equally. Group (3)
/// is the one whose intermediate is a plain heap array, and it is the one with a
/// byte delta to show.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtreadtrio</c> (or
/// <c>--suite crdtreadtrio</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtReadPathAllocationTrioBenchmarks
{
    // A set large enough that the doubling chain is several links long, and
    // representative of a tag set or a membership list read per request.
    private const int ElementCount = 256;
    private const int ElementBytes = 24;
    private const string ReplicaId = "replica-a";

    private GSet _gset = null!;
    private RwSet _rwset = null!;
    private Rga _rga = null!;
    private SequenceProvenanceDecoder _decoder = null!;

    /// <summary>Builds the three CRDT snapshots the lanes project.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _gset = new GSet();
        _rwset = new RwSet();
        _rga = new Rga();
        _decoder = new SequenceProvenanceDecoder();

        var parent = Rga.Root;
        for (var i = 0; i < ElementCount; i++)
        {
            var element = MakeElement(i);
            _gset.Add(element);
            _rwset.Add(element, ReplicaId, i + 1);
            parent = _rga.InsertAfter(parent, ReplicaId, element);
        }

        // A realistic remove-wins set is not pristine: tombstone a slice so the
        // survivor scan actually has removes to probe, which is the case where
        // a naive presize-from-Count would pay for the walk twice.
        for (var i = 0; i < ElementCount; i += 8)
        {
            _rwset.Remove(MakeElement(i), ReplicaId, ElementCount + i + 1);
        }

        // Warm the sequence's resolved-order cache once, so both sequence lanes
        // measure the projection handoff rather than a one-off traversal that
        // only the first lane to run would have been charged for.
        _ = _rga.ToList();
    }

    private static byte[] MakeElement(int index)
    {
        var element = new byte[ElementBytes];
        element[0] = (byte)(index & 0xFF);
        element[1] = (byte)((index >> 8) & 0xFF);
        for (var b = 2; b < ElementBytes; b++)
        {
            element[b] = (byte)(b + index);
        }

        return element;
    }

    // ========================================================================
    // (1) grow-only set whole-set read
    // ========================================================================

    /// <summary>
    /// The prior shape: materialise the iterator into a collection expression
    /// that cannot size its destination. Reproduced here because the production
    /// method no longer contains it; the projection it folds is byte-for-byte
    /// the same.
    /// </summary>
    [Benchmark]
    public int GSetRead_Baseline_CollectionExpressionOverIterator()
    {
        byte[][] values = [.. _gset.Values()];
        return values.Length;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b> <c>GSet.SnapshotValues</c>,
    /// which sizes the destination from the exact <c>Count</c> and skips the
    /// iterator entirely.
    /// </summary>
    [Benchmark]
    public int GSetRead_Optimized_RealSnapshotValues() => _gset.SnapshotValues().Length;

    /// <summary>
    /// The contrast lane that was <i>not</i> shipped: size the destination from
    /// <c>Count</c> but keep filling through the iterator. This is the shape the
    /// accessor's own <c>FlattenElements</c> already had, and it isolates how
    /// much of the win is the removed builder pass rather than the removed state
    /// machine.
    /// </summary>
    [Benchmark]
    public int GSetRead_Contrast_PresizeButStillIterate()
    {
        var values = new byte[_gset.Count][];
        var i = 0;
        foreach (var element in _gset.Values())
        {
            values[i++] = element;
        }

        return values.Length;
    }

    // ========================================================================
    // (2) remove-wins set whole-set read
    // ========================================================================

    /// <summary>
    /// The prior shape: <c>ToArray</c> over the survivor iterator, which cannot
    /// size its destination. Reproduced here because the production method no
    /// longer contains it; the survivor set it projects is identical.
    /// </summary>
    [Benchmark]
    public int RwSetRead_Baseline_ToArrayOverIterator() => _rwset.Elements().ToArray().Length;

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>RwSet.SnapshotElements</c>, which runs the survivor scan once and
    /// sizes the destination from its result.
    /// </summary>
    [Benchmark]
    public int RwSetRead_Optimized_RealSnapshotElements() => _rwset.SnapshotElements().Length;

    /// <summary>
    /// The contrast lane that was <i>not</i> shipped: size the destination from
    /// <c>RwSet.Count</c>, which is a correct exact bound but re-runs the whole
    /// remove/tombstone survivor scan to obtain it. Evidence that a derived
    /// bound is only worth taking when deriving it is cheaper than the work it
    /// avoids.
    /// </summary>
    [Benchmark]
    public int RwSetRead_Contrast_PresizeByRescanning()
    {
        var values = new byte[_rwset.Count][];
        var i = 0;
        foreach (var element in _rwset.Elements())
        {
            values[i++] = element;
        }

        return values.Length;
    }

    // ========================================================================
    // (3) sequence provenance projection
    // ========================================================================

    /// <summary>
    /// The prior shape: build the public tuple projection, then immediately
    /// re-project it into members and discard it. Reproduced here because the
    /// production method no longer contains it; the resolved order, the
    /// per-value ownership copy, and the emitted members are all identical to
    /// the optimized lane.
    /// </summary>
    [Benchmark]
    public int SequenceProvenance_Baseline_ViaPublicToList()
    {
        var live = _rga.ToList();
        if (live.Count == 0) return 0;

        var result = new List<CrdtMemberValue>(live.Count);
        for (var i = 0; i < live.Count; i++)
        {
            var (dot, value) = live[i];
            result.Add(new CrdtMemberValue
            {
                Element = value ?? Array.Empty<byte>(),
                ReplicaId = dot.ReplicaId,
                Ordinal = dot.Counter,
            });
        }

        return result.Count;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>SequenceProvenanceDecoder.DecodeCurrentValue</c>, reading the shared
    /// cached view and copying each value straight into the member it populates.
    /// </summary>
    [Benchmark]
    public int SequenceProvenance_Optimized_RealDecodeCurrentValue()
        => _decoder.DecodeCurrentValue(_rga).Count;
}
