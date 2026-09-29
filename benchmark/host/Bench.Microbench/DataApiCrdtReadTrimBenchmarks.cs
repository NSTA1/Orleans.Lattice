using System;
using System.Collections.Generic;

using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three CRDT whole-collection projections the external data-plane
/// API runs on every read - <c>LatticeDataApi.SetGetAsync</c>,
/// <c>LatticeDataApi.MapGetAsync</c>, and <c>LatticeDataApi.RwSetGetAsync</c> -
/// so the time and byte deltas are measurable in the clear rather than buried
/// under a silo, a grain call, and a transport.
/// <para>
/// (1) <b>Observed-remove set whole-set read.</b> The prior shape was
/// <c>[.. set.Elements()]</c>. <c>OrSet.Elements()</c> is a <c>yield return</c>
/// iterator, so it hides its element count from the collection expression: the
/// builder cannot size the destination and instead fills a chain of segments,
/// then copies the whole projection once more into the final array - on top of
/// the iterator state machine. The shipped shape is the new internal
/// <c>OrSet.SnapshotElements()</c>: one survivor scan feeding one exactly-sized
/// array. Sizing from <c>OrSet.Count</c> instead would be no better, because
/// <c>Count</c> re-runs the identical tombstone survivor scan - the contrast
/// lane measures that shape so "derive the bound, but never by repeating the
/// work" is evidenced rather than asserted.
/// </para>
/// <para>
/// (2) <b>Observed-remove map whole-map read.</b> This is the group with an
/// algorithmic delta rather than a builder one. The prior shape walked the map
/// <em>twice</em>: <c>OrMap.Keys()</c> walks <c>Adds</c> and runs a tombstone
/// probe per key to decide liveness and sorts the survivors, and then
/// <c>OrMap.Get(field)</c> repeats both the <c>Adds</c> lookup and the tombstone
/// probe for every key the first pass had just kept. The sort was then thrown
/// away, because the caller folds the result into an unordered
/// <see cref="Dictionary{TKey, TValue}"/> - which was itself unpresized, so it
/// rehashed its way up to the field count. The shipped shape is the new internal
/// <c>OrMap.SnapshotLiveEntriesUnordered()</c>: one walk, no sort, and the
/// liveness test <em>is</em> the merge (a fully-tombstoned key folds to
/// <c>null</c> and is dropped), so the separate probe disappears entirely and
/// the exact live count presizes the destination dictionary.
/// </para>
/// <para>
/// (3) <b>Remove-wins set whole-set read.</b> The same hidden-count problem as
/// (1), at the data-plane seam. It is fixed by <c>RwSet.SnapshotElements()</c>,
/// which already existed - it was introduced for <c>RwSetAccessor</c> in the
/// <c>crdtreadtrio</c> suite and was simply not reachable from this assembly,
/// which had no <c>InternalsVisibleTo</c> grant. This lane is therefore a
/// second call site for a shipped primitive rather than a new one, and is
/// measured here because the data-plane call site is a different read path with
/// its own traffic.
/// </para>
/// <para>
/// Read groups (1) and (3) for <b>time</b>, not bytes: the prior shapes fill
/// their intermediate segments from <c>ArrayPool</c>, so what the shipped shape
/// removes is the segment bookkeeping, the extra whole-projection copy, and the
/// iterator state machine - real work, but nearly invisible on the heap next to
/// the decoded elements, which dominate every lane equally. Group (2) is the one
/// that moves bytes, because the discarded sort list and the dictionary's
/// rehash chain are both plain heap allocations.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=dataapicrdtreads</c> (or
/// <c>--suite dataapicrdtreads</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class DataApiCrdtReadTrimBenchmarks
{
    // Large enough that the builder's doubling chain is several links long and
    // the dictionary rehashes repeatedly, and representative of a tag set, a
    // membership list, or a document read once per request.
    private const int ElementCount = 256;
    private const int FieldCount = 128;
    private const int ElementBytes = 24;
    private const string ReplicaId = "replica-a";

    private OrSet _orset = null!;
    private RwSet _rwset = null!;
    private OrMap<string, MvRegister> _ormap = null!;

    /// <summary>Builds the three CRDT snapshots the lanes project.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _orset = new OrSet();
        _rwset = new RwSet();
        _ormap = new OrMap<string, MvRegister>();

        for (var i = 0; i < ElementCount; i++)
        {
            var element = MakeElement(i);
            _orset.Add(element, ReplicaId, i + 1);
            _rwset.Add(element, ReplicaId, i + 1);
        }

        // A realistic set is not pristine: tombstone a slice so the survivor scan
        // actually has removes to probe, which is the case where a naive
        // presize-from-Count would pay for the whole walk twice.
        for (var i = 0; i < ElementCount; i += 8)
        {
            _orset.Remove(MakeElement(i));
            _rwset.Remove(MakeElement(i), ReplicaId, ElementCount + i + 1);
        }

        for (var i = 0; i < FieldCount; i++)
        {
            var register = new MvRegister();
            register.Set(ReplicaId, MakeElement(i));
            _ormap.Set(FieldName(i), ReplicaId, register);
        }
    }

    private static string FieldName(int index) => $"field-{index:D4}";

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
    // (1) observed-remove set whole-set read - SetGetAsync
    // ========================================================================

    /// <summary>
    /// The prior shape: materialise the iterator into a collection expression
    /// that cannot size its destination. Reproduced here because the production
    /// method no longer contains it; the survivor set it projects is identical.
    /// </summary>
    [Benchmark]
    public int OrSetRead_Baseline_CollectionExpressionOverIterator()
    {
        byte[][] values = [.. _orset.Elements()];
        return values.Length;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>OrSet.SnapshotElements</c>, which runs the tombstone survivor scan
    /// once and sizes the destination from its result.
    /// </summary>
    [Benchmark]
    public int OrSetRead_Optimized_RealSnapshotElements() => _orset.SnapshotElements().Length;

    /// <summary>
    /// The contrast lane that was <i>not</i> shipped: size the destination from
    /// <c>OrSet.Count</c>, which is a correct exact bound but re-runs the whole
    /// tombstone survivor scan to obtain it.
    /// </summary>
    [Benchmark]
    public int OrSetRead_Contrast_PresizeByRescanning()
    {
        var values = new byte[_orset.Count][];
        var i = 0;
        foreach (var element in _orset.Elements())
        {
            values[i++] = element;
        }

        return values.Length;
    }

    // ========================================================================
    // (2) observed-remove map whole-map read - MapGetAsync
    // ========================================================================

    /// <summary>
    /// The prior shape: enumerate the live keys (walk one, with a sort the
    /// caller discards), then re-resolve every one of them through
    /// <c>Get</c> (walk two), folding into an unpresized dictionary.
    /// Reproduced here because the production method no longer contains it; the
    /// dictionary it produces is equal to the optimized lane's.
    /// </summary>
    [Benchmark]
    public int MapRead_Baseline_KeysThenGetPerKey()
    {
        var result = new Dictionary<string, IReadOnlyList<byte[]>>();
        foreach (var field in _ormap.Keys())
        {
            var register = _ormap.Get(field);
            result[field] = register is null ? Array.Empty<byte[]>() : register.Values();
        }

        return result.Count;
    }

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>OrMap.SnapshotLiveEntriesUnordered</c> feeding an exactly-presized
    /// dictionary - one walk, no discarded sort, no rehash chain.
    /// </summary>
    [Benchmark]
    public int MapRead_Optimized_RealSingleWalkSnapshot()
    {
        var live = _ormap.SnapshotLiveEntriesUnordered();
        var result = new Dictionary<string, IReadOnlyList<byte[]>>(live.Length);
        foreach (var (field, register) in live)
        {
            result[field] = register.Values();
        }

        return result.Count;
    }

    /// <summary>
    /// The contrast lane that was <i>not</i> shipped: keep the two-walk
    /// <c>Keys</c>/<c>Get</c> shape but presize the dictionary from
    /// <c>OrMap.Count</c>. It separates the dictionary's rehash chain from the
    /// duplicated map walk, and shows that <c>Count</c> is itself a third walk.
    /// </summary>
    [Benchmark]
    public int MapRead_Contrast_PresizeButStillWalkTwice()
    {
        var result = new Dictionary<string, IReadOnlyList<byte[]>>(_ormap.Count);
        foreach (var field in _ormap.Keys())
        {
            var register = _ormap.Get(field);
            result[field] = register is null ? Array.Empty<byte[]>() : register.Values();
        }

        return result.Count;
    }

    // ========================================================================
    // (3) remove-wins set whole-set read - RwSetGetAsync
    // ========================================================================

    /// <summary>
    /// The prior shape at the data-plane seam: the same collection expression
    /// over a hidden-count survivor iterator. Reproduced here because the
    /// production method no longer contains it.
    /// </summary>
    [Benchmark]
    public int RwSetRead_Baseline_CollectionExpressionOverIterator()
    {
        byte[][] values = [.. _rwset.Elements()];
        return values.Length;
    }

    /// <summary>
    /// The shipped shape: <c>RwSet.SnapshotElements</c>, now reachable from the
    /// data-plane assembly, running the remove/tombstone survivor scan once.
    /// </summary>
    [Benchmark]
    public int RwSetRead_Optimized_RealSnapshotElements() => _rwset.SnapshotElements().Length;
}
