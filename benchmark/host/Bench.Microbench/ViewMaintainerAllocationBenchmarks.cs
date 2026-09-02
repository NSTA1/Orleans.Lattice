using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state allocation trims made to the materialised-view
/// maintainer's warm cross-tree / batch-coalesce paths in <c>Orleans.Lattice</c>
/// so their per-operation byte deltas are measurable in the clear. As with the
/// sibling <see cref="DictionaryAllocationTrimBenchmarks"/> suite, the full
/// end-to-end cluster benchmarks route each operation through Orleans
/// serialization, persistence, and task machinery and allocate on the order of
/// tens of kilobytes per op, so a sub-kilobyte trim sits below their run-to-run
/// noise floor. Each benchmark below reproduces exactly one optimized code shape
/// against its prior shape with no cluster in the loop, so the <c>Allocated</c>
/// column is deterministic and the baseline-vs-optimized delta is precisely the
/// heap the production change removes.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) <c>ViewCatalog.All</c> - the prior form returned
/// <c>_views.Values.ToArray()</c>, copying the ConcurrentDictionary's snapshot
/// (a <see cref="ReadOnlyCollection{T}"/> over a freshly built <c>List</c>) into a
/// second array even though every caller only enumerates it; returning the
/// snapshot directly removes that array copy per call;
/// (2) <c>ViewMaintainerGrain.ComputeViewWaitSet</c> - the prior form allocated a
/// <c>HashSet</c> (plus its bucket and entry arrays) for the participant
/// membership test on every cross-tree batch; a threshold-gated ordinal linear
/// scan removes it on the small participant lists that dominate the path;
/// (3) <c>ViewWriteCoalescer.Coalesce</c> - the prior form grew the survivor
/// <c>List</c> and the key-index <c>Dictionary</c> from empty as it folded the
/// batch, reallocating their backing arrays; presizing both to the known batch
/// count removes that regrowth churn.
/// </para>
/// <para>
/// The <c>ViewCatalog</c> and <c>ComputeViewWaitSet</c> shapes are reproduced
/// (the catalog and the method are <see langword="internal"/> / <c>private</c>);
/// the <c>Coalesce</c> shapes reproduce the two buffer-allocation forms around the
/// identical fold body, so each pair's delta is exactly the allocation the
/// production edit removes. Run it via
/// <c>BENCH_MICROBENCH_SUITE=viewmaint</c> (or <c>--suite viewmaint</c>); see
/// <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast to
/// run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ViewMaintainerAllocationBenchmarks
{
    /// <summary>A minimal stand-in for the internal <c>ViewRegistration</c> record.</summary>
    private sealed record Reg(string ViewName, string SourceTreeId);

    // ---- (1)/(2) a populated catalog, exactly as ViewCatalog holds it ----
    private ConcurrentDictionary<string, Reg> _catalog = null!;

    // ---- (2) a small participant list, as a cross-tree batch carries ----
    private string[] _participants = null!;

    // ---- (3) a batch of distinct-key writes, worst case for list/dict regrowth ----
    private List<ViewWrite> _writes = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        const int catalogSize = 64;
        _catalog = new ConcurrentDictionary<string, Reg>(StringComparer.Ordinal);
        for (var i = 0; i < catalogSize; i++)
        {
            var name = "view-" + i.ToString("D4", CultureInfo.InvariantCulture);
            var source = "tree-" + (i % 16).ToString("D4", CultureInfo.InvariantCulture);
            _catalog[name] = new Reg(name, source);
        }

        // A cross-tree atomic batch spans a small number of participant source
        // trees in the common case - well within the linear-scan threshold.
        _participants = new[] { "tree-0000", "tree-0003", "tree-0007", "tree-0011" };

        const int batch = 256;
        _writes = new List<ViewWrite>(batch);
        for (var i = 0; i < batch; i++)
        {
            var key = "k-" + i.ToString("D5", CultureInfo.InvariantCulture);
            var hlc = new HybridLogicalClock { WallClockTicks = 1_000 + i, Counter = 0 };
            _writes.Add(ViewWrite.Upsert(key, new byte[] { (byte)i }, hlc));
        }
    }

    // ------------------------------------------------------------------
    // (1) Catalog snapshot enumeration
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>_views.Values.ToArray()</c> copies the ConcurrentDictionary
    /// snapshot into a second array that every caller only enumerates.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Catalog All: .Values.ToArray (baseline)")]
    public int CatalogAll_ToArray()
    {
        var all = _catalog.Values.ToArray();
        var count = 0;
        foreach (var reg in all)
        {
            if (reg.SourceTreeId.Length > 0)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Optimized: returning the ConcurrentDictionary snapshot directly keeps the
    /// same moment-in-time, immutable enumeration and removes the array copy.
    /// </summary>
    [Benchmark(Description = "Catalog All: snapshot direct (optimized)")]
    public int CatalogAll_Direct()
    {
        var all = (IReadOnlyCollection<Reg>)_catalog.Values;
        var count = 0;
        foreach (var reg in all)
        {
            if (reg.SourceTreeId.Length > 0)
            {
                count++;
            }
        }

        return count;
    }

    // ------------------------------------------------------------------
    // (2) Cross-tree view wait-set
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: a <c>HashSet</c> for the participant membership test allocates
    /// the set plus its bucket and entry arrays on every cross-tree batch.
    /// </summary>
    [Benchmark(Description = "Wait set: HashSet participants (baseline)")]
    public int WaitSet_HashSet()
    {
        var participantSet = new HashSet<string>(_participants, StringComparer.Ordinal);
        var waitSet = new List<string>();
        foreach (var reg in (IReadOnlyCollection<Reg>)_catalog.Values)
        {
            if (participantSet.Contains(reg.SourceTreeId))
            {
                waitSet.Add(reg.ViewName);
            }
        }

        waitSet.Sort(StringComparer.Ordinal);
        return waitSet.Count;
    }

    /// <summary>
    /// Optimized: an ordinal linear scan over the small participant list answers
    /// the identical membership test with no <c>HashSet</c>.
    /// </summary>
    [Benchmark(Description = "Wait set: linear scan participants (optimized)")]
    public int WaitSet_LinearScan()
    {
        const int threshold = 8;
        HashSet<string>? participantSet = _participants.Length > threshold
            ? new HashSet<string>(_participants, StringComparer.Ordinal)
            : null;
        var waitSet = new List<string>();
        foreach (var reg in (IReadOnlyCollection<Reg>)_catalog.Values)
        {
            var isParticipant = participantSet is not null
                ? participantSet.Contains(reg.SourceTreeId)
                : ContainsOrdinal(_participants, reg.SourceTreeId);
            if (isParticipant)
            {
                waitSet.Add(reg.ViewName);
            }
        }

        waitSet.Sort(StringComparer.Ordinal);
        return waitSet.Count;
    }

    private static bool ContainsOrdinal(IReadOnlyList<string> items, string value)
    {
        for (var i = 0; i < items.Count; i++)
        {
            if (string.Equals(items[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    // ------------------------------------------------------------------
    // (3) Batch write coalescing
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the survivor list and key index grow from empty, reallocating
    /// their backing arrays as the batch is folded.
    /// </summary>
    [Benchmark(Description = "Coalesce: grow from empty (baseline)")]
    public int Coalesce_GrowFromEmpty()
    {
        var index = new Dictionary<string, int>(StringComparer.Ordinal);
        var survivors = new List<ViewWrite>();
        Fold(_writes, index, survivors);
        return survivors.Count;
    }

    /// <summary>
    /// Optimized: presizing both buffers to the known batch count removes the
    /// regrowth churn; the fold body is byte-identical.
    /// </summary>
    [Benchmark(Description = "Coalesce: presized buffers (optimized)")]
    public int Coalesce_Presized()
    {
        var capacity = _writes.TryGetNonEnumeratedCount(out var count) ? count : 0;
        var index = new Dictionary<string, int>(capacity, StringComparer.Ordinal);
        var survivors = new List<ViewWrite>(capacity);
        Fold(_writes, index, survivors);
        return survivors.Count;
    }

    // The identical last-writer-wins fold used by both coalesce lanes, mirroring
    // ViewWriteCoalescer.Coalesce so the only difference between the pair is the
    // buffer capacity.
    private static void Fold(List<ViewWrite> writes, Dictionary<string, int> index, List<ViewWrite> survivors)
    {
        foreach (var write in writes)
        {
            if (index.TryGetValue(write.Key, out var existingPos))
            {
                if (write.Timestamp.CompareTo(survivors[existingPos].Timestamp) > 0)
                {
                    survivors[existingPos] = write;
                }
            }
            else
            {
                index[write.Key] = survivors.Count;
                survivors.Add(write);
            }
        }
    }
}
