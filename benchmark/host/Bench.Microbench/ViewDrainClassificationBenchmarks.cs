using System.Collections.Generic;
using System.Globalization;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the view-maintenance drain-classification trims made to
/// <c>Orleans.Lattice</c> so their per-drain time delta is measurable in the
/// clear. The view maintainer drains its source WAL continuously; every drain
/// folds a batch of projected writes (or aggregation contributions) into a
/// buffer, and the prior code then re-walked that whole buffer with additional
/// <c>List.Exists(...)</c> passes purely to classify it - "did this batch carry a
/// RangeReconcile / RangeDelete?". Because the fold loop already visits every
/// element, the classification is now recorded inline during the fold and the
/// separate passes are removed, so a freshly drained batch is walked once
/// instead of two or three times per drain.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) the filter drain (<c>ViewMaintainerGrain.DrainAsync</c> /
/// <c>ApplySurvivorsAsync</c>) removed <b>two</b> post-hoc scans - one
/// <c>collected.Exists(w =&gt; w.Kind == RangeReconcile)</c> and one
/// <c>collected.Exists(w =&gt; w.Kind == RangeDelete)</c> - folding both flags into
/// the drain loop;
/// (2) the aggregation drain
/// (<c>ViewMaintainerGrain.Aggregation.DrainAggregationAsync</c>) removed the
/// single <c>contributions.Exists(c =&gt; c.Kind == RangeReconcile)</c> scan the
/// same way.
/// </para>
/// <para>
/// Each lane mirrors the buffer element shape (a <see langword="readonly"/>
/// <see langword="record"/> <see langword="struct"/> carrying a <c>Kind</c> enum,
/// exactly as <c>ViewWrite</c> and <c>AggregationContribution</c> do) rather than
/// referencing the internal contribution type; the scan cost is identical. Both
/// lanes in a pair perform the mandatory fold, so the baseline-vs-optimized delta
/// is precisely the redundant post-hoc pass(es) the production change removes.
/// The <c>Allocated</c> column is equal within a pair by design: the buffer list
/// is built identically in both lanes and the removed <c>.Exists</c> scans use a
/// cached static delegate, so this is a pure CPU-cycle trim on a continuously
/// running maintenance path.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=viewdrain</c> (or <c>--suite viewdrain</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast
/// to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ViewDrainClassificationBenchmarks
{
    /// <summary>Mirrors the <c>Kind</c> discriminator on a drained buffer element.</summary>
    private enum DrainWriteKind
    {
        Upsert,
        Delete,
        RangeReconcile,
        RangeDelete,
    }

    /// <summary>
    /// Mirrors the shape of a drained buffer element (<c>ViewWrite</c> /
    /// <c>AggregationContribution</c> are <see langword="readonly"/>
    /// <see langword="record"/> <see langword="struct"/>s carrying a <c>Kind</c>).
    /// </summary>
    private readonly record struct DrainWrite(DrainWriteKind Kind, string Key);

    /// <summary>The drained batch size. 256 is the maintainer's default drain batch.</summary>
    [Params(256, 1024)]
    public int N;

    // A steady-state batch of point writes: no range operations, the case a
    // healthy drain sees on every pass, so the removed post-hoc scans always run
    // to the end of the buffer.
    private DrainWrite[] _batch = null!;

    /// <summary>Builds the steady-state point-write batch shared by the lanes.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _batch = new DrainWrite[N];
        for (var i = 0; i < N; i++)
        {
            var key = "v-" + i.ToString("D6", CultureInfo.InvariantCulture);
            _batch[i] = new DrainWrite((i & 1) == 0 ? DrainWriteKind.Upsert : DrainWriteKind.Delete, key);
        }
    }

    // ------------------------------------------------------------------
    // (1) Filter drain: fold + two post-hoc scans removed
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the fold populates the buffer, then two full
    /// <c>List.Exists(...)</c> passes re-walk it to classify RangeReconcile and
    /// RangeDelete.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Filter drain classify: fold + 2 post-hoc .Exists scans (baseline)")]
    public int FilterClassify_PostScan()
    {
        var buffer = new List<DrainWrite>(N);
        foreach (var write in _batch)
        {
            buffer.Add(write);
        }

        var hasRangeReconcile = buffer.Exists(static w => w.Kind == DrainWriteKind.RangeReconcile);
        var hasRangeDelete = buffer.Exists(static w => w.Kind == DrainWriteKind.RangeDelete);
        return buffer.Count + (hasRangeReconcile ? 1 : 0) + (hasRangeDelete ? 2 : 0);
    }

    /// <summary>
    /// Optimized: the fold records both classifications inline, so neither
    /// second pass over the buffer is needed.
    /// </summary>
    [Benchmark(Description = "Filter drain classify: inline flags during fold (optimized)")]
    public int FilterClassify_InlineFlags()
    {
        var buffer = new List<DrainWrite>(N);
        var hasRangeReconcile = false;
        var hasRangeDelete = false;
        foreach (var write in _batch)
        {
            buffer.Add(write);
            if (write.Kind == DrainWriteKind.RangeReconcile)
            {
                hasRangeReconcile = true;
            }
            else if (write.Kind == DrainWriteKind.RangeDelete)
            {
                hasRangeDelete = true;
            }
        }

        return buffer.Count + (hasRangeReconcile ? 1 : 0) + (hasRangeDelete ? 2 : 0);
    }

    // ------------------------------------------------------------------
    // (2) Aggregation drain: fold + one post-hoc scan removed
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the fold populates the buffer, then one full
    /// <c>List.Exists(...)</c> pass re-walks it to classify RangeReconcile.
    /// </summary>
    [Benchmark(Description = "Aggregation drain classify: fold + 1 post-hoc .Exists scan (baseline)")]
    public int AggregationClassify_PostScan()
    {
        var buffer = new List<DrainWrite>(N);
        foreach (var write in _batch)
        {
            buffer.Add(write);
        }

        return buffer.Count + (buffer.Exists(static w => w.Kind == DrainWriteKind.RangeReconcile) ? 1 : 0);
    }

    /// <summary>
    /// Optimized: the fold records the classification inline, so no second pass
    /// over the buffer is needed.
    /// </summary>
    [Benchmark(Description = "Aggregation drain classify: inline flag during fold (optimized)")]
    public int AggregationClassify_InlineFlag()
    {
        var buffer = new List<DrainWrite>(N);
        var hasRangeReconcile = false;
        foreach (var write in _batch)
        {
            buffer.Add(write);
            if (write.Kind == DrainWriteKind.RangeReconcile)
            {
                hasRangeReconcile = true;
            }
        }

        return buffer.Count + (hasRangeReconcile ? 1 : 0);
    }
}
