using System.Collections.Generic;
using System.Globalization;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state allocation trims made to the view-maintainer
/// drain path (<c>Orleans.Lattice</c>) and the receiver-side causal-apply buffer
/// (<c>Orleans.Lattice.Replication</c>) so their per-operation byte deltas are
/// measurable in the clear. As with the sibling
/// <see cref="ViewMaintainerAllocationBenchmarks"/> suite, the full end-to-end
/// cluster benchmarks route each operation through Orleans serialization,
/// persistence, and task machinery and allocate on the order of tens of kilobytes
/// per op, so a sub-kilobyte trim sits below their run-to-run noise floor. Each
/// benchmark below reproduces exactly one optimized code shape against its prior
/// shape with no cluster in the loop, so the <c>Allocated</c> column is
/// deterministic and the baseline-vs-optimized delta is precisely the heap the
/// production change removes.
/// <para>
/// The pairs mirror the production edits verbatim:
/// (1) <c>ViewKeyCollisionDetector.Detect</c> - the prior form grew the
/// first-source <c>Dictionary</c> from empty as it walked every drain batch (the
/// detector runs unconditionally per drain); presizing it to the known batch count
/// removes the grow / rehash chain. The colliding list and set stay unpresized in
/// both lanes because a well-configured injective re-key never collides, so they
/// never allocate a backing store on the common path;
/// (2) <c>ViewMaintainerGrain.FlushCompletedFilterBatchesAsync</c> - the prior form
/// grew the <c>upserts</c> and <c>deletes</c> lists from empty while partitioning
/// the coalesced survivors; the two lists partition the survivors, so presizing
/// both to the coalesced count removes their regrowth churn;
/// (3) <c>CausalApplyBuffer.TryAdd</c> - the prior form allocated a fresh empty
/// eviction <c>List</c> on every add even though steady state never evicts; handing
/// back a shared empty sentinel and materialising a real list only when an eviction
/// actually occurs removes that per-add allocation from the hot receiver path.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=draintrims</c> (or
/// <c>--suite draintrims</c>); see <c>Program.cs</c>. The suite has no Orleans silo
/// dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for
/// tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class DrainAllocationTrimBenchmarks
{
    // ---- (1) a batch of injective (distinct source key) writes: the common
    //      collision-free drain the detector walks unconditionally ----
    private List<ViewWrite> _attributed = null!;

    // ---- (2) a coalesced survivor batch (mixed upserts + deletes) to partition ----
    private List<ViewWrite> _coalesced = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        const int batch = 256;

        _attributed = new List<ViewWrite>(batch);
        for (var i = 0; i < batch; i++)
        {
            var key = "k-" + i.ToString("D5", CultureInfo.InvariantCulture);
            var source = "s-" + i.ToString("D5", CultureInfo.InvariantCulture);
            var hlc = new HybridLogicalClock { WallClockTicks = 1_000 + i, Counter = 0 };
            _attributed.Add(ViewWrite.Upsert(key, new byte[] { (byte)i }, hlc, sourceKey: source));
        }

        _coalesced = new List<ViewWrite>(batch);
        for (var i = 0; i < batch; i++)
        {
            var key = "v-" + i.ToString("D5", CultureInfo.InvariantCulture);
            var hlc = new HybridLogicalClock { WallClockTicks = 2_000 + i, Counter = 0 };
            // A realistic drain is upsert-dominant with a minority of retracting
            // deletes; the partition presize covers both lists regardless of split.
            _coalesced.Add(i % 8 == 0
                ? ViewWrite.Delete(key, hlc)
                : ViewWrite.Upsert(key, new byte[] { (byte)i }, hlc));
        }
    }

    // ------------------------------------------------------------------
    // (1) Collision-detector first-source map
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the first-source map grows from empty as the unconditional
    /// per-drain detector walks the batch, reallocating its backing arrays.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Detect: grow first-source from empty (baseline)")]
    public int Detect_GrowFromEmpty()
    {
        var firstSource = new Dictionary<string, string>(StringComparer.Ordinal);
        return DetectFold(_attributed, firstSource);
    }

    /// <summary>
    /// Optimized: presizing the first-source map to the known batch count removes
    /// the grow / rehash chain; the fold body is identical.
    /// </summary>
    [Benchmark(Description = "Detect: presized first-source (optimized)")]
    public int Detect_Presized()
    {
        var capacity = _attributed.TryGetNonEnumeratedCount(out var count) ? count : 0;
        var firstSource = new Dictionary<string, string>(capacity, StringComparer.Ordinal);
        return DetectFold(_attributed, firstSource);
    }

    // The identical first-seen fold used by both detect lanes, mirroring
    // ViewKeyCollisionDetector.Detect so the only difference between the pair is
    // the first-source map capacity. The colliding list/set stay unpresized (and
    // never allocate a backing store on this collision-free batch), exactly as in
    // production.
    private static int DetectFold(List<ViewWrite> writes, Dictionary<string, string> firstSource)
    {
        var colliding = new List<string>();
        var collidingSet = new HashSet<string>(StringComparer.Ordinal);
        foreach (var write in writes)
        {
            if (write.SourceKey is not { } source)
            {
                continue;
            }

            if (!firstSource.TryGetValue(write.Key, out var existing))
            {
                firstSource[write.Key] = source;
                continue;
            }

            if (!string.Equals(existing, source, StringComparison.Ordinal)
                && collidingSet.Add(write.Key))
            {
                colliding.Add(write.Key);
            }
        }

        return colliding.Count;
    }

    // ------------------------------------------------------------------
    // (2) Atomic-staging upsert / delete partition
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the upsert and delete lists grow from empty as the coalesced
    /// survivors are partitioned, reallocating their backing arrays.
    /// </summary>
    [Benchmark(Description = "Partition: grow upserts/deletes from empty (baseline)")]
    public int Partition_GrowFromEmpty()
    {
        var upserts = new List<KeyValuePair<string, byte[]>>();
        var deletes = new List<string>();
        Partition(_coalesced, upserts, deletes);
        return upserts.Count + deletes.Count;
    }

    /// <summary>
    /// Optimized: presizing both lists to the coalesced count removes the regrowth
    /// churn; the partition body is identical.
    /// </summary>
    [Benchmark(Description = "Partition: presized upserts/deletes (optimized)")]
    public int Partition_Presized()
    {
        var upserts = new List<KeyValuePair<string, byte[]>>(_coalesced.Count);
        var deletes = new List<string>(_coalesced.Count);
        Partition(_coalesced, upserts, deletes);
        return upserts.Count + deletes.Count;
    }

    // The identical partition body used by both lanes, mirroring the switch in
    // FlushCompletedFilterBatchesAsync so the only difference is the list capacity.
    private static void Partition(
        List<ViewWrite> coalesced,
        List<KeyValuePair<string, byte[]>> upserts,
        List<string> deletes)
    {
        foreach (var write in coalesced)
        {
            switch (write.Kind)
            {
                case ViewWriteKind.Upsert:
                    upserts.Add(new KeyValuePair<string, byte[]>(write.Key, write.Value!));
                    break;
                case ViewWriteKind.Delete:
                    deletes.Add(write.Key);
                    break;
                default:
                    break;
            }
        }
    }

    // ------------------------------------------------------------------
    // (3) Causal-apply buffer eviction list
    // ------------------------------------------------------------------

    /// <summary>Shared empty sentinel mirroring <c>CausalApplyBuffer.EmptyEvicted</c>.</summary>
    private static readonly List<WalRecord> EmptyEvicted = new();

    // A burst of adds against a buffer that is well under its caps: the common
    // steady state, where no add evicts. The measured allocation is the eviction
    // list handed back per add.
    private const int AddBurst = 256;

    /// <summary>
    /// Baseline: each add allocates a fresh empty eviction list, discarded
    /// immediately because steady state never evicts.
    /// </summary>
    [Benchmark(Description = "Evicted: fresh empty list per add (baseline)")]
    public int Evicted_PerAddList()
    {
        var observed = 0;
        for (var i = 0; i < AddBurst; i++)
        {
            var evicted = new List<WalRecord>();
            observed += evicted.Count;
        }

        return observed;
    }

    /// <summary>
    /// Optimized: each no-eviction add hands back the shared empty sentinel, so the
    /// steady-state burst allocates nothing for the eviction list.
    /// </summary>
    [Benchmark(Description = "Evicted: shared empty sentinel (optimized)")]
    public int Evicted_SharedEmpty()
    {
        var observed = 0;
        for (var i = 0; i < AddBurst; i++)
        {
            var evicted = EmptyEvicted;
            observed += evicted.Count;
        }

        return observed;
    }
}
