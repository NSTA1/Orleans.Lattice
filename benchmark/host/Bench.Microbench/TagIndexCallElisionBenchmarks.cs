using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Evidence for three tag-index grain-call reductions (perf run 163), all in
/// <c>LatticeTagIndexContext</c>. Each lane replays the grain calls the path
/// issues, priced as a fixed simulated hop; the exact call counts are pinned by
/// <c>LatticeTagIndexRoundTripBatchingTests.CallElision</c> against the shipped
/// code.
/// <para>
/// (1) <b>Multi-tree handles share their caches.</b> Every
/// <c>ILatticeMultiTreeTagIndex.Tree(t)</c> handle started with empty caches, so
/// each one re-probed <c>TreeExistsAsync</c>, rewrote the covered-tree marker and
/// re-ensured the reconcile schedule before its row write. The handles now share
/// the index's caches, so only the first write to a tree pays them.
/// </para>
/// <para>
/// (2) <b>An eventual value+tags write skips the existence probe.</b> The value
/// write registers the tree, so the <c>TreeExistsAsync</c> the tag write issued
/// straight after it could only answer "registered".
/// </para>
/// <para>
/// (3) <b>The covered-tree marker rides in the first add's row batch.</b>
/// The first add to a tree wrote its rows with <c>SetManyAsync</c> and then the
/// marker with a separate <c>SetAsync</c>; the marker is now one more row in
/// that batch.
/// </para>
/// <para>
/// Run via <c>--suite tagindexcallelision</c>. No silo is needed.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class TagIndexCallElisionBenchmarks
{
    private const int HopMicroseconds = 200;

    /// <summary>Tag writes issued through fresh <c>Tree(t)</c> handles on one tree.</summary>
    [Params(8)]
    public int Writes { get; set; }

    /// <summary>Prints the per-lane hop counts once.</summary>
    [GlobalSetup]
    public void Setup()
    {
        Console.WriteLine(
            $"[tagindexcallelision] {Writes} handle writes: per-handle caches={Writes * 4} hops, shared caches={Writes + 2} hops");
        Console.WriteLine("[tagindexcallelision] eventual value+tags: with probe=3 hops, without=2 hops");
        Console.WriteLine("[tagindexcallelision] first add to a tree: separate marker=4 hops, marker in batch=3 hops");
    }

    /// <summary>Old: every handle pays existence, marker, schedule and the row write.</summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("handles")]
    public async Task<int> Handles_PerHandleCaches()
    {
        var hops = 0;
        for (var i = 0; i < Writes; i++)
        {
            hops += await FirstWriteToTree();
        }

        return hops;
    }

    /// <summary>Shipped: the first handle pays them (marker in its batch), the rest only write rows.</summary>
    [Benchmark]
    [BenchmarkCategory("handles")]
    public async Task<int> Handles_SharedCaches()
    {
        var hops = await FirstWriteToTreeMarkerFolded();
        for (var i = 1; i < Writes; i++)
        {
            await Hop();
            hops++;
        }

        return hops;
    }

    /// <summary>Old: value write, existence probe, row write.</summary>
    [Benchmark]
    [BenchmarkCategory("valuewrite")]
    public async Task<int> ValueWithTags_ProbeAfterWrite()
    {
        await Hop();
        await Hop();
        await Hop();
        return 3;
    }

    /// <summary>Shipped: value write, row write.</summary>
    [Benchmark]
    [BenchmarkCategory("valuewrite")]
    public async Task<int> ValueWithTags_NoProbe()
    {
        await Hop();
        await Hop();
        return 2;
    }

    /// <summary>Old: existence probe, row batch, separate covered-marker write, reconcile schedule.</summary>
    [Benchmark]
    [BenchmarkCategory("firstadd")]
    public Task<int> FirstAdd_SeparateMarker() => FirstWriteToTree();

    /// <summary>Shipped: existence probe, row batch carrying the marker, reconcile schedule.</summary>
    [Benchmark]
    [BenchmarkCategory("firstadd")]
    public Task<int> FirstAdd_MarkerInBatch() => FirstWriteToTreeMarkerFolded();

    private static Task Hop() => SimulatedHop.Wait(HopMicroseconds);

    // Existence probe, row write, covered marker write, reconcile schedule.
    private static async Task<int> FirstWriteToTree()
    {
        await Hop();
        await Hop();
        await Hop();
        await Hop();
        return 4;
    }

    // Existence probe, row write carrying the covered marker, reconcile schedule.
    private static async Task<int> FirstWriteToTreeMarkerFolded()
    {
        await Hop();
        await Hop();
        await Hop();
        return 3;
    }

    /// <summary>A fixed-duration simulated grain hop that does not depend on timer resolution.</summary>
    private static class SimulatedHop
    {
        public static async Task Wait(int microseconds)
        {
            await Task.Yield();
            var until = System.Diagnostics.Stopwatch.GetTimestamp()
                + (System.Diagnostics.Stopwatch.Frequency * microseconds / 1_000_000);
            var spin = new SpinWait();
            while (System.Diagnostics.Stopwatch.GetTimestamp() < until)
            {
                spin.SpinOnce(sleep1Threshold: -1);
            }
        }
    }
}
