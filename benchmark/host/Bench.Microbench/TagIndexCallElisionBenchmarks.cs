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
/// (3) <b>Replacing a key's tags overlaps its add and its removal.</b>
/// <c>SetAsync</c> awaited the batched add before issuing the removal deletes.
/// The two touch disjoint rows, so after validation they now go out together.
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

    /// <summary>Tags a replacement removes (each costs a tag-major and a key-major delete).</summary>
    [Params(2)]
    public int RemovedTags { get; set; }

    /// <summary>Prints the per-lane hop counts once.</summary>
    [GlobalSetup]
    public void Setup()
    {
        Console.WriteLine(
            $"[tagindexcallelision] {Writes} handle writes: per-handle caches={Writes * 4} hops, shared caches={Writes + 3} hops");
        Console.WriteLine("[tagindexcallelision] eventual value+tags: with probe=3 hops, without=2 hops");
        Console.WriteLine(
            $"[tagindexcallelision] set with {RemovedTags} removed tags: serial critical path=4 hops, overlapped=3 hops");
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

    /// <summary>Shipped: the first handle pays them, the rest only write rows.</summary>
    [Benchmark]
    [BenchmarkCategory("handles")]
    public async Task<int> Handles_SharedCaches()
    {
        var hops = await FirstWriteToTree();
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

    /// <summary>Old: read, existence, batched add, then the removal deletes.</summary>
    [Benchmark]
    [BenchmarkCategory("set")]
    public async Task<int> Set_AddThenRemove()
    {
        await Hop();
        await Hop();
        await Hop();
        await Removals();
        return 3 + (2 * RemovedTags);
    }

    /// <summary>Shipped: read, existence, then the batched add and the removal deletes together.</summary>
    [Benchmark]
    [BenchmarkCategory("set")]
    public async Task<int> Set_AddAndRemoveOverlapped()
    {
        await Hop();
        await Hop();
        await Task.WhenAll(Hop(), Removals());
        return 3 + (2 * RemovedTags);
    }

    private static Task Hop() => SimulatedHop.Wait(HopMicroseconds);

    // Existence probe, covered marker write, reconcile schedule, row write.
    private static async Task<int> FirstWriteToTree()
    {
        await Hop();
        await Hop();
        await Hop();
        await Hop();
        return 4;
    }

    // The removal deletes already overlap one another (a stateless-worker router).
    private Task Removals()
    {
        var deletes = new Task[2 * RemovedTags];
        for (var i = 0; i < deletes.Length; i++)
        {
            deletes[i] = Hop();
        }

        return Task.WhenAll(deletes);
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
