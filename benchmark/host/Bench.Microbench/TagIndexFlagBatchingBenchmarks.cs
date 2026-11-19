using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Evidence for three tag-index grain-call reductions on the flag-mode write
/// paths (perf run 164), all in <c>LatticeTagIndexContext</c>. Each lane replays
/// the grain calls the path issues, priced as a fixed simulated hop; concurrent
/// calls are issued together, so a lane's wall-clock is its critical path. The
/// exact call counts are pinned by
/// <c>LatticeTagIndexRoundTripBatchingTests.FlagAdd</c> and
/// <c>.MirrorRepair</c> against the shipped code.
/// <para>
/// (1) <b>An OR-Flag add is one read and one delta batch.</b> Every membership
/// row and mirror was its own <c>OrFlag(row).EnableAsync</c>: a read plus a delta
/// apply, two calls per row. They now go through <c>EnableManyAsync</c>, one
/// <c>GetManyAsync</c> plus one <c>ApplyCrdtDeltaManyAsync</c> for the whole add,
/// with the covered marker folded in on a tree's first add.
/// </para>
/// <para>
/// (2) <b>The RW-Flag first add writes its marker in the row wave.</b> RW
/// enables stay per row, but the covered marker no longer follows the wave as a
/// serial read-and-apply.
/// </para>
/// <para>
/// (3) <b>Mirror repair batches its writes and overlaps its reads.</b> The
/// subject and index verification reads were serial; they are now issued
/// together. The repairs were one write per mirror (<c>SetAsync</c>, or a read
/// plus a delta apply under OR-Flag); they are now one <c>SetManyAsync</c> or one
/// <c>ApplyCrdtDeltaManyAsync</c> minted from the verification read.
/// </para>
/// <para>
/// Run via <c>--suite tagindexflagbatching</c>. No silo is needed.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class TagIndexFlagBatchingBenchmarks
{
    private const int HopMicroseconds = 200;

    /// <summary>Tags written by one add.</summary>
    [Params(4)]
    public int Tags { get; set; }

    /// <summary>Missing mirrors repaired by one reconcile window.</summary>
    [Params(8)]
    public int Mirrors { get; set; }

    /// <summary>Prints the per-lane call counts once.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var rows = Tags * 2;
        Console.WriteLine(
            $"[tagindexflagbatching] or-flag add ({Tags} tags): per-row enables={rows * 2} calls, batched=2 calls");
        Console.WriteLine(
            $"[tagindexflagbatching] or-flag first add: {(rows * 2) + 3} calls -> 3 calls; rw-flag first add critical path 5 -> 3 hops");
        Console.WriteLine(
            $"[tagindexflagbatching] mirror repair ({Mirrors}): lww {Mirrors + 2} -> 3 calls, or-flag {(Mirrors * 2) + 2} -> 3 calls");
    }

    /// <summary>Old: two calls (read, delta apply) per row, all rows in one wave.</summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("orflagadd")]
    public Task<int> OrFlagAdd_PerRowEnables() => Wave(Tags * 2, Enable);

    /// <summary>Shipped: one GetManyAsync plus one ApplyCrdtDeltaManyAsync.</summary>
    [Benchmark]
    [BenchmarkCategory("orflagadd")]
    public async Task<int> OrFlagAdd_Batched()
    {
        await Hop();
        await Hop();
        return 2;
    }

    /// <summary>Old: the row wave, then the marker enable, then the reconcile schedule.</summary>
    [Benchmark]
    [BenchmarkCategory("orflagfirstadd")]
    public async Task<int> OrFlagFirstAdd_SeparateMarker()
    {
        var calls = await Wave(Tags * 2, Enable);
        calls += await Enable();
        await Hop();
        return calls + 1;
    }

    /// <summary>Shipped: one read and one delta batch carrying the marker, then the schedule.</summary>
    [Benchmark]
    [BenchmarkCategory("orflagfirstadd")]
    public async Task<int> OrFlagFirstAdd_MarkerInBatch()
    {
        await Hop();
        await Hop();
        await Hop();
        return 3;
    }

    /// <summary>Old: the RW row wave, then the marker enable, then the reconcile schedule.</summary>
    [Benchmark]
    [BenchmarkCategory("rwflagfirstadd")]
    public async Task<int> RwFlagFirstAdd_SeparateMarker()
    {
        var calls = await Wave(Tags * 2, Enable);
        calls += await Enable();
        await Hop();
        return calls + 1;
    }

    /// <summary>Shipped: the marker joins the RW row wave, then the schedule.</summary>
    [Benchmark]
    [BenchmarkCategory("rwflagfirstadd")]
    public async Task<int> RwFlagFirstAdd_MarkerInWave()
    {
        var calls = await Wave((Tags * 2) + 1, Enable);
        await Hop();
        return calls + 1;
    }

    /// <summary>Old: serial subject and index reads, then one SetAsync per mirror.</summary>
    [Benchmark]
    [BenchmarkCategory("lwwrepair")]
    public async Task<int> LwwRepair_PerMirrorWrites()
    {
        await Hop();
        await Hop();
        return 2 + await Wave(Mirrors, Write);
    }

    /// <summary>Shipped: overlapped reads, then one SetManyAsync.</summary>
    [Benchmark]
    [BenchmarkCategory("lwwrepair")]
    public async Task<int> LwwRepair_Batched()
    {
        await Task.WhenAll(Hop(), Hop());
        await Hop();
        return 3;
    }

    /// <summary>Old: serial reads, then a read plus a delta apply per mirror.</summary>
    [Benchmark]
    [BenchmarkCategory("orflagrepair")]
    public async Task<int> OrFlagRepair_PerMirrorEnables()
    {
        await Hop();
        await Hop();
        return 2 + await Wave(Mirrors, Enable);
    }

    /// <summary>Shipped: overlapped reads, then one ApplyCrdtDeltaManyAsync minted from them.</summary>
    [Benchmark]
    [BenchmarkCategory("orflagrepair")]
    public async Task<int> OrFlagRepair_Batched()
    {
        await Task.WhenAll(Hop(), Hop());
        await Hop();
        return 3;
    }

    private static Task Hop() => SimulatedHop.Wait(HopMicroseconds);

    // One flag enable: the row read, then the delta apply.
    private static async Task<int> Enable()
    {
        await Hop();
        await Hop();
        return 2;
    }

    private static async Task<int> Write()
    {
        await Hop();
        return 1;
    }

    private static async Task<int> Wave(int count, Func<Task<int>> call)
    {
        var tasks = new List<Task<int>>(count);
        for (var i = 0; i < count; i++)
        {
            tasks.Add(call());
        }

        var calls = 0;
        foreach (var n in await Task.WhenAll(tasks))
        {
            calls += n;
        }

        return calls;
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
