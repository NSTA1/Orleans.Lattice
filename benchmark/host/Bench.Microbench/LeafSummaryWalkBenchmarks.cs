using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Extends the leaf-chain dispatch benchmarks with count, diagnostics and
/// storage-footprint summary walks over four leaves. Uses the shipped page
/// types, with transport simulated as in LeafChainWalkElisionBenchmarks.
/// This isolates removed serial round trips, not end-to-end silo throughput.
/// Run with --suite leafsummary; Delay1Ms is subject to OS timer rounding.
/// </summary>
[MemoryDiagnoser]
public class LeafSummaryWalkBenchmarks
{
    /// <summary>The independently optimized leaf walks.</summary>
    public enum Site
    {
        /// <summary>Ranged and reconciliation counts: two leaf calls to one.</summary>
        Count,
        /// <summary>Deep diagnostics: two leaf calls to one.</summary>
        Diagnostics,
        /// <summary>Footprints: three calls to two, retaining snapshot storage.</summary>
        Footprint,
    }

    /// <summary>The changed walk.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The simulated transport, shared with the existing leaf-chain suite.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Check result equivalence and exact dispatch savings before measuring.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != (Path == Site.Footprint ? 12 : 8)
            || _calls != (Path == Site.Footprint ? 8 : 4))
            throw new InvalidOperationException("Summary walk result or dispatch count differs.");
    }

    /// <summary>The previous payload, optional snapshot, then topology calls.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        long total = 0;
        for (var leaf = 0; leaf < 4; leaf++)
        {
            if (Path == Site.Count)
                total += await CountAsync();
            else
            {
                var stats = await StatsAsync();
                total += stats.LiveKeys + stats.Tombstones + stats.StateBytes;
                if (Path == Site.Footprint) total += await SnapshotAsync();
            }
            total += (await LinkAsync(leaf)).KeyRange.HighKeyExclusive?.Length ?? 0;
        }
        return total;
    }

    /// <summary>The combined summary call plus unchanged snapshot-storage read.</summary>
    [Benchmark]
    public async Task<long> AfterAsync()
    {
        long total = 0;
        for (var leaf = 0; leaf < 4; leaf++)
        {
            if (Path == Site.Count)
            {
                var page = await CountPageAsync(leaf);
                total += page.Count + (page.Link.KeyRange.HighKeyExclusive?.Length ?? 0);
            }
            else
            {
                var page = await StatsPageAsync(leaf);
                total += page.Stats.LiveKeys + page.Stats.Tombstones + page.Stats.StateBytes
                    + (page.Link.KeyRange.HighKeyExclusive?.Length ?? 0);
                if (Path == Site.Footprint) total += await SnapshotAsync();
            }
        }
        return total;
    }

    private static LeafChainLink Link(int leaf) => new()
    {
        KeyRange = new LeafKeyRange { HighKeyExclusive = leaf == 3 ? null : "next" },
    };

    private static LeafStats Stats => new() { LiveKeys = 128, Tombstones = 8, StateBytes = 8192 };

    private async Task<int> CountAsync() { await DispatchAsync(); return 128; }
    private async Task<LeafStats> StatsAsync() { await DispatchAsync(); return Stats; }
    private async Task<long> SnapshotAsync() { await DispatchAsync(); return 4096; }
    private async Task<LeafChainLink> LinkAsync(int leaf) { await DispatchAsync(); return Link(leaf); }
    private async Task<LeafCountPage> CountPageAsync(int leaf)
    {
        await DispatchAsync();
        return new LeafCountPage { Count = 128, Link = Link(leaf) };
    }
    private async Task<LeafStatsPage> StatsPageAsync(int leaf)
    {
        await DispatchAsync();
        return new LeafStatsPage { Stats = Stats, Link = Link(leaf) };
    }

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
