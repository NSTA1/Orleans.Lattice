using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Extends the leaf-chain dispatch model with split-aware counts, projection
/// rebuilds and bulk-append births. Measures serial dispatch shape over four
/// leaves, not end-to-end Orleans throughput. Run with --suite leafmaintenance.
/// </summary>
[MemoryDiagnoser]
public class LeafMaintenanceCallBenchmarks
{
    /// <summary>The three independent dispatch reductions.</summary>
    public enum Site
    {
        /// <summary>Keys and topology: eight calls become four.</summary>
        SplitCount,
        /// <summary>Pre-reset topology and reset: eight calls become four.</summary>
        Rebuild,
        /// <summary>Birth metadata, rows, links and graft: twenty calls become ten.</summary>
        BulkAppend,
    }

    /// <summary>The operation under measurement.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness's simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;
    private static readonly string[] Keys = ["a", "b", "c"];

    /// <summary>Require equal payloads and precise call budgets before timing.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != (Path == Site.BulkAppend ? 20 : 8)
            || _calls != (Path == Site.BulkAppend ? 10 : 4))
            throw new InvalidOperationException("Leaf maintenance result or dispatch budget differs.");
    }

    /// <summary>The previous calls, including unchanged data and graft calls.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        var total = 0;
        for (var leaf = 0; leaf < 4; leaf++)
        {
            if (Path == Site.BulkAppend)
            {
                await DispatchAsync();
                await DispatchAsync();
                await DispatchAsync();
                if (leaf > 0)
                {
                    await DispatchAsync();
                    await DispatchAsync();
                }
                total += Keys.Length;
            }
            else if (Path == Site.SplitCount)
            {
                var keys = await ReadKeysAsync();
                var link = await ReadLinkAsync(leaf);
                total += keys.Count + (link.KeyRange.HighKeyExclusive?.Length ?? 0);
            }
            else
            {
                var link = await ReadLinkAsync(leaf);
                await DispatchAsync();
                total += link.KeyRange.HighKeyExclusive?.Length ?? 0;
            }
        }
        if (Path == Site.BulkAppend) { await DispatchAsync(); await DispatchAsync(); }
        return total;
    }

    /// <summary>The combined calls, retaining birth-pin-before-data ordering.</summary>
    [Benchmark]
    public async Task<int> AfterAsync()
    {
        var total = 0;
        for (var leaf = 0; leaf < 4; leaf++)
        {
            if (Path == Site.BulkAppend)
            {
                await DispatchAsync(); // Initialization includes the durable birth pin.
                await DispatchAsync(); // The row merge remains a separate awaited call.
                total += Keys.Length;
            }
            else if (Path == Site.SplitCount)
            {
                var page = await ReadKeysPageAsync(leaf);
                total += page.Keys.Count + (page.Link.KeyRange.HighKeyExclusive?.Length ?? 0);
            }
            else
            {
                var link = Link(leaf);
                await DispatchAsync();
                total += link.KeyRange.HighKeyExclusive?.Length ?? 0;
            }
        }
        if (Path == Site.BulkAppend) { await DispatchAsync(); await DispatchAsync(); }
        return total;
    }

    private static LeafChainLink Link(int leaf) => new()
    {
        KeyRange = new LeafKeyRange { HighKeyExclusive = leaf == 3 ? null : "next" },
    };

    private async Task<List<string>> ReadKeysAsync() { await DispatchAsync(); return new(Keys); }
    private async Task<LeafChainLink> ReadLinkAsync(int leaf) { await DispatchAsync(); return Link(leaf); }
    private async Task<(List<string> Keys, LeafChainLink Link)> ReadKeysPageAsync(int leaf)
    {
        await DispatchAsync();
        return (new(Keys), Link(leaf));
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
