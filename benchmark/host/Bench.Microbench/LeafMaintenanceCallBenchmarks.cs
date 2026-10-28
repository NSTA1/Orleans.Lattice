using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Extends the leaf-chain dispatch model with split-aware counts, projection
/// rebuilds and bulk-append births. Measures serial dispatch shape over four
/// leaves, not end-to-end Orleans throughput. Run with --suite leafmaintenance.
/// The --suite leafbinding subset measures root creation's binding segment and
/// single-key/batch CRDT repair of one unbound leaf. Repair lanes include both
/// unchanged apply dispatches, but not exception construction or persistence.
/// Both binding writes, birth-pin I/O and replay gates remain unchanged.
/// </summary>
[MemoryDiagnoser]
public class LeafMaintenanceCallBenchmarks
{
    /// <summary>The independent dispatch reductions.</summary>
    public enum Site
    {
        /// <summary>Keys and topology: eight calls become four.</summary>
        SplitCount,
        /// <summary>Pre-reset topology and reset: eight calls become four.</summary>
        Rebuild,
        /// <summary>Birth metadata, rows, links and graft: twenty calls become ten.</summary>
        BulkAppend,
        /// <summary>Root creation binding: two calls become one.</summary>
        RootBinding,
        /// <summary>One unbound single-key apply and retry: four calls become three.</summary>
        SingleCrdtRepair,
        /// <summary>One unbound leaf's batch apply and retry: four calls become three.</summary>
        BatchCrdtRepair,
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
        var (expectedBefore, expectedAfter) = Path switch
        {
            Site.BulkAppend => (20, 10),
            Site.RootBinding => (2, 1),
            Site.SingleCrdtRepair or Site.BatchCrdtRepair => (4, 3),
            _ => (8, 4),
        };
        if (before != after || beforeCalls != expectedBefore || _calls != expectedAfter)
            throw new InvalidOperationException("Leaf maintenance result or dispatch budget differs.");
    }

    /// <summary>The previous calls, including unchanged data and graft calls.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        if (IsBindingSite)
            return await BindingAsync(combined: false);
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
        if (IsBindingSite)
            return await BindingAsync(combined: true);
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

    private bool IsBindingSite => Path is Site.RootBinding or Site.SingleCrdtRepair or Site.BatchCrdtRepair;

    private async Task<int> BindingAsync(bool combined)
    {
        var repair = Path != Site.RootBinding;
        if (repair) await DispatchAsync(); // The initial unbound apply.
        await DispatchAsync(); // Tree binding, or the combined binding.
        if (!combined) await DispatchAsync(); // Separate shard binding.
        if (repair) await DispatchAsync(); // The unchanged single retry.
        return Path == Site.BatchCrdtRepair ? Keys.Length : 1;
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
