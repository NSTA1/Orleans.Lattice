using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-model evidence for three internal-node grain-call reductions: split
/// sibling seeding, bulk-graft rightmost-edge descent and the node-binding walk.
/// Uses the existing leaf-chain hop model: Yield measures scheduling overhead;
/// Delay1Ms models latency, not real Orleans throughput. Persist cost inside the
/// seeding call is modelled as one dispatch per grain call.
/// Run with --suite internalnodefusion. The --suite internalnodebinding subset
/// measures tree binding plus initialization at three construction sites. Both
/// durable writes and child digest seeding are unchanged and excluded from
/// these dispatch-only measurements. BulkConstruction sums per-node dispatch
/// work; production uses parallel waves, so this is not bulk-load wall time.
/// The parentstructure subset measures leaf adoption, internal adoption and a
/// matching-owner targeted topology read, each with a two-to-one call budget.
/// Denied topology reads already took one call and are not a claimed saving.
/// </summary>
[MemoryDiagnoser]
public class InternalNodeCallFusionBenchmarks
{
    /// <summary>The optimized call sites.</summary>
    public enum Site
    {
        /// <summary>Seeding a split sibling with 32 children: 32 calls become 1.</summary>
        SplitSeed,
        /// <summary>Graft descent over 3 internal levels: 6 calls become 3.</summary>
        GraftDescent,
        /// <summary>Binding walk over 40 internal nodes: 80 calls become 40.</summary>
        BindingWalk,
        /// <summary>Root promotion: bind and initialize, 2 calls become 1.</summary>
        RootConstruction,
        /// <summary>Bulk construction of 40 internal nodes: 80 calls become 40.</summary>
        BulkConstruction,
        /// <summary>Split sibling recovery: bind and seed, 2 calls become 1.</summary>
        SiblingConstruction,
        /// <summary>Leaf reparent plus child snapshot: 2 calls become 1.</summary>
        LeafParentSeed,
        /// <summary>Internal reparent plus subtree snapshot: 2 calls become 1.</summary>
        InternalParentSeed,
        /// <summary>Matching tree ownership plus topology: 2 calls become 1.</summary>
        OwnedTopology,
    }

    /// <summary>The call site under measurement.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness's simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private const int SiblingChildren = 32;
    private const int Levels = 3;
    private const int Nodes = 40;
    private int _calls;

    /// <summary>Check call budgets before collecting measurements.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        BeforeAsync().GetAwaiter().GetResult();
        var before = _calls;
        _calls = 0;
        AfterAsync().GetAwaiter().GetResult();
        var (expectedBefore, expectedAfter) = Path switch
        {
            Site.SplitSeed => (SiblingChildren, 1),
            Site.GraftDescent => (Levels * 2, Levels),
            Site.RootConstruction or Site.SiblingConstruction or Site.LeafParentSeed
                or Site.InternalParentSeed or Site.OwnedTopology => (2, 1),
            _ => (Nodes * 2, Nodes),
        };
        if (before != expectedBefore || _calls != expectedAfter)
            throw new InvalidOperationException("Internal-node call budget differs.");
    }

    /// <summary>The prior sequential grain-call shape.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        for (var i = 0; i < Count(before: true); i++) await DispatchAsync();
        return _calls;
    }

    /// <summary>The fused grain-call shape.</summary>
    [Benchmark]
    public async Task<int> AfterAsync()
    {
        for (var i = 0; i < Count(before: false); i++) await DispatchAsync();
        return _calls;
    }

    private int Count(bool before) => Path switch
    {
        Site.SplitSeed => before ? SiblingChildren : 1,
        Site.GraftDescent => before ? Levels * 2 : Levels,
        Site.RootConstruction or Site.SiblingConstruction or Site.LeafParentSeed
            or Site.InternalParentSeed or Site.OwnedTopology => before ? 2 : 1,
        _ => before ? Nodes * 2 : Nodes,
    };

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
