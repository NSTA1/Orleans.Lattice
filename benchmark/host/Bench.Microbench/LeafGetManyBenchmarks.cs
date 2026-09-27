using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures the real leaf multi-get with and without a committed prepared
/// override, using allocation-free runtime fakes and one fixed registry view.
/// Run with <c>--suite leafgetmany</c>.
/// </summary>
[MemoryDiagnoser]
public class LeafGetManyBenchmarks
{
    private BPlusLeafGrain _leaf = null!;
    private readonly List<string> _keys = ["a", "b", "c", "d"];
    private readonly Guid _txid = Guid.NewGuid();
    private Dictionary<Guid, TxStatus> _snapshot = null!;

    /// <summary>Whether the read traverses the pending visibility gate.</summary>
    [Params(false, true)]
    public bool Pending { get; set; }

    /// <summary>Seeds four ordinary keys and, optionally, a prepared override.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var factory = new FakeGrainFactory();
        var registry = new FakeLatticeRegistry();
        registry.SetDefaultEntry(new TreeRegistryEntry { MaxLeafKeys = 128, ShardCount = 1 });
        factory.RouteByString<ILatticeRegistry>(_ => registry);
        var options = new FakeOptionsMonitor<LatticeOptions>(new LatticeOptions());
        _leaf = new BPlusLeafGrain(
            new FakeGrainContext(GrainId.Create("leaf", "get-many-bench")),
            new FakePersistentState<LeafNodeState>(),
            factory,
            new LatticeOptionsResolver(factory, options),
            new MutationObserverDispatcher([], NullLogger<MutationObserverDispatcher>.Instance),
            new DefaultLatticeOriginClusterIdResolver());
        foreach (var key in _keys)
            _leaf.SetAsync(key, [1]).GetAwaiter().GetResult();
        if (Pending)
        {
            LatticeTransactionContext.Set(_txid);
            try
            {
                using (LatticePreparedContext.BeginScope())
                    _leaf.SetAsync("a", [2]).GetAwaiter().GetResult();
            }
            finally
            {
                LatticeTransactionContext.Set(Guid.Empty);
            }
        }
        _snapshot = new Dictionary<Guid, TxStatus> { [_txid] = TxStatus.Committed };
        var result = GetManyAsync().GetAwaiter().GetResult();
        if (result.Count != 4 || result["a"][0] != (Pending ? 2 : 1))
            throw new InvalidOperationException("Leaf multi-get benchmark did not exercise its configured visibility path.");
    }

    /// <summary>Reads a fixed four-key batch through the shipping leaf method.</summary>
    [Benchmark]
    public Task<Dictionary<string, byte[]>> GetManyAsync()
    {
        LatticeRegistrySnapshotContext.Current = _snapshot;
        return _leaf.GetManyAsync(_keys);
    }

    /// <summary>Clears the ambient registry view after measurement.</summary>
    [GlobalCleanup]
    public void Cleanup() => LatticeRegistrySnapshotContext.Current = null;
}
