using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures the real leaf key and entry range reads with and without a
/// prepared transactional write in the range, so the cost of the
/// transactional-read signal a range read over pending writes publishes
/// (issue #2823) is visible against the steady-state read.
/// Run with <c>--suite leafrangeread</c>.
/// </summary>
[MemoryDiagnoser]
public class LeafRangeReadBenchmarks
{
    private BPlusLeafGrain _leaf = null!;
    private readonly Guid _txid = Guid.NewGuid();
    private Dictionary<Guid, TxStatus> _snapshot = null!;

    /// <summary>Whether the read traverses the pending visibility gate.</summary>
    [Params(false, true)]
    public bool Pending { get; set; }

    /// <summary>Seeds sixteen ordinary keys and, optionally, a prepared override.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var factory = new FakeGrainFactory();
        var registry = new FakeLatticeRegistry();
        registry.SetDefaultEntry(new TreeRegistryEntry { MaxLeafKeys = 128, ShardCount = 1 });
        factory.RouteByString<ILatticeRegistry>(_ => registry);
        var options = new FakeOptionsMonitor<LatticeOptions>(new LatticeOptions());
        _leaf = new BPlusLeafGrain(
            new FakeGrainContext(GrainId.Create("leaf", "range-read-bench")),
            new FakePersistentState<LeafNodeState>(),
            factory,
            new LatticeOptionsResolver(factory, options),
            new MutationObserverDispatcher([], NullLogger<MutationObserverDispatcher>.Instance),
            new DefaultLatticeOriginClusterIdResolver());
        for (var i = 0; i < 16; i++)
            _leaf.SetAsync($"k{i:D2}", [1]).GetAwaiter().GetResult();
        if (Pending)
        {
            LatticeTransactionContext.Set(_txid);
            try
            {
                using (LatticePreparedContext.BeginScope())
                    _leaf.SetAsync("k00", [2]).GetAwaiter().GetResult();
            }
            finally
            {
                LatticeTransactionContext.Set(Guid.Empty);
            }
        }
        _snapshot = new Dictionary<Guid, TxStatus> { [_txid] = TxStatus.Committed };
        var entries = GetEntriesAsync().GetAwaiter().GetResult();
        if (entries.Count != 16 || entries[0].Value[0] != (Pending ? 2 : 1))
            throw new InvalidOperationException("Leaf range-read benchmark did not exercise its configured visibility path.");
    }

    /// <summary>Reads every key through the shipping leaf key range read.</summary>
    [Benchmark]
    public Task<List<string>> GetKeysAsync()
    {
        LatticeRegistrySnapshotContext.Current = _snapshot;
        return _leaf.GetKeysAsync();
    }

    /// <summary>Reads every entry through the shipping leaf entry range read.</summary>
    [Benchmark]
    public Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync()
    {
        LatticeRegistrySnapshotContext.Current = _snapshot;
        return _leaf.GetEntriesAsync();
    }

    /// <summary>Clears the ambient registry view after measurement.</summary>
    [GlobalCleanup]
    public void Cleanup() => LatticeRegistrySnapshotContext.Current = null;
}
