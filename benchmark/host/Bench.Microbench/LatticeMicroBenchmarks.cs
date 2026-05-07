using System.Globalization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// BenchmarkDotNet workloads that exercise the public <see cref="ILattice"/>
/// surface end-to-end — shard-map lookup, <see cref="IShardRootGrain"/>
/// dispatch, and the leaf-grain primitive — through hand-instantiated grains
/// with NSubstitute mocks for the Orleans runtime seams. There is no silo
/// and no grain dispatcher: the benchmark thread executes the entire vertical
/// synchronously through the await chain, so the measurement isolates the
/// lattice algorithm from Orleans' message envelope and scheduler.
/// <para>
/// Orleans-native end-to-end cost (dispatcher, serialization, scheduler) is
/// captured by the docker-compose scenarios current-state-no-replication and onwards. microbench is
/// intentionally micro: it answers <em>what does the lattice algorithm
/// cost?</em>, not <em>what does a real consumer pay?</em>. As a side-effect,
/// any contention that would arise from Orleans' single-threaded grain
/// scheduler is artificially absent here.
/// </para>
/// <para>
/// Tuning knobs come from environment variables read in
/// <see cref="GlobalSetup"/>:
/// <list type="bullet">
///   <item><c>BENCH_MICROBENCH_KEY_COUNT</c> &mdash; pre-seeded keyspace size (default 10 000).</item>
///   <item><c>BENCH_MICROBENCH_VALUE_BYTES</c> &mdash; payload size for writes (default 128).</item>
///   <item><c>BENCH_MICROBENCH_BULK_BATCH</c> &mdash; entries per <see cref="BulkLoad"/> invocation (default 1 000).</item>
/// </list>
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LatticeMicroBenchmarks
{
    private const string TreeName = "microbench-tree";

    // The public surface under measurement. All workloads call through this.
    private ILattice _lattice = null!;

    // Real grain instances backing the IGrainFactory routes. ShardCount=1 plus
    // a generously-sized MaxLeafKeys pin keeps the tree as a single
    // root-is-leaf shard, so no internal-node grains are ever created.
    private readonly Dictionary<Guid, IBPlusLeafGrain> _leaves = [];
    private readonly Dictionary<string, IShardRootGrain> _shards = [];
    private readonly Dictionary<string, ILeafCacheGrain> _leafCaches = [];
    private IGrainFactory _grainFactory = null!;
    private IOptionsMonitor<LatticeOptions> _optionsMonitor = null!;
    private LatticeOptionsResolver _optionsResolver = null!;
    private MutationObserverDispatcher _observers = null!;
    private int _maxLeafKeys;

    private string[] _keys = null!;
    private byte[] _value = null!;
    private List<KeyValuePair<string, byte[]>> _bulkBatch = null!;

    private int _writeCursor;
    private int _readCursor;
    private int _mixedCursor;

    // ===== Cycle 11 fanout instrument: 4-physical-shard sibling tree =====
    // A second ILattice activation rooted at FanoutTreeName routes through
    // the same NSubstitute IGrainFactory but resolves to a TreeRegistryEntry
    // whose ShardCount=4. This exercises the multi-shard fanout / cursor
    // scatter-gather paths in LatticeGrain that the single-shard
    // microbench cannot reach. Existing benchmarks are not modified.
    private const string FanoutTreeName = "microbench-fanout";
    private const int FanoutShardCount = 4;
    private const int FanoutScanWindowKeys = 1_000;
    private ILattice _fanoutLattice = null!;
    private List<KeyValuePair<string, byte[]>> _fanoutBulkBatch = null!;
    private string _fanoutScanStart = null!;
    private string _fanoutScanEnd = null!;

    /// <summary>
    /// Wires up the mock Orleans seams, instantiates a single-shard
    /// <see cref="ILattice"/> tree backed by real grain instances for the
    /// hot path (lattice / shard / leaf / leaf-cache) and NSubstitute stubs
    /// for fire-and-forget auxiliary grains, then pre-seeds the keyspace via
    /// direct
    /// <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
    /// calls.
    /// </summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        try
        {
            GlobalSetupCore();
        }
        catch (Exception ex)
        {
            // BenchmarkDotNet swallows GlobalSetup exceptions and reports
            // "executed benchmarks: 0" with no diagnostic. Surface the root
            // cause to stderr so the bench operator sees what failed.
            Console.Error.WriteLine($"[microbench] GlobalSetup failed: {ex}");
            throw;
        }
    }

    private void GlobalSetupCore()
    {
        var keyCount = ReadIntEnv("BENCH_MICROBENCH_KEY_COUNT", 10_000);
        var valueBytes = ReadIntEnv("BENCH_MICROBENCH_VALUE_BYTES", 128);
        var bulkBatch = ReadIntEnv("BENCH_MICROBENCH_BULK_BATCH", 1_000);

        // MaxLeafKeys must comfortably exceed every workload's keyspace plus
        // the bulk-load batch, otherwise the leaf would split and the bench
        // would attempt to construct internal-node grains we deliberately
        // do not wire. Sized 4x for safety.
        _maxLeafKeys = Math.Max(keyCount, bulkBatch) * 4;

        _optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        _optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        _observers = new MutationObserverDispatcher([], NullLogger<MutationObserverDispatcher>.Instance);

        // Build the IGrainFactory via NSubstitute and configure routes for
        // every grain type the LatticeGrain / ShardRootGrain / BPlusLeafGrain
        // hot path can reach. Unconfigured routes return null, which would
        // NRE at the call site; every grain interface touched on the hot
        // path is explicitly stubbed below.
        _grainFactory = Substitute.For<IGrainFactory>();

        // Real grain routes (constructed lazily, cached by key). These are
        // the data-bearing grains that actually drive the algorithm under
        // measurement.
        _grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>())
            .Returns(c => GetOrCreateLeaf(c.ArgAt<Guid>(0)));
        _grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(c => GetOrCreateLeaf(GuidFromGrainId(c.ArgAt<GrainId>(0))));
        _grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>())
            .Returns(c => GetOrCreateShard(c.ArgAt<string>(0)));
        _grainFactory.GetGrain<ILeafCacheGrain>(Arg.Any<string>())
            .Returns(c => GetOrCreateLeafCache(c.ArgAt<string>(0)));

        // Registry: an in-memory NSubstitute stub returning a fixed structural
        // pin so LatticeOptionsResolver resolves the same shape for every
        // tree id. Methods not used on the hot path are auto-mocked by
        // NSubstitute (Task.CompletedTask / Task<default>).
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = 1,
            }));
        registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>()).Returns(Task.CompletedTask);
        registry.UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>()).Returns(Task.CompletedTask);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.ArgAt<string>(0)));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(true));
        _grainFactory.GetGrain<ILatticeRegistry>(Arg.Any<string>()).Returns(registry);

        // Fire-and-forget auxiliary grains touched once per activation by
        // LatticeGrain.SetAsync. NSubstitute auto-mocks every Task-returning
        // method to Task.CompletedTask, which is exactly the no-op behaviour
        // we want — the bench is not measuring tombstone compaction or hot
        // shard monitoring.
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        _grainFactory.GetGrain<ITombstoneCompactionGrain>(Arg.Any<string>()).Returns(compaction);

        var monitor = Substitute.For<IHotShardMonitorGrain>();
        _grainFactory.GetGrain<IHotShardMonitorGrain>(Arg.Any<string>()).Returns(monitor);

        var stats = Substitute.For<ILatticeStats>();
        _grainFactory.GetGrain<ILatticeStats>(Arg.Any<string>()).Returns(stats);

        // The resolver depends on the factory + monitor — same singleton
        // shared by every grain layer.
        _optionsResolver = new LatticeOptionsResolver(_grainFactory, _optionsMonitor);

        // Build the LatticeGrain (the public ILattice). Service provider is
        // only dereferenced by LatticeEventPublisher when PublishEvents is
        // enabled (it is not in the default options), so a Substitute is safe.
        var latticeContext = Substitute.For<IGrainContext>();
        latticeContext.GrainId.Returns(GrainId.Create("lattice", TreeName));
        var serviceProvider = Substitute.For<IServiceProvider>();
        var lattice = new LatticeGrain(
            latticeContext,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            serviceProvider,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(lattice);
        _lattice = lattice;

        // Stable lexicographic ordering with fixed-width zero-padded indices.
        _keys = new string[keyCount];
        for (var i = 0; i < keyCount; i++)
        {
            _keys[i] = "k-" + i.ToString("D8", CultureInfo.InvariantCulture);
        }

        _value = new byte[valueBytes];
        Random.Shared.NextBytes(_value);

        // Pre-seed the keyspace through the public surface so PointRead and
        // Mixed have data to read.
        for (var i = 0; i < keyCount; i++)
        {
            _lattice.SetAsync(_keys[i], _value).GetAwaiter().GetResult();
        }

        // Pre-build the bulk batch outside the seeded keyspace so PointRead's
        // entries aren't disturbed when BulkLoad runs.
        _bulkBatch = new List<KeyValuePair<string, byte[]>>(bulkBatch);
        for (var i = 0; i < bulkBatch; i++)
        {
            var k = "bulk-" + i.ToString("D8", CultureInfo.InvariantCulture);
            _bulkBatch.Add(new(k, _value));
        }

        BuildFanoutTree(keyCount, bulkBatch);
    }

    /// <summary>
    /// Lazily constructs and caches a <see cref="BPlusLeafGrain"/> for the
    /// given GUID. Reused between activations so the in-memory state of a
    /// leaf survives across hot-path calls.
    /// </summary>
    private IBPlusLeafGrain GetOrCreateLeaf(Guid id)
    {
        if (_leaves.TryGetValue(id, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("leaf", id.ToString("N")));
        var state = new FakePersistentState<LeafNodeState>();
        var leaf = new BPlusLeafGrain(ctx, state, _grainFactory, _optionsResolver, _observers);
        _leaves[id] = leaf;
        return leaf;
    }

    /// <summary>
    /// Lazily constructs and caches a <see cref="ShardRootGrain"/> for the
    /// given string key (<c>{treeId}/{shardIndex}</c>).
    /// </summary>
    private IShardRootGrain GetOrCreateShard(string key)
    {
        if (_shards.TryGetValue(key, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shard", key));
        var state = new FakePersistentState<ShardRootState>();
        var shard = new ShardRootGrain(
            ctx, state, _grainFactory, _optionsResolver,
            NullLogger<ShardRootGrain>.Instance, _observers);
        _shards[key] = shard;
        return shard;
    }

    /// <summary>
    /// Lazily constructs and caches a real <see cref="LeafCacheGrain"/> for
    /// the given leaf-id string. The cache grain reads from its primary leaf
    /// via the factory using the GrainId parsed out of its own context key,
    /// so we set the context's GrainId.Key to the leaf-id string we were
    /// asked for. The factory then routes back to the corresponding cached
    /// real <see cref="BPlusLeafGrain"/>.
    /// </summary>
    private ILeafCacheGrain GetOrCreateLeafCache(string leafIdString)
    {
        if (_leafCaches.TryGetValue(leafIdString, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        // The cache grain calls GrainId.Parse(context.GrainId.Key.ToString())
        // to recover the primary leaf id, so the key must be the leaf-id
        // round-trippable string.
        ctx.GrainId.Returns(GrainId.Create("leafcache", leafIdString));
        var cache = new LeafCacheGrain(ctx, _grainFactory, _optionsMonitor);
        _leafCaches[leafIdString] = cache;
        return cache;
    }

    /// <summary>
    /// Extracts the deterministic <see cref="Guid"/> embedded in a leaf
    /// <see cref="GrainId"/>. Leaves are created via
    /// <see cref="IGrainFactory.GetGrain{T}(Guid)"/> so the GrainId's key is
    /// the GUID's hex representation.
    /// </summary>
    private static Guid GuidFromGrainId(GrainId id)
    {
        var key = id.Key.ToString()!;
        return Guid.TryParseExact(key, "N", out var n) ? n : Guid.Parse(key);
    }

    /// <summary>
    /// Baseline control: returns a completed <see cref="Task"/> immediately
    /// without touching <see cref="ILattice"/>. Anything reported above
    /// ~32 bytes/op for this workload is BenchmarkDotNet measurement floor +
    /// AppDomain background allocation. Subtract this from the other
    /// workloads to estimate the actual lattice-vertical alloc cost.
    /// </summary>
    [Benchmark(Description = "Noop (baseline)", Baseline = true)]
    public Task Noop()
    {
        // Read the rotating cursor so the JIT can't elide the keyspace access entirely.
        _ = unchecked(_writeCursor++) & int.MaxValue;
        return Task.CompletedTask;
    }

    /// <summary>Single <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/> against a rotating key.</summary>
    [Benchmark(Description = "Point write")]
    public Task PointWrite()
    {
        var i = unchecked(_writeCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.SetAsync(key, _value);
    }

    /// <summary>Single <see cref="ILattice.GetAsync(string, CancellationToken)"/> against a rotating key.</summary>
    [Benchmark(Description = "Point read")]
    public Task<byte[]?> PointRead()
    {
        var i = unchecked(_readCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.GetAsync(key);
    }

    /// <summary>One <see cref="ILattice.SetManyAsync"/> invocation flushing the pre-built batch.</summary>
    [Benchmark(Description = "Bulk load")]
    public Task BulkLoad()
    {
        return _lattice.SetManyAsync(_bulkBatch);
    }

    /// <summary>
    /// 70% read / 30% write mix &mdash; a single op per invocation that picks
    /// read or write based on the rotating cursor.
    /// </summary>
    [Benchmark(Description = "Mixed 70R/30W")]
    public Task Mixed_70R_30W()
    {
        var i = unchecked(_mixedCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        // Modulo-10: 0..6 = read, 7..9 = write.
        if ((i % 10) < 7)
        {
            return _lattice.GetAsync(key);
        }
        return _lattice.SetAsync(key, _value);
    }

    /// <summary>
    /// Builds a sibling <see cref="ILattice"/> activation rooted at
    /// <see cref="FanoutTreeName"/> with <see cref="FanoutShardCount"/>
    /// physical shards and pre-seeds a bounded scan window. The NSubstitute
    /// most-specific-match rule lets us override <c>registry.GetEntryAsync</c>
    /// and <c>_grainFactory.GetGrain&lt;ILattice&gt;</c> for the literal
    /// <see cref="FanoutTreeName"/> without disturbing the catch-all routes
    /// the single-shard <c>_lattice</c> uses.
    /// </summary>
    private void BuildFanoutTree(int keyCount, int bulkBatch)
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(FanoutTreeName);
        registry.GetEntryAsync(FanoutTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = FanoutShardCount,
            }));

        var fanoutContext = Substitute.For<IGrainContext>();
        fanoutContext.GrainId.Returns(GrainId.Create("lattice", FanoutTreeName));
        var fanoutSp = Substitute.For<IServiceProvider>();
        var fanoutLattice = new LatticeGrain(
            fanoutContext,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            fanoutSp,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(FanoutTreeName).Returns(fanoutLattice);
        _fanoutLattice = fanoutLattice;

        // Pre-seed the keyspace through the public surface so KeyScan has
        // data to traverse across all 4 physical shards.
        for (var i = 0; i < keyCount; i++)
        {
            _fanoutLattice.SetAsync(_keys[i], _value).GetAwaiter().GetResult();
        }

        // Pre-build a fanout-specific bulk batch with disjoint keys so
        // SetMany_4Shards does not collide with the seeded keyspace.
        _fanoutBulkBatch = new List<KeyValuePair<string, byte[]>>(bulkBatch);
        for (var i = 0; i < bulkBatch; i++)
        {
            var k = "fbulk-" + i.ToString("D8", CultureInfo.InvariantCulture);
            _fanoutBulkBatch.Add(new(k, _value));
        }

        // Scan window: first FanoutScanWindowKeys keys (or all of them if
        // the keyspace is smaller). Append \u0000 to the upper bound so the
        // half-open range [start, end) covers the inclusive lexicographic
        // tail of the chosen suffix.
        var window = Math.Min(FanoutScanWindowKeys, keyCount);
        _fanoutScanStart = _keys[0];
        _fanoutScanEnd = _keys[window - 1] + "\u0000";
    }

    /// <summary>
    /// One <see cref="ILattice.SetManyAsync"/> invocation against the
    /// 4-shard fanout tree. Exercises the bulk-fanout grain-reference cache.
    /// </summary>
    [Benchmark(Description = "SetMany 4 shards")]
    public Task SetMany_4Shards()
    {
        return _fanoutLattice.SetManyAsync(_fanoutBulkBatch);
    }

    /// <summary>
    /// Paginated key scan over the 4-shard fanout tree. Drains the
    /// <see cref="ILattice.KeysAsync(string, string, bool, bool?, CancellationToken)"/>
    /// async-enumerable so the per-shard cursor open + reconciliation paths
    /// in LatticeGrain.Keys.cs are walked end-to-end.
    /// </summary>
    [Benchmark(Description = "Key scan 4 shards")]
    public async Task KeyScan_PageOver4Shards()
    {
        await foreach (var _ in _fanoutLattice.KeysAsync(_fanoutScanStart, _fanoutScanEnd))
        {
        }
    }

    private static int ReadIntEnv(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > 0
            ? v
            : fallback;
    }
}
