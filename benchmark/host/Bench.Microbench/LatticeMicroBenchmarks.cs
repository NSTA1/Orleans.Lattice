using System.Globalization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

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
///   <item><c>BENCH_MICROBENCH_DEEPER_MAX_LEAF_KEYS</c> &mdash; <see cref="BuildDeeperTree"/> leaf fan-out cap (default 4).</item>
///   <item><c>BENCH_MICROBENCH_DEEPER_MAX_INTERNAL_CHILDREN</c> &mdash; <see cref="BuildDeeperTree"/> internal fan-out cap (default 4).</item>
///   <item><c>BENCH_MICROBENCH_DEEPER_KEY_COUNT</c> &mdash; <see cref="BuildDeeperTree"/> seeded keyspace (default 256, yields 3 internal levels at fan-out 4).</item>
///   <item><c>BENCH_MICROBENCH_DEEPER_BULK_BATCH</c> &mdash; <see cref="BulkLoad_DeeperTree"/> batch size (default 32).</item>
///   <item><c>BENCH_MICROBENCH_ATOMIC_BATCH</c> &mdash; <see cref="SetManyAtomic"/> entries per saga (default 16).</item>
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

    // ===== Cycle 13 deep-tree instrument: depth-2 tree =====
    // A third ILattice activation rooted at DeepTreeName forces a multi-level
    // tree by pinning MaxLeafKeys to a small value (DeepMaxLeafKeys) and
    // seeding DeepKeyCount keys. The result is a depth-2 tree: one internal
    // root holding ~DeepKeyCount/DeepMaxLeafKeys leaf children. Every
    // PointWrite / BulkLoad against this tree drives ShardRootGrain.Traversal
    // through grainFactory.GetGrain<IBPlusInternalGrain>(rootId) per
    // operation, exercising the internal-node-grain materialisation cost
    // that the single-shard / fanout trees cannot reach (both pin
    // MaxLeafKeys high enough that RootIsLeaf=true).
    //
    // The IBPlusInternalGrain factory route is wired in GlobalSetupCore
    // alongside the leaf / shard / cache routes; instances are constructed
    // on demand by GetOrCreateInternalGrain (mirrors GetOrCreateLeaf).
    private const string DeepTreeName = "microbench-deep";
    private const int DeepMaxLeafKeys = 4;
    private const int DeepKeyCount = 32;
    private const int DeepBulkBatch = 16;
    private ILattice _deepLattice = null!;
    private string[] _deepKeys = null!;
    private List<KeyValuePair<string, byte[]>> _deepBulkBatch = null!;
    private readonly Dictionary<GrainId, IBPlusInternalGrain> _internalGrains = [];
    private int _deepWriteCursor;

    // ===== Cycle 17 deeper-tree instrument: depth-3+ tree =====
    // A fourth ILattice activation rooted at DeeperTreeName forces a tree
    // with two or more internal levels above the leaves by pinning BOTH
    // MaxLeafKeys AND MaxInternalChildren small. The default sizing
    // (DeeperMaxLeafKeysDefault=4, DeeperMaxInternalChildrenDefault=4,
    // DeeperKeyCountDefault=256) yields:
    //   leaves        = ⌈256 / 4⌉ = 64 nodes
    //   level-1 mid   = ⌈64  / 4⌉ = 16 nodes
    //   level-2 mid   = ⌈16  / 4⌉ = 4  nodes
    //   level-3 root  = ⌈4   / 4⌉ = 1  node
    // → every traversal walks root → L2 → L1 → leaf, paying THREE
    // grainFactory.GetGrain<IBPlusInternalGrain>(...) materialisations
    // per op. The depth-2 DeepTree above pays one. This lifts the
    // measurement above the BDN MemoryDiagnoser bucket-resolution floor
    // for any optimisation targeting per-internal-hop allocations
    // (cycle 14's routing-table cache, the still-deferred
    // Task<RoutingTableSnapshot> cache-miss alloc, and any future
    // internal-grain-ref multi-slot LRU widening).
    //
    // All four shape parameters are env-overridable so a future agent
    // can author a depth-N variant by env file alone, without further
    // edits to this bench-host file. The IBPlusInternalGrain factory
    // routes in GlobalSetupCore already handle arbitrary internal-node
    // GrainIds via GetOrCreateInternalGrain, so no additional wiring is
    // needed.
    private const string DeeperTreeName = "microbench-deeper";
    private const int DeeperMaxLeafKeysDefault = 4;
    private const int DeeperMaxInternalChildrenDefault = 4;
    private const int DeeperKeyCountDefault = 256;
    private const int DeeperBulkBatchDefault = 32;
    private int _deeperMaxLeafKeys;
    private int _deeperMaxInternalChildren;
    private int _deeperKeyCount;
    private int _deeperBulkBatch;
    private ILattice _deeperLattice = null!;
    private string[] _deeperKeys = null!;
    private List<KeyValuePair<string, byte[]>> _deeperBulkBatchList = null!;
    private int _deeperWriteCursor;
    private int _deeperReadCursor;

    // ===== Atomic-write instrument: SetManyAtomicAsync saga =====
    // A fifth ILattice activation rooted at AtomicTreeName drives the
    // SetManyAtomicAsync saga end-to-end through a real AtomicWriteGrain
    // and TxRegistryGrain. The saga vertical exercises far more code than
    // the plain SetMany_4Shards bench:
    //   1. AtomicWriteGrain.PrepareAsync persists per-key pre-values via
    //      IShardRootGrain.GetRawEntryAsync.
    //   2. AtomicWriteGrain.ExecutePhaseAsync issues per-key SetAsync
    //      under LatticePreparedContext + LatticeAtomicBatchContext.
    //   3. AtomicWriteGrain.BroadcastTerminalsAsync notifies every
    //      touched shard via IShardRootGrain.AppendTxTerminalAsync and
    //      records the global commit decision via ITxRegistryGrain.
    //   4. AtomicWriteGrain.CompleteSagaAsync emits the
    //      orleans.lattice.atomic_write.{completed,duration,batch_size}
    //      instruments — the operator-visible signal for sustained
    //      atomic-write throughput.
    // Every saga mints a fresh operationId, so each [Benchmark] iteration
    // creates a brand-new AtomicWriteGrain (and persisted state). The
    // sustained ops/s figure surfaced by HarnessJsonExporter as
    // microbench_set_many_atomic_per_second is the headline number.
    private const string AtomicTreeName = "microbench-atomic";
    private const int AtomicBatchDefault = 16;
    private ILattice _atomicLattice = null!;
    private List<KeyValuePair<string, byte[]>> _atomicBatch = null!;
    private readonly Dictionary<string, IAtomicWriteGrain> _atomicSagas = [];
    private readonly Dictionary<string, ITxRegistryGrain> _txRegistries = [];
    private IReminderRegistry _atomicReminderRegistry = null!;

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
        // Cycle 13: route IBPlusInternalGrain for the deep-tree benchmarks.
        // Existing single-shard / fanout benchmarks pin MaxLeafKeys high
        // enough that this route is never exercised by them, so wiring it
        // does not perturb their measurements. Both Guid and GrainId
        // overloads must be stubbed: BPlusLeafGrain.SplitAsync, the root
        // promotion path in ShardRootGrain.CompletePromotionAsync, and the
        // internal-grain split path all call the Guid overload directly,
        // while the per-traversal hot path in ShardRootGrain.Traversal calls
        // the GrainId overload.
        _grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<Guid>())
            .Returns(c => GetOrCreateInternalGrain(GrainId.Create("internal", c.ArgAt<Guid>(0).ToString("N"))));
        _grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>())
            .Returns(c => GetOrCreateInternalGrain(c.ArgAt<GrainId>(0)));

        // Atomic-write saga routes: a real AtomicWriteGrain per saga key
        // ({treeId}/{operationId}) and a real TxRegistryGrain per tree.
        // The IReminderRegistry seam is mocked: RegisterOrUpdateReminder
        // and UnregisterReminder return Task.CompletedTask, GetReminder
        // returns null. That short-circuits the keepalive-reminder
        // bookkeeping which is irrelevant under the synchronous bench
        // harness (there is no scheduler that could fire it anyway).
        _atomicReminderRegistry = Substitute.For<IReminderRegistry>();
        _atomicReminderRegistry
            .RegisterOrUpdateReminder(Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        _atomicReminderRegistry
            .GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult<IGrainReminder?>(null));
        _atomicReminderRegistry
            .UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>())
            .Returns(Task.FromResult(true));
        _grainFactory.GetGrain<IAtomicWriteGrain>(Arg.Any<string>())
            .Returns(c => GetOrCreateAtomicSaga(c.ArgAt<string>(0)));
        _grainFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>())
            .Returns(c => GetOrCreateTxRegistry(c.ArgAt<string>(0)));

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
        BuildDeepTree();
        BuildDeeperTree();
        BuildAtomicTree();
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
    /// so we set the context's GrainId.Key to the leaf-id string to the leaf-id
    /// string.
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
    /// Lazily constructs and caches a real <see cref="BPlusInternalGrain"/>
    /// for the given <see cref="GrainId"/>. Used by the deep-tree benchmarks
    /// that force a depth-2 tree shape (single internal root + multiple
    /// leaves). Mirrors the leaf / shard / leaf-cache constructors above.
    /// </summary>
    private IBPlusInternalGrain GetOrCreateInternalGrain(GrainId id)
    {
        if (_internalGrains.TryGetValue(id, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(id);
        var state = new FakePersistentState<InternalNodeState>();
        var grain = new BPlusInternalGrain(ctx, state, _grainFactory, _optionsResolver);
        _internalGrains[id] = grain;
        return grain;
    }

    /// <summary>
    /// Lazily constructs and caches a real <see cref="AtomicWriteGrain"/>
    /// for the composite saga key (<c>{treeId}/{operationId}</c>).
    /// Each <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// invocation mints a fresh operationId, so a new saga grain is
    /// constructed per benchmark iteration. The dictionary retains
    /// completed sagas in memory through GlobalCleanup; their
    /// AtomicWriteState is small (entry list + pre-values) and doesn't
    /// dominate the measured allocation profile.
    /// </summary>
    private IAtomicWriteGrain GetOrCreateAtomicSaga(string key)
    {
        if (_atomicSagas.TryGetValue(key, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("atomic-write", key));
        var sagaState = new FakePersistentState<AtomicWriteState>();
        var saga = new AtomicWriteGrain(
            ctx,
            _grainFactory,
            _atomicReminderRegistry,
            _optionsMonitor,
            NullLogger<AtomicWriteGrain>.Instance,
            sagaState);
        _atomicSagas[key] = saga;
        return saga;
    }

    /// <summary>
    /// Lazily constructs and caches a real <see cref="TxRegistryGrain"/>
    /// for the given tree id. Used by <see cref="AtomicWriteGrain"/>'s
    /// terminal-broadcast phase to record the global commit/abort
    /// decision and by <see cref="BPlusLeafGrain"/>'s pending-tx readers
    /// for fence resolution. Per-tree singleton.
    /// </summary>
    private ITxRegistryGrain GetOrCreateTxRegistry(string treeId)
    {
        if (_txRegistries.TryGetValue(treeId, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("tx-registry", treeId));
        var registryState = new FakePersistentState<TxRegistryState>();
        var registry = new TxRegistryGrain(ctx, registryState);
        _txRegistries[treeId] = registry;
        return registry;
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

    /// <summary>
    /// Single <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
    /// against a depth-2 tree. Each invocation drives
    /// <see cref="ShardRootGrain"/>'s traversal through
    /// <c>grainFactory.GetGrain&lt;IBPlusInternalGrain&gt;(rootId)</c> exactly
    /// once before reaching the target leaf — the call site this cycle
    /// targets for caching.
    /// </summary>
    [Benchmark(Description = "Point write deep tree")]
    public Task PointWrite_DeepTree()
    {
        var i = unchecked(_deepWriteCursor++) & int.MaxValue;
        var key = _deepKeys[i % _deepKeys.Length];
        return _deepLattice.SetAsync(key, _value);
    }

    /// <summary>
    /// One <see cref="ILattice.SetManyAsync"/> invocation against the
    /// depth-2 tree. The internal layer is materialised
    /// <see cref="DeepBulkBatch"/> times per call (once per key, since the
    /// batch is dispatched via per-key traversal in the depth-2 fan-in).
    /// </summary>
    [Benchmark(Description = "Bulk load deep tree")]
    public Task BulkLoad_DeepTree()
    {
        return _deepLattice.SetManyAsync(_deepBulkBatch);
    }

    /// <summary>
    /// Single <see cref="ILattice.GetAsync(string, CancellationToken)"/>
    /// against the depth-3+ deeper tree. Each invocation drives
    /// <see cref="ShardRootGrain"/>'s read traversal through
    /// <c>grainFactory.GetGrain&lt;IBPlusInternalGrain&gt;(internalId)</c>
    /// at every level above the leaves — at default sizing
    /// (<see cref="DeeperMaxLeafKeysDefault"/> = 4,
    /// <see cref="DeeperMaxInternalChildrenDefault"/> = 4,
    /// <see cref="DeeperKeyCountDefault"/> = 256), three internal-grain
    /// hops per op. Lifts internal-hop alloc above the BDN MemoryDiagnoser
    /// bucket-resolution floor for cycle-14+ optimisations targeting the
    /// internal-grain-ref / routing-table-cache / RPC-elision paths that
    /// the depth-2 deep tree could not resolve.
    /// </summary>
    [Benchmark(Description = "Point read deeper tree")]
    public Task<byte[]?> PointRead_DeeperTree()
    {
        var i = unchecked(_deeperReadCursor++) & int.MaxValue;
        var key = _deeperKeys[i % _deeperKeys.Length];
        return _deeperLattice.GetAsync(key);
    }

    /// <summary>
    /// Single <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
    /// against the depth-3+ deeper tree. Same per-op internal-hop count
    /// as <see cref="PointRead_DeeperTree"/>; isolates the write-path
    /// allocation profile (HLC tick, version vector advance, leaf
    /// commit) over a multi-internal traversal.
    /// </summary>
    [Benchmark(Description = "Point write deeper tree")]
    public Task PointWrite_DeeperTree()
    {
        var i = unchecked(_deeperWriteCursor++) & int.MaxValue;
        var key = _deeperKeys[i % _deeperKeys.Length];
        return _deeperLattice.SetAsync(key, _value);
    }

    /// <summary>
    /// One <see cref="ILattice.SetManyAsync"/> invocation against the
    /// depth-3+ deeper tree. The internal layers are walked once per
    /// key in the batch, surfacing per-traversal internal allocations
    /// at <c>batch_size × internal_levels</c> density.
    /// </summary>
    [Benchmark(Description = "Bulk load deeper tree")]
    public Task BulkLoad_DeeperTree()
    {
        return _deeperLattice.SetManyAsync(_deeperBulkBatchList);
    }

    /// <summary>
    /// One <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// invocation against a single-shard atomic-write tree. Each
    /// iteration mints a fresh operationId and drives a brand-new
    /// <see cref="AtomicWriteGrain"/> end-to-end:
    /// Prepare (per-key pre-value capture) →
    /// Execute (per-key SetAsync under
    /// <see cref="LatticeAtomicBatchContext"/>) →
    /// BroadcastTerminals (per-shard
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> +
    /// <see cref="ITxRegistryGrain.MarkCommittedAsync"/>) →
    /// Complete (records
    /// <see cref="LatticeMetrics.AtomicWriteCompleted"/>,
    /// <see cref="LatticeMetrics.AtomicWriteDuration"/>, and
    /// <see cref="LatticeMetrics.AtomicWriteBatchSize"/>).
    /// <para>
    /// The harness reports sustained atomic-write ops/s as
    /// <c>microbench_set_many_atomic_per_second</c> in
    /// <c>results.json</c>. Tunable via
    /// <c>BENCH_MICROBENCH_ATOMIC_BATCH</c> (default
    /// <see cref="AtomicBatchDefault"/> = 16 entries per saga).
    /// </para>
    /// </summary>
    [Benchmark(Description = "SetMany atomic")]
    public Task SetManyAtomic()
    {
        return _atomicLattice.SetManyAtomicAsync(_atomicBatch);
    }

    /// <summary>
    /// Builds a sibling <see cref="ILattice"/> activation rooted at
    /// <see cref="DeepTreeName"/> with <see cref="DeepMaxLeafKeys"/>
    /// pinned small enough that the seed phase forces a root split.
    /// After seeding, the tree shape is depth-2: one internal root
    /// (<see cref="IBPlusInternalGrain"/>) holding
    /// ⌈<see cref="DeepKeyCount"/>/<see cref="DeepMaxLeafKeys"/>⌉ leaf
    /// children. Every subsequent traversal for any key in the seeded
    /// range walks root → leaf, paying one
    /// <c>grainFactory.GetGrain&lt;IBPlusInternalGrain&gt;(rootId)</c>
    /// materialisation per call.
    /// </summary>
    private void BuildDeepTree()
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(DeepTreeName);
        registry.GetEntryAsync(DeepTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = DeepMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = 1,
            }));

        var deepContext = Substitute.For<IGrainContext>();
        deepContext.GrainId.Returns(GrainId.Create("lattice", DeepTreeName));
        var deepSp = Substitute.For<IServiceProvider>();
        var deepLattice = new LatticeGrain(
            deepContext,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            deepSp,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(DeepTreeName).Returns(deepLattice);
        _deepLattice = deepLattice;

        // Distinct keyspace from the single-shard / fanout trees so the
        // deep-tree's seeded keys don't collide with theirs (the keyspaces
        // share an IGrainFactory but the leaves are addressed by GUID, so
        // collisions would only occur if we reused the leaf-id mapping;
        // separate keys are still cleaner for diagnosis).
        _deepKeys = new string[DeepKeyCount];
        for (var i = 0; i < DeepKeyCount; i++)
        {
            _deepKeys[i] = "d-" + i.ToString("D4", CultureInfo.InvariantCulture);
        }

        // Seed all keys so the tree splits into a depth-2 shape. The seed
        // phase runs in [GlobalSetup] and is not part of any [Benchmark]
        // op, so its cost does not affect measured allocations.
        for (var i = 0; i < DeepKeyCount; i++)
        {
            _deepLattice.SetAsync(_deepKeys[i], _value).GetAwaiter().GetResult();
        }

        // Pre-build the bulk batch with disjoint keys so BulkLoad_DeepTree
        // does not collide with the seeded keys (avoids triggering a
        // further root split inside the measured op).
        _deepBulkBatch = new List<KeyValuePair<string, byte[]>>(DeepBulkBatch);
        for (var i = 0; i < DeepBulkBatch; i++)
        {
            var k = "dbulk-" + i.ToString("D4", CultureInfo.InvariantCulture);
            _deepBulkBatch.Add(new(k, _value));
        }
    }

    /// <summary>
    /// Builds a sibling <see cref="ILattice"/> activation rooted at
    /// <see cref="DeeperTreeName"/> with both
    /// <see cref="TreeRegistryEntry.MaxLeafKeys"/> and
    /// <see cref="TreeRegistryEntry.MaxInternalChildren"/> pinned small
    /// (defaults 4 / 4 / 256-keys), forcing a tree with three internal
    /// levels above the leaves:
    /// <list type="bullet">
    ///   <item>level 0 (leaves): ⌈KeyCount/MaxLeafKeys⌉ = 64 nodes</item>
    ///   <item>level 1 (internal): ⌈64 / MaxInternalChildren⌉ = 16 nodes</item>
    ///   <item>level 2 (internal): ⌈16 / MaxInternalChildren⌉ = 4 nodes</item>
    ///   <item>level 3 (root, internal): ⌈4  / MaxInternalChildren⌉ = 1 node</item>
    /// </list>
    /// Every subsequent traversal for any key in the seeded range walks
    /// root → L2 → L1 → leaf, paying THREE
    /// <c>grainFactory.GetGrain&lt;IBPlusInternalGrain&gt;(internalId)</c>
    /// materialisations per call. The depth-2 <see cref="BuildDeepTree"/>
    /// pays one. All four shape parameters are env-overridable so a
    /// future agent can author a depth-N variant by env file alone.
    /// </summary>
    private void BuildDeeperTree()
    {
        _deeperMaxLeafKeys = ReadIntEnv("BENCH_MICROBENCH_DEEPER_MAX_LEAF_KEYS", DeeperMaxLeafKeysDefault);
        _deeperMaxInternalChildren = ReadIntEnv("BENCH_MICROBENCH_DEEPER_MAX_INTERNAL_CHILDREN", DeeperMaxInternalChildrenDefault);
        _deeperKeyCount = ReadIntEnv("BENCH_MICROBENCH_DEEPER_KEY_COUNT", DeeperKeyCountDefault);
        _deeperBulkBatch = ReadIntEnv("BENCH_MICROBENCH_DEEPER_BULK_BATCH", DeeperBulkBatchDefault);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(DeeperTreeName);
        registry.GetEntryAsync(DeeperTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _deeperMaxLeafKeys,
                MaxInternalChildren = _deeperMaxInternalChildren,
                ShardCount = 1,
            }));

        var deeperContext = Substitute.For<IGrainContext>();
        deeperContext.GrainId.Returns(GrainId.Create("lattice", DeeperTreeName));
        var deeperSp = Substitute.For<IServiceProvider>();
        var deeperLattice = new LatticeGrain(
            deeperContext,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            deeperSp,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(DeeperTreeName).Returns(deeperLattice);
        _deeperLattice = deeperLattice;

        // Distinct keyspace from the single-shard / fanout / deep trees so the
        // deeper-tree's seeded keys don't collide with theirs (the keyspaces
        // share an IGrainFactory but the leaves are addressed by GUID, so
        // collisions would only occur if we reused the leaf-id mapping;
        // separate keys are still cleaner for diagnosis).
        _deeperKeys = new string[_deeperKeyCount];
        for (var i = 0; i < _deeperKeyCount; i++)
        {
            _deeperKeys[i] = "deeper-" + i.ToString("D6", CultureInfo.InvariantCulture);
        }

        // Seed all keys so the tree splits into the target shape. The seed
        // phase runs in [GlobalSetup] and is not part of any [Benchmark]
        // op, so its cost does not affect measured allocations.
        for (var i = 0; i < _deeperKeyCount; i++)
        {
            _deeperLattice.SetAsync(_deeperKeys[i], _value).GetAwaiter().GetResult();
        }

        // Pre-build the bulk batch with disjoint keys so BulkLoad_DeeperTree
        // does not collide with the seeded keys (avoids triggering a
        // further internal-node split inside the measured op).
        _deeperBulkBatchList = new List<KeyValuePair<string, byte[]>>(_deeperBulkBatch);
        for (var i = 0; i < _deeperBulkBatch; i++)
        {
            var k = "deeperbulk-" + i.ToString("D6", CultureInfo.InvariantCulture);
            _deeperBulkBatchList.Add(new(k, _value));
        }
    }

    /// <summary>
    /// Builds a sibling <see cref="ILattice"/> activation rooted at
    /// <see cref="AtomicTreeName"/> as a single-shard, root-is-leaf tree
    /// dedicated to <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// measurement. The keyspace ("atomic-NNNNNNNN") is disjoint from the
    /// other benchmark trees so saga writes don't collide with their
    /// pre-seeded data. Pre-seeds the keys once so the saga's
    /// per-key SetAsync calls overwrite existing entries (LWW) instead
    /// of triggering a leaf split mid-iteration.
    /// </summary>
    private void BuildAtomicTree()
    {
        var atomicBatchSize = ReadIntEnv("BENCH_MICROBENCH_ATOMIC_BATCH", AtomicBatchDefault);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(AtomicTreeName);
        registry.GetEntryAsync(AtomicTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = 1,
            }));

        var atomicContext = Substitute.For<IGrainContext>();
        atomicContext.GrainId.Returns(GrainId.Create("lattice", AtomicTreeName));
        var atomicSp = Substitute.For<IServiceProvider>();
        var atomicLattice = new LatticeGrain(
            atomicContext,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            atomicSp,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(AtomicTreeName).Returns(atomicLattice);
        _atomicLattice = atomicLattice;

        // Pre-build the saga batch with disjoint keys ("atomic-NNNNNNNN")
        // so the saga writes against this tree don't collide with the
        // single-shard / fanout / deep / deeper trees' keyspaces.
        _atomicBatch = new List<KeyValuePair<string, byte[]>>(atomicBatchSize);
        for (var i = 0; i < atomicBatchSize; i++)
        {
            var k = "atomic-" + i.ToString("D8", CultureInfo.InvariantCulture);
            _atomicBatch.Add(new(k, _value));
        }

        // Pre-seed the same keys so the first benchmarked saga overwrites
        // existing leaf entries rather than triggering a leaf split (the
        // single-shard tree is sized to accommodate the whole batch, but
        // pre-seeding still removes one-time leaf-create allocations from
        // the steady-state measurement).
        for (var i = 0; i < atomicBatchSize; i++)
        {
            _atomicLattice.SetAsync(_atomicBatch[i].Key, _value).GetAwaiter().GetResult();
        }
    }

    private static int ReadIntEnv(string name, int fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value)) return fallback;
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
        {
            return result;
        }
        Console.Error.WriteLine($"[microbench] invalid {name} value: {value}");
        return fallback;
    }
}
