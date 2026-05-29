using System.Buffers;
using System.Globalization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Benchmark.Microbench.Profiling;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Storage.AzureTable;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Timers;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// BenchmarkDotNet workloads that exercise the public <see cref="ILattice"/>
/// surface end-to-end - shard-map lookup, <see cref="IShardRootGrain"/>
/// dispatch, and the leaf-grain primitive - through hand-instantiated grains
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

    // GetMany batch keys for the PointGetMany benchmark. A small fixed-size
    // batch (4 keys) drawn from the pre-seeded _keys[] keyspace. Sized to
    // mirror a typical user-facing multi-get rather than the bulk-load
    // surface BulkLoad already exercises; isolates the
    // LatticeGrain.GetManyAsyncCore state-machine allocation rather than
    // the per-shard fan-out cost.
    private List<string> _getManyBatch = null!;

    // Phase R (c2-viii open question (c)): pre-built GetMany batches at
    // a sweep of small sizes. PointGetMany_BatchSize uses these to answer
    // "what is the per-call gain ratio as batch size grows?" without
    // per-iteration List<string> allocation noise. Indexed by enum-style
    // size constant (1, 2, 4, 8, 16, 32, 64) so the Arguments attribute
    // can dispatch by ordinal. Each batch is drawn from the seeded
    // keyspace, identity-stable across iterations, and shares the same
    // allocation discipline as _getManyBatch.
    private Dictionary<int, List<string>> _getManyBatchesBySize = null!;
    private static readonly int[] GetManyBatchSizesSweep = { 1, 2, 4, 8, 16, 32, 64 };

    private int _writeCursor;
    private int _readCursor;
    private int _mixedCursor;
    private int _getManyCursor;

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
    //      instruments - the operator-visible signal for sustained
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

    // ===== F-055 acceptance instruments: 4-shard atomic, concurrent atomic, read-under-saga =====
    // The single-shard SetManyAtomic above measures the saga-vertical cost on the
    // best-case topology (one shard, one terminal-broadcast target). F-055's
    // acceptance text additionally requires:
    //   (a) saga-RPS scales linearly with batch concurrency - measured by
    //       SetManyAtomic_Concurrent over [Arguments(1, 4, 16, 64)]; the per-saga
    //       grain RPS bound (rather than per-tree) means N independent sagas
    //       should achieve ~N× the throughput up to the activation budget.
    //   (b) the WAL-only prepare path is no slower than v3.4.0 single-key write
    //       throughput multiplied by batch size - measured by the per-second
    //       slug ratio between SetManyAtomic and PointWrite.
    //   (c) GetAsync shows no measurable latency regression versus v3.4.0
    //       baseline when no saga is in flight - measured by
    //       PointRead_AtomicTreeIdle, which routes through the same atomic-write
    //       code path but with empty per-leaf pending-tx buckets.
    // The 4-shard variant additionally exercises the multi-terminal-broadcast
    // fan-out path that the single-shard tree cannot reach.
    private const string AtomicFanoutTreeName = "microbench-atomic-fanout";
    private const int AtomicFanoutShardCount = 4;
    private const int AtomicConcurrentSagaCount = 64;
    private ILattice _atomicFanoutLattice = null!;
    private List<KeyValuePair<string, byte[]>> _atomicFanoutBatch = null!;
    private List<KeyValuePair<string, byte[]>>[] _atomicConcurrentBatches = null!;
    private string[] _atomicReadKeys = null!;
    private int _atomicReadCursor;
    private Guid _atomicPendingTxId;

    // ===== EventPipe-driven per-method profiler =====
    // Lifecycle:
    //   * GlobalSetupCore() instantiates the profiler at the END of seeding so
    //     setup allocations (10k pre-seed writes etc.) don't dominate the
    //     attribution. The factory returns null when
    //     BENCH_MICROBENCH_PROFILE=off (default) or when EventPipe cannot
    //     be opened on the host, so the harness path is a complete no-op
    //     under normal cohort runs.
    //   * GlobalCleanup() stops the session, post-processes the captured
    //     .nettrace, and writes profile.json next to the harness results.json.
    // The profiler perturbs measurement (per-event stack-walking inside the
    // runtime), so PROFILE=alloc|cpu|both runs are diagnostic snapshots, not
    // comparable cohort baselines. The harness documentation calls this out.
    private BenchmarkProfiler? _profiler;

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
        // we want - the bench is not measuring tombstone compaction or hot
        // shard monitoring.
        var compaction = Substitute.For<ITombstoneCompactionGrain>();
        _grainFactory.GetGrain<ITombstoneCompactionGrain>(Arg.Any<string>()).Returns(compaction);

        var monitor = Substitute.For<IHotShardMonitorGrain>();
        _grainFactory.GetGrain<IHotShardMonitorGrain>(Arg.Any<string>()).Returns(monitor);

        var stats = Substitute.For<ILatticeStats>();
        _grainFactory.GetGrain<ILatticeStats>(Arg.Any<string>()).Returns(stats);

        // The resolver depends on the factory + monitor - same singleton
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

        // Fixed 4-key GetMany batch drawn from the pre-seeded keyspace.
        // The batch is reused across iterations; rotating offsets via
        // _getManyCursor would only add a per-iteration allocation
        // (a fresh List<string>) that would mask the state-machine
        // delta we are measuring. Identity batch keeps the alloc
        // baseline stable.
        var getManyBatchSize = Math.Min(4, _keys.Length);
        _getManyBatch = new List<string>(getManyBatchSize);
        for (var i = 0; i < getManyBatchSize; i++)
        {
            _getManyBatch.Add(_keys[i]);
        }

        // Phase R: pre-build the sweep batches. Each batch's keys are
        // drawn contiguously from _keys[] so larger batches strictly
        // superset smaller ones, which means shard-fan-out grows
        // monotonically with batch size. Capped at _keys.Length so a
        // small BENCH_MICROBENCH_KEY_COUNT does not over-request.
        _getManyBatchesBySize = new Dictionary<int, List<string>>(GetManyBatchSizesSweep.Length);
        foreach (var size in GetManyBatchSizesSweep)
        {
            var capped = Math.Min(size, _keys.Length);
            var batch = new List<string>(capped);
            for (var i = 0; i < capped; i++)
            {
                batch.Add(_keys[i]);
            }
            _getManyBatchesBySize[size] = batch;
        }

        BuildFanoutTree(keyCount, bulkBatch);
        BuildDeepTree();
        BuildDeeperTree();
        BuildAtomicTree();
        BuildAtomicFanoutTree();
        BuildAtomicConcurrentBatches();
        BuildAtomicReadFixture();
        BuildWalEncodeFixture();
        BuildShipFramingFixture();

        // Last step in setup: open the optional EventPipe profile session.
        // We do this AFTER seeding so the multi-thousand pre-seed writes
        // (which dominate alloc volume by 2+ orders of magnitude) don't
        // drown out the in-loop benchmark allocations in the top-N table.
        _profiler = TryStartProfiler();
    }

    /// <summary>
    /// Stops the optional EventPipe profile session (no-op when profiling is
    /// disabled). Writes the per-method attribution sidecar
    /// <c>profile.json</c> next to the harness <c>results.json</c>.
    /// </summary>
    [GlobalCleanup]
    public void GlobalCleanup()
    {
        try
        {
            _profiler?.Stop();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[microbench] GlobalCleanup failed: {ex}");
        }
    }

    /// <summary>
    /// Resolves the <c>profile.json</c> output path next to the harness
    /// <c>results.json</c> (env <c>BENCH_RESULTS_PATH</c>) and starts the
    /// EventPipe-driven profiler. Returns <see langword="null"/> when
    /// profiling is disabled or the session cannot be opened.
    /// </summary>
    private static BenchmarkProfiler? TryStartProfiler()
    {
        var resultsPath = Environment.GetEnvironmentVariable("BENCH_RESULTS_PATH");
        string profilePath;
        if (string.IsNullOrWhiteSpace(resultsPath))
        {
            profilePath = Path.Combine(Directory.GetCurrentDirectory(), "profile.json");
        }
        else
        {
            var dir = Path.GetDirectoryName(resultsPath) ?? Directory.GetCurrentDirectory();
            profilePath = Path.Combine(dir, "profile.json");
        }
        var runId = Environment.GetEnvironmentVariable("BENCH_RUN_ID") ?? string.Empty;
        var gitSha = Environment.GetEnvironmentVariable("BENCH_GIT_SHA") ?? string.Empty;
        return BenchmarkProfiler.TryStart(profilePath, runId, gitSha);
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
        var leaf = new BPlusLeafGrain(ctx, state, _grainFactory, _optionsResolver, _observers, new DefaultLatticeOriginClusterIdResolver());
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
        var cache = new LeafCacheGrain(ctx, _grainFactory, _optionsMonitor, new DefaultLatticeOriginClusterIdResolver());
        _leafCaches[leafIdString] = cache;
        return cache;
    }

    /// <summary>
    /// Lazily constructs and caches a real <see cref="BPlusInternalGrain"""
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
        var registry = new TxRegistryGrain(ctx, _optionsMonitor, registryState);
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

    /// <summary>
    /// Single <see cref="ILattice.GetWithVersionAsync(string, CancellationToken)"/>
    /// against a rotating key. Mirrors <see cref="PointRead"/> but drives the
    /// version-returning traversal path
    /// (<c>ShardRootGrain.TraverseForReadWithVersionAsync</c>) so allocation
    /// optimisations targeting that sibling helper have an empirical
    /// validation lane. Reuses <c>_readCursor</c> since the rotating-key
    /// pattern is identical to <see cref="PointRead"/>.
    /// </summary>
    [Benchmark(Description = "Point read with version")]
    public Task<VersionedValue> PointReadWithVersion()
    {
        var i = unchecked(_readCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.GetWithVersionAsync(key);
    }

    /// <summary>
    /// Single <see cref="ILattice.ExistsAsync(string, CancellationToken)"/>
    /// against a rotating key. Mirrors <see cref="PointRead"/> but drives the
    /// existence-check traversal path
    /// (<c>ShardRootGrain.TraverseForExistsAsync</c>) so allocation
    /// optimisations targeting that sibling helper have an empirical
    /// validation lane. Reuses <c>_readCursor</c> since the rotating-key
    /// pattern is identical to <see cref="PointRead"/>.
    /// </summary>
    [Benchmark(Description = "Point exists")]
    public Task<bool> PointExists()
    {
        var i = unchecked(_readCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.ExistsAsync(key);
    }

    /// <summary>
    /// Single <see cref="ILattice.GetManyAsync(List{string}, CancellationToken)"/>
    /// against a pre-built fixed 4-key batch drawn from the seeded keyspace.
    /// Drives the multi-key fan-out path
    /// (<c>LatticeGrain.GetManyAsyncCore</c> -> per-shard
    /// <c>IShardRootGrain.GetManyAsync</c> -> <c>LeafCacheGrain.GetManyAsync</c>)
    /// so allocation optimisations targeting <c>GetManyAsyncCore</c>
    /// (e.g. pooling the state-machine box) have an empirical validation
    /// lane. The batch list is shared across iterations so per-iteration
    /// allocation is dominated by the state machine + the result dictionary
    /// rather than batch construction.
    /// </summary>
    [Benchmark(Description = "Point get many")]
    public Task<Dictionary<string, byte[]>> PointGetMany()
    {
        // Increment cursor to keep the structural pattern symmetric with
        // PointRead / PointExists; the result is not used to index because
        // the batch is intentionally identity-stable for allocation
        // baseline reasons (see _getManyBatch comment).
        _ = unchecked(_getManyCursor++) & int.MaxValue;
        return _lattice.GetManyAsync(_getManyBatch);
    }

    /// <summary>
    /// Phase R (c2-viii open question (c)): sweep
    /// <see cref="ILattice.GetManyAsync(System.Collections.Generic.List{string}, System.Threading.CancellationToken)"/>
    /// across batch sizes <c>{1, 2, 4, 8, 16, 32, 64}</c> to surface
    /// the per-call gain ratio as batch size grows. At <c>size=1</c> the
    /// benchmark exercises the same single-key path as <see cref="PointRead"/>
    /// but through the multi-key surface (a List with one element), so
    /// the diff between the two rows isolates the
    /// <c>LatticeGrain.GetManyAsyncCore</c> shard-bucket dictionary +
    /// double-snapshot retry overhead from the underlying read cost.
    /// Larger sizes surface the per-shard fan-out scaling. Batches are
    /// pre-built in <see cref="GlobalSetup"/> and reused identity-stable
    /// across iterations so per-iteration allocation reflects only the
    /// async state machine + result dictionary.
    /// </summary>
    [Benchmark(Description = "Point get many sweep")]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(4)]
    [Arguments(8)]
    [Arguments(16)]
    [Arguments(32)]
    [Arguments(64)]
    public Task<Dictionary<string, byte[]>> PointGetMany_BatchSize(int batchSize)
    {
        return _lattice.GetManyAsync(_getManyBatchesBySize[batchSize]);
    }

    /// <summary>
    /// One <see cref="ILattice.SetManyAsync"/> invocation flushing the pre-built batch.</summary>
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
    /// <see cref="ILattice.KeysAsync(string, string, bool, bool?, CancellationToken)"""
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
    /// once before reaching the target leaf - the call site this cycle
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
    /// at every level above the leaves - at default sizing
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
    /// One <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// invocation against a four-shard atomic-write tree. Exercises the
    /// multi-shard terminal-broadcast fan-out path that the single-shard
    /// <see cref="SetManyAtomic"/> bench cannot reach: every saga issues
    /// <c>ShardCount</c> distinct
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> calls in the
    /// terminal phase, and the per-key Execute fan-out under
    /// <see cref="LatticeAtomicBatchContext"/> walks shards in proportion
    /// to key entropy. Surfaced as
    /// <c>microbench_set_many_atomic_4_shards_per_second</c>.
    /// </summary>
    [Benchmark(Description = "SetMany atomic 4 shards")]
    public Task SetManyAtomic_4Shards()
    {
        return _atomicFanoutLattice.SetManyAtomicAsync(_atomicFanoutBatch);
    }

    /// <summary>
    /// Drives <paramref name="concurrency"/> independent sagas concurrently
    /// against the single-shard atomic-write tree, awaiting all in
    /// parallel via <see cref="Task.WhenAll(IEnumerable{Task})"/>. Each
    /// saga uses a disjoint key partition so they don't abort each other.
    /// Per F-055 acceptance: saga-RPS should scale roughly linearly with
    /// <paramref name="concurrency"/> up to the per-saga
    /// <see cref="IAtomicWriteGrain"/> activation budget, validating that
    /// concurrency is bounded by Orleans grain RPS (per-saga, not
    /// per-tree) rather than capped at single-grain throughput.
    /// </summary>
    [Benchmark(Description = "SetMany atomic concurrent")]
    [Arguments(1)]
    [Arguments(4)]
    [Arguments(16)]
    [Arguments(64)]
    public Task SetManyAtomic_Concurrent(int concurrency)
    {
        if (concurrency == 1)
        {
            return _atomicLattice.SetManyAtomicAsync(_atomicConcurrentBatches[0]);
        }
        var tasks = new Task[concurrency];
        for (var i = 0; i < concurrency; i++)
        {
            tasks[i] = _atomicLattice.SetManyAtomicAsync(_atomicConcurrentBatches[i]);
        }
        return Task.WhenAll(tasks);
    }

    /// <summary>
    /// Single <see cref="ILattice.GetAsync(string, CancellationToken)"/>
    /// against the single-shard atomic-write tree with no saga in flight.
    /// Validates F-055 acceptance: GetAsync shows no measurable latency
    /// regression versus v3.4.0 baseline when no saga is in flight. The
    /// atomic tree's leaves carry the same projection-cache + pending-tx
    /// structures the saga path populates, but the steady state has
    /// empty <c>_pendingTx</c> buckets - this benchmark confirms the
    /// read fast-path skips the empty-bucket consultation cleanly.
    /// </summary>
    [Benchmark(Description = "Point read atomic tree (idle)")]
    public Task<byte[]?> PointRead_AtomicTreeIdle()
    {
        var i = unchecked(_atomicReadCursor++) & int.MaxValue;
        var key = _atomicReadKeys[i % _atomicReadKeys.Length];
        return _atomicLattice.GetAsync(key);
    }

    /// <summary>
    /// Single <see cref="ILattice.GetAsync(string, CancellationToken)"/>
    /// against the single-shard atomic-write tree while a long-lived
    /// saga holds prepared mutations on a disjoint key set. Validates
    /// that an in-flight saga on one key partition does not regress
    /// reads against the rest of the tree - the per-leaf pending-tx
    /// consultation is keyed and short-circuits when the read key has
    /// no entry in <c>_pendingTx</c>. The pending mutations are
    /// installed in <see cref="GlobalSetup"/> via
    /// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> on a
    /// synthetic transaction id; no terminal mark is ever broadcast,
    /// so the saga remains active for every benchmark iteration.
    /// </summary>
    [Benchmark(Description = "Point read atomic tree (saga active)")]
    public Task<byte[]?> PointRead_AtomicTreeWithActiveSaga()
    {
        var i = unchecked(_atomicReadCursor++) & int.MaxValue;
        var key = _atomicReadKeys[i % _atomicReadKeys.Length];
        return _atomicLattice.GetAsync(key);
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

        // Pre-build the saga batch with disjoint keys so the saga's per-key
        // Execute fan-out walks all four physical shards (the shard-routing
        // function is content-hashed on the key, and a multiplicative-prefix
        // entropy distributes 16 keys across >= 2 shards with high probability).
        _atomicBatch = new List<KeyValuePair<string, byte[]>>(atomicBatchSize);
        for (var i = 0; i < atomicBatchSize; i++)
        {
            var k = "atomic-" + ((i * 257) & 0xFFFF).ToString("X4", CultureInfo.InvariantCulture)
                + "-" + i.ToString("D8", CultureInfo.InvariantCulture);
            _atomicBatch.Add(new(k, _value));
        }

        // Pre-seed the same keys so the first benchmarked saga overwrites
        // existing leaf entries rather than triggering a leaf split.
        for (var i = 0; i < atomicBatchSize; i++)
        {
            _atomicLattice.SetAsync(_atomicBatch[i].Key, _value).GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Builds a sibling <see cref="ILattice"/> activation rooted at
    /// <see cref="AtomicFanoutTreeName"/> with <see cref="AtomicFanoutShardCount"/>
    /// physical shards. Each shard is a single-shard, root-is-leaf tree
    /// dedicated to <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// measurement. The keyspaces ("atomic-NNNNNNNN") are disjoint from the
    /// other benchmark trees so concurrent saga writes don't collide with
    /// their pre-seeded data. Pre-seeds the keys once so the sagas'
    /// per-key SetAsync calls overwrite existing entries (LWW) instead
    /// of triggering a leaf split mid-iteration.
    /// </summary>
    private void BuildAtomicFanoutTree()
    {
        var atomicBatchSize = ReadIntEnv("BENCH_MICROBENCH_ATOMIC_BATCH", AtomicBatchDefault);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(AtomicFanoutTreeName);
        registry.GetEntryAsync(AtomicFanoutTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = AtomicFanoutShardCount,
            }));

        var atomicFanoutContext = Substitute.For<IGrainContext>();
        atomicFanoutContext.GrainId.Returns(GrainId.Create("lattice", AtomicFanoutTreeName));
        var atomicFanoutSp = Substitute.For<IServiceProvider>();
        var atomicFanoutLattice = new LatticeGrain(
            atomicFanoutContext,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            atomicFanoutSp,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(AtomicFanoutTreeName).Returns(atomicFanoutLattice);
        _atomicFanoutLattice = atomicFanoutLattice;

        // Pre-build the fanout-specific batch with shard-spreading keys so
        // the saga's per-key Execute fan-out walks all four physical shards
        // (the shard-routing function is content-hashed on the key, and
        // a multiplicative-prefix entropy distributes 16 keys across >= 2
        // shards with high probability).
        _atomicFanoutBatch = new List<KeyValuePair<string, byte[]>>(atomicBatchSize);
        for (var i = 0; i < atomicBatchSize; i++)
        {
            var k = "atomic-fan-" + ((i * 257) & 0xFFFF).ToString("X4", CultureInfo.InvariantCulture)
                + "-" + i.ToString("D8", CultureInfo.InvariantCulture);
            _atomicFanoutBatch.Add(new(k, _value));
        }

        // Pre-seed the same keys so the first benchmarked saga overwrites
        // existing leaf entries rather than triggering a leaf split.
        for (var i = 0; i < atomicBatchSize; i++)
        {
            _atomicFanoutLattice.SetAsync(_atomicFanoutBatch[i].Key, _value).GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Builds <see cref="AtomicConcurrentSagaCount"/> disjoint
    /// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// batches against the single-shard atomic tree. Concurrent sagas
    /// must use disjoint key sets, otherwise the registry-side
    /// linearisation aborts the second saga to commit (LWW on prepared
    /// values is undefined for overlapping keys), which would distort
    /// the throughput measurement. Each batch holds
    /// <c>BENCH_MICROBENCH_ATOMIC_BATCH</c> entries.
    /// </summary>
    private void BuildAtomicConcurrentBatches()
    {
        var atomicBatchSize = ReadIntEnv("BENCH_MICROBENCH_ATOMIC_BATCH", AtomicBatchDefault);

        _atomicConcurrentBatches = new List<KeyValuePair<string, byte[]>>[AtomicConcurrentSagaCount];
        for (var i = 0; i < AtomicConcurrentSagaCount; i++)
        {
            var batch = new List<KeyValuePair<string, byte[]>>(atomicBatchSize);
            for (var j = 0; j < atomicBatchSize; j++)
            {
                var k = "atomic-conc-"
                    + i.ToString("D2", CultureInfo.InvariantCulture) + "-"
                    + j.ToString("D8", CultureInfo.InvariantCulture);
                batch.Add(new(k, _value));
            }
            _atomicConcurrentBatches[i] = batch;

            // Pre-seed every concurrent saga's keys against the single-shard
            // atomic tree so concurrent sagas never trigger a leaf split
            // mid-iteration. Without pre-seeding, the first concurrent run
            // would pay one-time leaf-create allocations that distort the
            // steady-state measurement.
            for (var j = 0; j < atomicBatchSize; j++)
            {
                _atomicLattice.SetAsync(batch[j].Key, _value).GetAwaiter().GetResult();
            }
        }
    }

    /// <summary>
    /// Builds the read-path fixture for
    /// <see cref="PointRead_AtomicTreeIdle"/> and
    /// <see cref="PointRead_AtomicTreeWithActiveSaga"/>. Pre-seeds a
    /// dedicated key range ("atomic-read-NNNNNNNN") on the single-shard
    /// atomic tree so the read benchmarks rotate over keys distinct
    /// from any saga keyspace, then installs a long-lived pending
    /// mutation on a separate "saga-pending-NNN" key set via
    /// <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> under
    /// a synthetic transaction id. No terminal mark is ever broadcast,
    /// so the leaf's <c>_pendingTx</c> bucket stays non-empty across
    /// every <see cref="PointRead_AtomicTreeWithActiveSaga"/> iteration
    /// - the read path's "is there a pending entry for this key?"
    /// short-circuit is exercised continuously.
    /// </summary>
    private void BuildAtomicReadFixture()
    {
        const int ReadKeyCount = 1024;
        _atomicReadKeys = new string[ReadKeyCount];
        for (var i = 0; i < ReadKeyCount; i++)
        {
            _atomicReadKeys[i] = "atomic-read-" + i.ToString("D8", CultureInfo.InvariantCulture);
        }

        // Pre-seed read keys on the atomic tree so PointRead_AtomicTreeIdle
        // resolves through the visible projection (not a cache miss).
        for (var i = 0; i < ReadKeyCount; i++)
        {
            _atomicLattice.SetAsync(_atomicReadKeys[i], _value).GetAwaiter().GetResult();
        }

        // Install a long-lived pending mutation on a disjoint key set so
        // the leaf's per-tx pending bucket stays non-empty for every
        // PointRead_AtomicTreeWithActiveSaga iteration. The receiver-side
        // ApplyPreparedSetAsync surface routes the entry into _pendingTx
        // without driving a saga coordinator (no AtomicWriteGrain, no
        // TxRegistryGrain mark), so the saga "stays open" indefinitely.
        _atomicPendingTxId = Guid.NewGuid();
        var applyGrain = (IReplicationApplyGrain)_atomicLattice;
        const int PendingKeyCount = 8;
        for (var i = 0; i < PendingKeyCount; i++)
        {
            var key = "atomic-pending-" + i.ToString("D4", CultureInfo.InvariantCulture);
            var hlc = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
            applyGrain.ApplyPreparedSetAsync(
                key,
                _value,
                hlc,
                originClusterId: "microbench-source",
                sourceVectorClock: null,
                expiresAtTicks: 0,
                transactionId: _atomicPendingTxId,
                atomicBatchSize: PendingKeyCount,
                atomicBatchIndex: i).GetAwaiter().GetResult();
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

    // ===== WAL Azure-Table encode-batch instrument =====
    // Drives Orleans.Lattice.Storage.AzureTable.AzureTableWalStorageProvider's
    // per-entry encode loop in-process, without going through TableClient
    // or the Azurite endpoint. The benchmark exercises the exact code path
    // AppendBatchAsync runs between batch validation and the
    // SubmitTransactionAsync await: one Serializer<LatticeMutation>.Serialize
    // call into an ArrayBufferWriter<byte>, one WrittenSpan.ToArray, and
    // one AzureTableWalEntity construction per WalEntry. This is the
    // allocation-shaped hot path for any future pooled-writer optimisation
    // on the WAL provider.
    //
    // Method-Provider seam: the bench reaches the loop through the
    // internal helper EncodeEntriesForBatch, exposed via the provider's
    // InternalsVisibleTo on this assembly. The List<TableTransactionAction>
    // is freshly allocated per invocation (mirroring AppendBatchAsync)
    // and is excluded from the measurement only inasmuch as BDN reports
    // total bytes-per-op; the per-entry allocations are what dominate.
    //
    // Parameter sweep: 1, 10, 50, 99 entries. Azure Tables caps a single
    // partition transaction at 100 actions and the provider reserves one
    // for the HEAD sentinel, so 99 is the maximum legal entry batch.
    private AzureTableWalStorageProvider _walProvider = null!;
    private WalEntry[] _walEncodeEntries = null!;
    private string _walEncodePartitionKey = null!;

    private void BuildWalEncodeFixture()
    {
        // Build a self-contained ServiceProvider that exposes the Orleans
        // Serializer<LatticeMutation> the provider needs. The rest of the
        // microbench setup uses NSubstitute mocks for grain seams, but
        // the WAL encode path does not touch IGrainFactory - only the
        // serializer - so a minimal AddSerializer() container is enough.
        var sp = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        var serializer = (Serializer<WalRecord>)sp.GetService(typeof(Serializer<WalRecord>))!;

        var options = Microsoft.Extensions.Options.Options.Create(new AzureTableWalStorageOptions
        {
            // The encode path never inspects ConnectionString / TableName -
            // those are only touched by EnsureTableAsync. Setting a benign
            // value here keeps the option validator from complaining.
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "BenchEncodeProbe",
        });
        _walProvider = new AzureTableWalStorageProvider(options, serializer);

        const int MaxEntries = 99;
        var valueBytes = ReadIntEnv("BENCH_MICROBENCH_VALUE_BYTES", 128);
        var payload = new byte[valueBytes];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i & 0xFF);
        }

        _walEncodeEntries = new WalEntry[MaxEntries];
        var hlc = HybridLogicalClock.Zero;
        for (var i = 0; i < MaxEntries; i++)
        {
            hlc = HybridLogicalClock.Tick(hlc);
            var mutation = new LatticeMutation
            {
                TreeId = "wal-encode-bench",
                Kind = MutationKind.Set,
                Key = "k-" + i.ToString("D6", CultureInfo.InvariantCulture),
                Value = payload,
                Timestamp = hlc,
                IsTombstone = false,
                ExpiresAtTicks = 0L,
                OriginClusterId = "microbench-source",
                Category = MutationCategory.User,
                TransactionId = Guid.Empty,
                AtomicBatchSize = 0,
                AtomicBatchIndex = 0,
                IsPrepared = false,
                ShardIndex = 0,
                IsBackstop = false,
                AtomicShardCount = 0,
                IsMerge = false,
            };
            _walEncodeEntries[i] = new WalEntry { Offset = i, Mutation = mutation };
        }

        _walEncodePartitionKey = AzureTableWalStorageProvider.BuildPartitionKey("wal-encode-bench", 0);
    }

    /// <summary>
    /// Drives <see cref="AzureTableWalStorageProvider.EncodeEntriesForBatch"/>
    /// over a pre-built <see cref="WalEntry"/> array of length
    /// <paramref name="entryCount"/>. Each invocation encodes the same
    /// pre-built entries into a fresh
    /// <c>List&lt;TableTransactionAction&gt;</c>, so the measurement
    /// surfaces:
    /// <list type="bullet">
    ///   <item>The Orleans Serializer&lt;LatticeMutation&gt; allocation per entry (currently a fresh <c>ArrayBufferWriter&lt;byte&gt;</c>).</item>
    ///   <item>The <c>WrittenSpan.ToArray()</c> copy per entry.</item>
    ///   <item>The <c>AzureTableWalEntity</c> object construction per entry.</item>
    ///   <item>The <c>TableTransactionAction</c> wrapper construction per entry.</item>
    /// </list>
    /// Pre-iteration setup (the entries array, the partition key, the
    /// serializer) is built once in <see cref="GlobalSetup"/> so the
    /// reported alloc-bytes/op are dominated by the per-entry encode
    /// pattern rather than fixture costs.
    /// </summary>
    [Benchmark(Description = "WAL encode batch (Azure Table)")]
    [Arguments(1)]
    [Arguments(10)]
    [Arguments(50)]
    [Arguments(99)]
    public List<global::Azure.Data.Tables.TableTransactionAction> EncodeWalBatch_AzureTable(int entryCount)
    {
        // Slice to the parameterised batch size. ArraySegment<T> avoids an
        // allocation here and IReadOnlyList<WalEntry> is satisfied because
        // the encode helper accepts the interface. However the helper is
        // typed against IReadOnlyList<WalEntry>; ArraySegment<T> does not
        // implement that directly, so use the simpler approach of passing
        // a freshly-allocated array view via the array's segment-as-list
        // wrapper. The cheapest and most explicit option here is a
        // pre-sized List<WalEntry> built once per call - the cost is
        // captured by the measurement and is what the production
        // AppendBatchAsync caller pays today (it receives an
        // IReadOnlyList<WalEntry> from the WAL grain).
        var slice = new List<WalEntry>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            slice.Add(_walEncodeEntries[i]);
        }
        var actions = new List<global::Azure.Data.Tables.TableTransactionAction>(entryCount + 1);
        _walProvider.EncodeEntriesForBatch(_walEncodePartitionKey, slice, actions);
        return actions;
    }

    // ===== Composite ship-path A/B (typed envelope vs framing-only) =====
    //
    // Surfaces the allocation delta on the producer-side leg of the
    // shipper -> gRPC marshaller composite path between the historical
    // typed-envelope encode (OrleansBinaryReplicationBatchEncoder.Encode,
    // which Orleans-serialises a ReplicationBatchEnvelope { WalRecord[] }
    // element-wise) and the R-114 framing-only encode
    // (IReplicationBatchEncoder.EncodeFraming, which writes a 32-byte
    // fixed header plus length-prefixed pre-encoded entry segments).
    //
    // Both methods drain the same N pre-encoded WAL entries through the
    // same encoder instance, so the only difference between the two
    // measurements is the encode strategy. The fixture pre-encodes the
    // entries via OrleansBinaryWalRecordEncoder once in [GlobalSetup] -
    // mirroring the production reality that the shipper retrieves
    // already-encoded bytes from IWalStorageProvider.ReadEncodedAsync
    // (the unconditional shipper path post R-114/R-115); the typed-
    // envelope baseline shows what the shipper used to pay before
    // R-114 - re-encoding every entry through the envelope-level
    // Orleans serializer call - while the framing-only path hands the
    // existing segments to the marshaller verbatim.
    //
    // Parameter sweep: 16 / 64 / 256 / 1024 entries at 64 B / 1 KB /
    // 16 KB payload. The 1024 x 64 B and 16 x 16 KB corners answer the
    // roadmap acceptance row's small-payload-dominated and
    // payload-bytes-dominated extremes respectively.

    private Serializer<ReplicationBatchEnvelope> _shipEnvelopeSerializer = null!;
    private IReplicationBatchEncoder _shipFramingEncoder = null!;
    // [payloadIndex, entryCount-index] -> pre-encoded entries in a
    // ReplicationBatchEnvelope (typed) and as ArraySegment<byte>[]
    // (framing). Indexed by payload-size bucket then entry-count bucket
    // so the parameterised benchmark can pick a fixture pair without
    // rebuilding it per invocation.
    private ReplicationBatchEnvelope[,] _shipTypedEnvelopes = null!;
    private ArraySegment<byte>[,][] _shipFramingSegments = null!;
    private static readonly int[] ShipPayloadBytes = [64, 1024, 16 * 1024];
    private static readonly int[] ShipEntryCounts = [16, 64, 256, 1024];

    private void BuildShipFramingFixture()
    {
        // Self-contained ServiceProvider for the two Orleans serializers
        // the fixture needs - one for the per-entry WalRecord encode
        // (driven through OrleansBinaryWalRecordEncoder, replicating the
        // producer-site append), and one for the envelope-level encode
        // that the typed-envelope shipper path drives directly. The
        // production OrleansBinaryReplicationBatchEncoder.Encode method
        // is internal to Orleans.Lattice.Replication; the benchmark
        // calls Serializer<ReplicationBatchEnvelope>.Serialize directly,
        // which is exactly what that method does internally (and what
        // determines its allocation profile - one per-entry Orleans
        // codec call element-wise). For the framing-only path the
        // benchmark uses a 4-line in-bench IReplicationBatchEncoder
        // stub so the default interface method's framing implementation
        // dispatches correctly.
        var sp = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        var walSerializer = (Serializer<WalRecord>)sp.GetService(typeof(Serializer<WalRecord>))!;
        var walEncoder = new OrleansBinaryWalRecordEncoder(walSerializer);
        _shipEnvelopeSerializer = (Serializer<ReplicationBatchEnvelope>)sp.GetService(typeof(Serializer<ReplicationBatchEnvelope>))!;
        _shipFramingEncoder = new BenchFramingEncoder();

        _shipTypedEnvelopes = new ReplicationBatchEnvelope[ShipPayloadBytes.Length, ShipEntryCounts.Length];
        _shipFramingSegments = new ArraySegment<byte>[ShipPayloadBytes.Length, ShipEntryCounts.Length][];

        for (var pi = 0; pi < ShipPayloadBytes.Length; pi++)
        {
            var payloadSize = ShipPayloadBytes[pi];

            for (var ci = 0; ci < ShipEntryCounts.Length; ci++)
            {
                var entryCount = ShipEntryCounts[ci];
                var hlc = HybridLogicalClock.Zero;

                // Build the per-batch WalRecord array first; we need the
                // strongly-typed records for the typed-envelope path,
                // and the same records re-projected to ArraySegment<byte>
                // for the framing path. This mirrors what the shipper
                // sees: segment bytes from ReadShippingAsync are the
                // sole input today, but the typed-envelope baseline
                // needs the strongly-typed records the pre-R-114
                // shipper drained. Encoding per-entry through the WAL
                // record encoder reproduces the exact bytes the in-memory
                // and Azure-Table providers retain.
                //
                // Critical: every entry gets its OWN distinct payload
                // byte[]. Real WAL entries come from independent caller
                // SetAsync invocations - the producer hands a fresh
                // byte[] per call, so no two entries share a reference.
                // If the fixture re-used a single payload byte[] across
                // entries, the Orleans serializer's session-based
                // reference tracking would emit every entry after the
                // first as a small back-reference, making the typed-
                // envelope path look ~10x cheaper than it really is in
                // production. The per-entry seed pattern below ensures
                // each WalRecord.Value is a distinct heap object.
                var records = new WalRecord[entryCount];
                var segments = new ArraySegment<byte>[entryCount];
                for (var i = 0; i < entryCount; i++)
                {
                    var payload = new byte[payloadSize];
                    for (var b = 0; b < payload.Length; b++)
                    {
                        payload[b] = (byte)((b + i) & 0xFF);
                    }

                    hlc = HybridLogicalClock.Tick(hlc);
                    var record = new WalRecord
                    {
                        TreeId = "ship-bench",
                        Op = MutationKind.Set,
                        Key = "k-" + i.ToString("D6", CultureInfo.InvariantCulture),
                        Value = payload,
                        Timestamp = hlc,
                        IsTombstone = false,
                        ExpiresAtTicks = 0L,
                        OriginClusterId = "microbench-source",
                        Mode = LatticeMergeMode.LwwRegister,
                    };
                    records[i] = record;

                    var writer = new System.Buffers.ArrayBufferWriter<byte>(payloadSize + 64);
                    walEncoder.Encode(in record, writer);
                    segments[i] = new ArraySegment<byte>(writer.WrittenSpan.ToArray());
                }

                _shipTypedEnvelopes[pi, ci] = new ReplicationBatchEnvelope
                {
                    WireVersion = ReplicationBatchEnvelope.CurrentVersion,
                    TreeName = "ship-bench",
                    OriginClusterId = "microbench-source",
                    Entries = records,
                };
                _shipFramingSegments[pi, ci] = segments;
            }
        }
    }

    private static int IndexOf(int[] table, int value)
    {
        for (var i = 0; i < table.Length; i++)
        {
            if (table[i] == value) return i;
        }
        throw new ArgumentOutOfRangeException(nameof(value), value, "value not in table");
    }

    /// <summary>
    /// Drives the historical typed-envelope shipper marshaller path:
    /// <c>OrleansBinaryReplicationBatchEncoder.Encode(envelope, writer)</c>
    /// over a <see cref="ReplicationBatchEnvelope"/> carrying
    /// <paramref name="entryCount"/> pre-built <see cref="WalRecord"/>
    /// values whose <c>Value</c> payload is <paramref name="payloadBytes"/>
    /// long. Each invocation Orleans-serialises every entry element-
    /// wise into a fresh <see cref="System.Buffers.ArrayBufferWriter{T}"/>,
    /// matching what <c>ReplicationShipperGrain.PumpOnceAsync</c> pays
    /// paid before R-114 on the typed-envelope path.
    /// </summary>
    [Benchmark(Description = "Ship typed envelope (today)")]
    [ArgumentsSource(nameof(ShipArguments))]
    public int Ship_TypedEnvelope(int entryCount, int payloadBytes)
    {
        var pi = IndexOf(ShipPayloadBytes, payloadBytes);
        var ci = IndexOf(ShipEntryCounts, entryCount);
        var envelope = _shipTypedEnvelopes[pi, ci];
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        // Mirrors OrleansBinaryReplicationBatchEncoder.Encode verbatim:
        // a single Serializer<ReplicationBatchEnvelope>.Serialize call
        // that walks the WalRecord[] element-wise inside the codec.
        _shipEnvelopeSerializer.Serialize(envelope, writer);
        return writer.WrittenCount;
    }

    /// <summary>
    /// Drives the R-114 framing-only shipper marshaller path:
    /// <see cref="IReplicationBatchEncoder.EncodeFraming"/> over a
    /// pre-encoded <see cref="ArraySegment{T}"/> array of length
    /// <paramref name="entryCount"/> at the same payload size. The
    /// fixture's segments were produced by
    /// <c>OrleansBinaryWalRecordEncoder.Encode</c> once per entry at
    /// fixture-build time, mirroring what the shipper receives from
    /// <c>IWalStorageProvider.ReadEncodedAsync</c> (the unconditional
    /// shipper read path after R-114/R-115) - so the per-invocation cost
    /// captured here is purely the framing-header write plus the
    /// length-prefixed segment copies, with no per-entry Orleans
    /// serializer call.
    /// </summary>
    [Benchmark(Description = "Ship framing only (R-114)")]
    [ArgumentsSource(nameof(ShipArguments))]
    public int Ship_FramingOnly(int entryCount, int payloadBytes)
    {
        var pi = IndexOf(ShipPayloadBytes, payloadBytes);
        var ci = IndexOf(ShipEntryCounts, entryCount);
        var segments = _shipFramingSegments[pi, ci];
        var header = new EncodedBatchHeader
        {
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            EntryCount = segments.Length,
            BatchSequence = 1L,
            AtomicBatchSpanCount = 0,
            OriginClusterIdHash = 0u,
        };
        var writer = new System.Buffers.ArrayBufferWriter<byte>();
        _shipFramingEncoder.EncodeFraming(in header, "ship-bench", "microbench-source", segments, writer);
        return writer.WrittenCount;
    }

    /// <summary>
    /// Sweep over (entryCount x payloadBytes) used by both
    /// <see cref="Ship_TypedEnvelope"/> and <see cref="Ship_FramingOnly"/>.
    /// 12 corners covers the small-payload-dominated and
    /// payload-bytes-dominated extremes the roadmap calls for.
    /// </summary>
    public IEnumerable<object[]> ShipArguments()
    {
        foreach (var entryCount in ShipEntryCounts)
        {
            foreach (var payloadBytes in ShipPayloadBytes)
            {
                yield return new object[] { entryCount, payloadBytes };
            }
        }
    }
}

/// <summary>
/// In-bench stub <see cref="IReplicationBatchEncoder"/> used solely so
/// the framing-only benchmark can dispatch through the interface's
/// default <c>EncodeFraming</c> implementation. The Orleans-binary
/// production encoder
/// (<c>Orleans.Lattice.Replication.OrleansBinaryReplicationBatchEncoder</c>)
/// is internal to its package and the bench stays out of the package's
/// <c>InternalsVisibleTo</c> ambit on purpose - the framing methods are
/// pure interface defaults and do not depend on the Orleans codec, so a
/// minimal stub is sufficient to surface the framing-encode allocation
/// profile without piercing the internal boundary.
/// </summary>
internal sealed class BenchFramingEncoder : IReplicationBatchEncoder
{
    public string ContentType => "application/x-bench-framing";

    public int CurrentWireVersion => EncodedBatchHeader.CurrentWireVersion;

    public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        => throw new NotSupportedException("Stub does not implement the typed envelope path; use the default framing methods only.");

    public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
        => throw new NotSupportedException("Stub does not implement the typed envelope path; use the default framing methods only.");
}
