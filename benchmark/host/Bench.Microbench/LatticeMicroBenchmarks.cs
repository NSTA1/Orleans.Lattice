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

    // ===== Per-call retry-envelope cohort instrument (cycle 45+) =====
    // Six dedicated cursors + two dedicated never-seeded keyspaces, added
    // so every public ILattice mutation surface that routes through
    // LatticeGrain.RetryOnStaleRoutingAsync has a steady-state allocation
    // baseline. Each cursor rotates over an existing pre-seeded keyspace
    // (PointWrite-style) so no per-iteration state drift contaminates the
    // measured _alloc_b. PointDelete / DeleteRangeAbsent route against a
    // dedicated keyspace that is never seeded (so every iteration returns
    // false / 0 - the same closure / delegate construction in the retry
    // envelope is exercised on the not-present path). PointApplyCrdtDelta
    // also routes against its own dedicated keyspace so the leaf's
    // PnCounter merge always succeeds (the pre-seeded _keys[] hold random
    // bytes from _value, which fail PnCounter JSON decode).
    private string[] _deleteAbsentKeys = null!;
    private string[] _crdtDeltaKeys = null!;
    private int _deleteCursor;
    private int _getOrSetCursor;
    private int _setIfVersionCursor;
    private int _ttlCursor;
    private int _crdtDeltaCursor;
    // Fixed range outside every seeded keyspace; DeleteRange returns 0 on
    // every iteration. The range bounds are kept identity-stable across
    // iterations so the per-iteration allocation is dominated by the retry
    // envelope (the LatticeGrain.cs side-effect under measurement), not by
    // range-string construction. Lexicographic ordering vs the seeded
    // "k-*" / "bulk-*" / "fbulk-*" / "del-absent-*" keyspaces is preserved
    // by the "zz-empty-*" prefix.
    private const string DeleteRangeAbsentStart = "zz-empty-00000000";
    private const string DeleteRangeAbsentEnd = "zz-empty-99999999";
    // Idempotent constant TTL used by PointSetWithTtl. Large enough that
    // entries never expire mid-iteration even under -Fidelity full so the
    // measured path is always the cold-set / overwrite branch, never the
    // tombstone branch.
    private static readonly TimeSpan SetWithTtlIdempotent = TimeSpan.FromHours(1);
    // Tiny CRDT delta payload (PnCounter mode) reused identity-stable
    // across iterations so the per-iteration allocation is dominated by
    // the retry envelope's closure / delegate cost, not by delta
    // construction. Mode + payload are bit-identical for every call,
    // making the measurement repeatable. ApplyCrdtDeltaAsync rejects
    // LatticeMergeMode.LwwRegister by design (LWW writes go through
    // SetAsync / SetIfVersionAsync); PnCounter is the cheapest CRDT
    // mode whose delta payload survives leaf-side JSON decode.
    private byte[] _crdtDeltaPayload = null!;

    // ===== CRDT primitive merge instrument (MvRegister.MergeFrom) =====
    // Two identity-stable MvRegister states representing concurrent writes
    // from two different replicas, each carrying a populated dot-context
    // (8 shared observed replicas + one own live entry). MvRegister.Merge
    // clones the left side and folds the right in through MergeFrom, which
    // is the per-write CRDT merge path - every MvRegister write folds
    // through MergeDelta -> MergeFrom. Both states are built once and never
    // mutated (Merge does not touch its inputs), so the measured window
    // isolates the merge algorithm's steady-state allocation rather than
    // the grain dispatch / retry envelope the other CRDT workloads pay.
    // Surfaced as microbench_crdt_mvregister_merge_alloc_b.
    private readonly MvRegister _mvRegisterLeft = BuildMvRegister("replica-left", 8);
    private readonly MvRegister _mvRegisterRight = BuildMvRegister("replica-right", 8);

    // ===== CrdtShapeRegistry MvRegister delta-coalescing instrument =====
    // Two identity-stable MvRegisterDeltas with disjoint replica dots, so
    // the pre-ship same-key coalescing merge keeps every entry (neither
    // side's dot context dominates the other). Combining them through the
    // shape's public CombineDeltas entry point exercises
    // CrdtShapeRegistry.CombineMvRegisterDelta, whose post-merge live-
    // entries List<MvRegisterEntry> now backs the combined delta directly
    // rather than being copied out with a defensive .ToArray(). This is the
    // instrument that proves that copy elision as a deterministic allocated-
    // bytes reduction. Both inputs are built once and never mutated.
    // Surfaced as microbench_crdt_mvregister_combine_delta_alloc_b.
    private readonly CrdtShape _mvRegisterShape = CrdtShape.ForMvRegister();
    private readonly MvRegisterDelta _mvDeltaLeft = BuildMvRegisterDelta("dl", 8);
    private readonly MvRegisterDelta _mvDeltaRight = BuildMvRegisterDelta("dr", 8);

    private static MvRegisterDelta BuildMvRegisterDelta(string replicaPrefix, int entries)
    {
        var list = new List<MvRegisterEntry>(entries);
        var context = new Dictionary<string, long>(StringComparer.Ordinal);
        for (var i = 0; i < entries; i++)
        {
            var replicaId = $"{replicaPrefix}-{i:D2}";
            context[replicaId] = 1;
            list.Add(new MvRegisterEntry
            {
                ReplicaId = replicaId,
                Counter = 1,
                Value = new byte[16],
            });
        }
        return new MvRegisterDelta { Entries = list, Context = context };
    }

    // ===== MvRegister.Values() read-path instrument =====
    // Two identity-stable register states for the Values() read path. The
    // single-valued register is the steady state the optimisation targets:
    // Values() must short-circuit to a 1-element array with no sort and no
    // LINQ pipeline. The multi-valued register exercises the Array.Sort
    // comparer path that replaces the former OrderBy.ThenBy.Select.ToArray
    // chain. Both are built once and never mutated, so the measured window
    // isolates the read path's steady-state allocation. Surfaced as
    // microbench_crdt_mvregister_values_alloc_b.
    private readonly MvRegister _mvRegisterSingle = BuildMvRegisterWithValues("replica-a", 1);
    private readonly MvRegister _mvRegisterMulti = BuildMvRegisterWithValues("replica", 8);

    private static MvRegister BuildMvRegisterWithValues(string replicaPrefix, int liveValues)
    {
        var register = new MvRegister();
        for (var i = 0; i < liveValues; i++)
        {
            var replicaId = liveValues == 1 ? replicaPrefix : $"{replicaPrefix}-{i:D2}";
            register.Context[replicaId] = 1;
            register.Entries.Add(new MvRegisterEntry
            {
                ReplicaId = replicaId,
                Counter = 1,
                Value = new byte[16],
            });
        }
        return register;
    }

    private static MvRegister BuildMvRegister(string liveReplica, int observedReplicas)
    {
        var register = new MvRegister();
        // Shared observed dot-context: both sides have already seen these
        // replicas, so the merge takes a pointwise-max over a non-trivial
        // context (the dictionary whose redundant defensive clone this
        // benchmark exists to measure).
        for (var i = 0; i < observedReplicas; i++)
        {
            register.Context[$"obs-{i:D2}"] = 5;
        }
        // The side's own live, dot-tagged value. Neither side has observed
        // the other's dot, so both survive the merge as conflict candidates.
        register.Context[liveReplica] = 1;
        register.Entries.Add(new MvRegisterEntry
        {
            ReplicaId = liveReplica,
            Counter = 1,
            Value = new byte[16],
        });
        return register;
    }

    // ===== CRDT primitive merge instrument (PnCounter.Clone + MergeFrom) =====
    // Two identity-stable PnCounter states representing concurrent activity
    // from two different replicas, each carrying populated Increments /
    // Decrements components (8 shared observed replicas + one own live
    // entry per side). PnCounter.Merge clones the left side and folds the
    // right in through MergeFrom; the clone is the entry-by-entry-vs-
    // copy-constructor path this benchmark exists to measure. Both states are
    // built once and never mutated (Merge does not touch its inputs), so the
    // measured window isolates the merge algorithm's steady-state allocation.
    // Surfaced as microbench_crdt_pncounter_merge_alloc_b.
    private readonly PnCounter _pnCounterLeft = BuildPnCounter("replica-left", 8);
    private readonly PnCounter _pnCounterRight = BuildPnCounter("replica-right", 8);

    private static PnCounter BuildPnCounter(string liveReplica, int observedReplicas)
    {
        var counter = new PnCounter();
        // Shared observed components: both sides have already recorded these
        // replicas on both sides, so the clone presizes over a non-trivial
        // pair of dictionaries (exactly the resize-grow churn the copy
        // constructor eliminates) and the merge takes a real pointwise-max.
        for (var i = 0; i < observedReplicas; i++)
        {
            counter.Increment($"obs-{i:D2}", 5);
            counter.Decrement($"obs-{i:D2}", 2);
        }
        // The side's own live component. Neither side has observed the
        // other's replica, so both survive the merge.
        counter.Increment(liveReplica, 3);
        return counter;
    }

    // ===== CRDT grow-state apply instrument (O(state) re-serialise) =====
    // Per-N pre-seeded OrSet keys whose post-merge state holds ~N
    // dot-tagged elements. The CrdtApplyGrowstate benchmark applies a
    // single fixed OrSet add-delta per iteration; the leaf apply path
    // re-serialises the WHOLE post-merge OrSet state on every call
    // (BPlusLeafGrain.CrdtApply.cs: `shape.SerializeState(typedState)`,
    // O(state)). Allocated-bytes-per-op therefore scales O(N), which is
    // exactly the term the existing PointApplyCrdtDelta benchmark - a
    // constant-size single-replica PnCounter - is structurally blind to.
    // The [Arguments(64/512/4096)] sweep is scoped to this one method so
    // the rest of the suite's benchmark matrix is NOT multiplied (a
    // class-level [Params] would inflate every workload 3x; BDN
    // [Arguments] is per-method).
    private static readonly int[] CrdtGrowStateSizes = [64, 512, 4096];
    private Dictionary<int, string> _crdtGrowStateKeys = null!;
    private byte[] _crdtGrowStateAddDelta = null!;

    // ===== CRDT grow-state apply instrument WITH commit-log writer wired =====
    // Identical to the CrdtApplyGrowstate fixture EXCEPT the leaf backing the
    // measured keys resolves a NON-null ICommitLogWriter (a no-op in-memory
    // double) from its grain context, so the apply takes the durable WRITER
    // path. Pre the "extend lazy CRDT row materialisation to the durable
    // writer path" change, a writer-wired leaf serialises the entire O(state)
    // post-merge OrSet on every apply (eager path: shape.SerializeState), so
    // _alloc_b grows O(N). With the change, the writer-wired leaf defers to
    // the O(delta) streaming fold even with a writer present, so _alloc_b
    // flattens. The existing CrdtApplyGrowstate leaf resolves a NULL writer
    // (NSubstitute auto-substitute IServiceProvider yields null for
    // GetService(ICommitLogWriter)), so it already defers on both main and
    // the branch and is structurally blind to the gate lift - this writer-
    // wired variant is the only lane that can see the term.
    //
    // The writer-wired keys live on a DEDICATED tree (WriterTreeName) so the
    // writer leaf is isolated from the shared single-shard leaf every other
    // workload (point_read / point_write / point_apply_crdt_delta) routes
    // through. The shared leaf keeps resolving a null writer, so the guard
    // cohorts stay on the writerless path and are not perturbed by this
    // fixture. Surfaced as microbench_crdt_apply_growstate_writer_<N>_alloc_b
    // (primary) and microbench_crdt_apply_growstate_writer_<N>_mean_ns.
    private const string WriterTreeName = "microbench-crdt-writer-tree";
    private ILattice _crdtGrowStateWriterLattice = null!;
    private Dictionary<int, string> _crdtGrowStateWriterKeys = null!;
    // Setup-only flag: when true, GetOrCreateLeaf wires the no-op commit-log
    // writer into every leaf it constructs. Set true only while seeding the
    // WriterTreeName fixture (whose leaf must take the writer path) and reset
    // immediately after, so no other tree's leaf inherits a writer. Leaves are
    // cached on first construction, so the benchmark's later calls reuse the
    // already-writer-wired leaf without re-reading the flag.
    private bool _wireWriterForNewLeaves;
    private readonly NoOpCommitLogWriter _noOpCommitLogWriter = new();

    // ===== Receiver-side CRDT batch-apply A/B instrument =====
    // Side-by-side lanes for the receiver's typed-CRDT delta-apply path,
    // applying N OrSet add-deltas to N distinct keys per iteration:
    //   * PerEntry (baseline) faithfully reproduces the historical receiver
    //     path - for each entry GetWithVersionAsync -> deserialise the full
    //     OrSet state -> MergeDelta -> reserialise -> SetIfVersionAsync
    //     (the optimistic read-merge-write the applier ran per entry).
    //   * Batched (candidate) folds all N deltas server-side in a single
    //     IReplicationApplyGrain.ApplyCrdtDeltaManyAsync grain turn.
    // Both lanes run on a DEDICATED single-shard root-is-leaf tree, on
    // DISJOINT key sets, with a FIXED idempotent add-delta so the per-key
    // OrSet state is pinned at one element across all iterations. Surfaced
    // as microbench_crdt_receiver_apply_per_entry_<N>_{mean_ns,alloc_b} and
    // microbench_crdt_receiver_apply_batched_<N>_{mean_ns,alloc_b}.
    private static readonly int[] CrdtReceiverBatchSizes = [16, 64, 256];
    private const string ReceiverBatchTreeName = "microbench-crdt-receiver-batch-tree";
    private ILattice _crdtReceiverBatchLattice = null!;
    private OrSetDelta _crdtReceiverAddDelta;
    private Dictionary<int, string[]> _crdtReceiverPerEntryKeys = null!;
    private Dictionary<int, ApplyCrdtDeltaItem[]> _crdtReceiverBatchItems = null!;

    // ===== Predicate push-down scan instrument (predicate Utf8JsonReader fast-path) =====
    // A dedicated keyspace seeded with small JSON documents. The predicate
    // evaluator parses each candidate value as a JSON document, so the
    // existing random-byte keyspaces would short-circuit at
    // JsonDocument.Parse's JsonException catch and never reach member
    // resolution - they cannot measure the real predicate cost. PredicateKeyScan
    // drains KeysWherePredicateAsync over this window, so the measured
    // allocation isolates the per-row LatticePredicateEvaluator.Matches cost
    // (JsonDocument.Parse + member resolution); KeysWherePredicateAsync ships
    // no values over the wire, so value marshalling does not contaminate it.
    // The "pred-" prefix is lexicographically disjoint from the "k-"/"bulk-"
    // keyspaces so the other single-shard benchmarks are undisturbed.
    private const string PredicateKeyPrefix = "pred-";
    private const int PredicateScanWindowKeys = 1_000;
    private LatticePredicateNode _predicate;
    private string _predScanStart = null!;
    private string _predScanEnd = null!;

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

    // ===== Fixed-shape atomic-write instruments (single-tree vs cross-tree) =====
    // The SetManyAtomic bench above sizes its saga batch from
    // BENCH_MICROBENCH_ATOMIC_BATCH (default 16), which is the right knob for
    // the sustained-throughput headline. The published single-silo perf doc,
    // however, wants two FIXED batch shapes (2 keys and 64 keys) so an operator
    // can compare single-tree against cross-tree (multi-tree) atomic-write
    // throughput at MATCHED batch sizes. These rows are therefore pinned to
    // their shapes and do not move with the env knob.
    //
    // Single-tree fixed batches run against the same single-shard atomic tree
    // (_atomicLattice) the env-sized SetManyAtomic bench uses; the keys are
    // drawn from disjoint "atomic2-"/"atomic64-" prefixes so they never collide
    // with the env-sized "atomic-" keyspace.
    private const int FixedAtomicBatch2 = 2;
    private const int FixedAtomicBatch64 = 64;
    private List<KeyValuePair<string, byte[]>> _atomicBatch2 = null!;
    private List<KeyValuePair<string, byte[]>> _atomicBatch64 = null!;

    // Cross-tree atomic write: two distinct single-shard trees commit
    // all-or-nothing through the LatticeCrossTreeTxGrain coordinator
    // (grainFactory.BeginAtomicWrite(op).ForTree(a)...ForTree(b)...CommitAsync()).
    // Each iteration mints a fresh operationId so a brand-new coordinator and a
    // pair of per-tree sub-sagas are driven end-to-end. The 2-key shape places
    // one key on each tree; the 64-key shape splits 32 keys per tree across the
    // two trees. Both trees are pre-seeded so the saga overwrites existing leaf
    // entries (LWW) rather than triggering a split mid-iteration.
    private const string CrossTreeNameA = "microbench-xtree-a";
    private const string CrossTreeNameB = "microbench-xtree-b";
    private const int CrossTreeKeysPerTree64 = 32;
    private ILattice _crossTreeLatticeA = null!;
    private ILattice _crossTreeLatticeB = null!;
    private IReminderRegistry _crossTreeReminderRegistry = null!;
    private readonly Dictionary<string, ILatticeCrossTreeTxGrain> _crossTreeCoordinators = [];
    private string[] _crossTreeKeysA2 = null!;
    private string[] _crossTreeKeysB2 = null!;
    private string[] _crossTreeKeysA64 = null!;
    private string[] _crossTreeKeysB64 = null!;

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
        // LatticeCrossTreeTxGrain.ResolveTtl() reads optionsMonitor.CurrentValue
        // directly (not via .Get(name)); without this stub it returns null and
        // the cross-tree finalize TTL slide NREs, so the cross-tree benches never
        // produced numbers.
        _optionsMonitor.CurrentValue.Returns(new LatticeOptions());

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

        // Cross-tree atomic-write coordinator route: a real
        // LatticeCrossTreeTxGrain per operationId. Shares the same mocked
        // IReminderRegistry as the per-tree sub-sagas (keepalive +
        // retention reminders are no-ops under the synchronous bench
        // harness). The coordinator fans prepare/finalize out to the
        // per-tree IAtomicWriteGrain sub-sagas (keyed {treeId}/{operationId})
        // which are already served by the GetOrCreateAtomicSaga route above.
        _crossTreeReminderRegistry = _atomicReminderRegistry;
        _grainFactory.GetGrain<ILatticeCrossTreeTxGrain>(Arg.Any<string>())
            .Returns(c => GetOrCreateCrossTreeTx(c.ArgAt<string>(0)));

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
        var serviceProvider = AuthBench.CreateServiceProvider();
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

        // Per-call retry-envelope cohort instrument: dedicated
        // never-seeded keyspace for PointDelete. Each iteration deletes a
        // key that was never set, hitting the "absent" return-false branch.
        // The closure / delegate construction inside
        // LatticeGrain.DeleteAsyncCore -> RetryOnStaleRoutingAsync happens
        // before the underlying shard probes the leaf, so the per-call
        // allocation we are measuring is identical on the present vs
        // absent paths - the absent path just lets us run for unbounded
        // iterations without consuming the seeded keyspace.
        _deleteAbsentKeys = new string[keyCount];
        for (var i = 0; i < keyCount; i++)
        {
            _deleteAbsentKeys[i] = "del-absent-" + i.ToString("D8", CultureInfo.InvariantCulture);
        }

        // Per-call retry-envelope cohort instrument: dedicated keyspace
        // for PointApplyCrdtDelta. The pre-seeded _keys[] hold random
        // bytes from _value (an LWW byte payload) which fail the leaf's
        // PnCounter JSON decode; routing the CRDT delta against a fresh
        // key range means the first merge per key creates a valid
        // PnCounter state and every subsequent merge (when the cursor
        // wraps) folds an additional increment into a well-formed
        // PnCounter. Either path exercises the
        // ApplyCrdtDeltaAsyncCore -> RetryOnStaleRoutingAsync envelope
        // identically.
        _crdtDeltaKeys = new string[keyCount];
        for (var i = 0; i < keyCount; i++)
        {
            _crdtDeltaKeys[i] = "crdt-" + i.ToString("D8", CultureInfo.InvariantCulture);
        }

        // Tiny CRDT delta payload for the PointApplyCrdtDelta benchmark.
        // A single-replica single-increment PnCounter delta is the
        // smallest payload that survives the leaf-side JSON decode
        // (~40 bytes of UTF-8) while still exercising the
        // ApplyCrdtDeltaAsyncCore -> RetryOnStaleRoutingAsync envelope.
        var seedDelta = new PnCounterDelta
        {
            Increments = new Dictionary<string, long>(StringComparer.Ordinal) { ["bench"] = 1 },
            Decrements = new Dictionary<string, long>(0, StringComparer.Ordinal),
        };
        _crdtDeltaPayload = JsonLatticeSerializer<PnCounterDelta>.Default.Serialize(seedDelta);

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
        BuildFixedAtomicBatches();
        BuildCrossTreeTrees();
        BuildWalEncodeFixture();
        BuildShipFramingFixture();
        BuildVersionVectorFixture();
        BuildOrMapFixture();
        BuildOrSetFixture();
        BuildOrSetOpsFixture();
        BuildOrMapBulkSetFixture();
        BuildLeafQueueFixture();
        BuildCrdtGrowStateFixture();
        BuildCrdtGrowStateWriterFixture();
        BuildCrdtReceiverBatchFixture();

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
        // Setup-only: the CRDT writer-path fixture flips this flag while
        // seeding so its leaf resolves a non-null ICommitLogWriter and takes
        // the durable writer path. Every other leaf (and every benchmark not
        // under that fixture) leaves it false, so ResolveCommitLogWriter()
        // yields null exactly as before - the auto-substitute IServiceProvider
        // returns null for an unstubbed GetService(ICommitLogWriter).
        if (_wireWriterForNewLeaves)
        {
            ctx.ActivationServices.GetService(typeof(ICommitLogWriter)).Returns(_noOpCommitLogWriter);
        }
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
        var registry = new TxRegistryGrain(ctx, _grainFactory, _optionsMonitor, registryState);
        _txRegistries[treeId] = registry;
        return registry;
    }

    /// <summary>
    /// Lazily constructs and caches a real <see cref="LatticeCrossTreeTxGrain"/>
    /// for the given cross-tree operationId. Each
    /// <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite(IGrainFactory, string)"/>
    /// commit mints a fresh operationId, so a new coordinator grain is
    /// constructed per benchmark iteration. The coordinator drives the
    /// per-tree <see cref="AtomicWriteGrain"/> sub-sagas (keyed
    /// <c>{treeId}/{operationId}</c>) through prepare-and-pause then a single
    /// global commit decision. Mirrors <see cref="GetOrCreateAtomicSaga"/>.
    /// </summary>
    private ILatticeCrossTreeTxGrain GetOrCreateCrossTreeTx(string operationId)
    {
        if (_crossTreeCoordinators.TryGetValue(operationId, out var existing)) return existing;
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("cross-tree-tx", operationId));
        var coordinatorState = new FakePersistentState<CrossTreeTxState>();
        var coordinator = new LatticeCrossTreeTxGrain(
            ctx,
            _grainFactory,
            _crossTreeReminderRegistry,
            _optionsMonitor,
            NullLogger<LatticeCrossTreeTxGrain>.Instance,
            coordinatorState);
        _crossTreeCoordinators[operationId] = coordinator;
        return coordinator;
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
    /// Single <see cref="ILattice.DeleteAsync(string, CancellationToken)"/>
    /// against a rotating key drawn from a dedicated never-seeded
    /// keyspace, hitting the "key absent" return-false branch on every
    /// iteration. The closure / delegate construction inside
    /// <c>LatticeGrain.DeleteAsyncCore</c> -&gt;
    /// <c>RetryOnStaleRoutingAsync</c> happens BEFORE the underlying
    /// shard probes the leaf, so the per-call allocation we are measuring
    /// is identical on the present and absent paths. The absent path is
    /// chosen because it lets the bench run for unbounded iterations
    /// without consuming the pre-seeded keyspace that PointRead /
    /// PointExists / GetMany rely on. Surfaced as
    /// <c>microbench_point_delete_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Point delete")]
    public Task<bool> PointDelete()
    {
        var i = unchecked(_deleteCursor++) & int.MaxValue;
        var key = _deleteAbsentKeys[i % _deleteAbsentKeys.Length];
        return _lattice.DeleteAsync(key);
    }

    /// <summary>
    /// Single <see cref="ILattice.DeleteRangeAsync(string, string, CancellationToken)"/>
    /// against a fixed range outside every seeded keyspace, hitting the
    /// "no matches" return-0 branch on every iteration. Same rationale
    /// as <see cref="PointDelete"/>: the closure / delegate construction
    /// inside <c>DeleteRangeAsyncOuter</c> -&gt;
    /// <c>RetryOnStaleRoutingAsync</c> happens before the underlying
    /// range-scan probes leaves, so the per-call allocation we measure is
    /// identical regardless of how many tombstones the scan would
    /// produce; the absent range lets the bench run for unbounded
    /// iterations without state drift. Surfaced as
    /// <c>microbench_delete_range_absent_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Delete range absent")]
    public Task<int> DeleteRangeAbsent()
    {
        return _lattice.DeleteRangeAsync(DeleteRangeAbsentStart, DeleteRangeAbsentEnd);
    }

    /// <summary>
    /// Single <see cref="ILattice.GetOrSetAsync(string, byte[], CancellationToken)"/>
    /// against a rotating key drawn from the pre-seeded keyspace. Every
    /// iteration hits the "key already present" return-existing branch -
    /// the steady-state cost a caller pays when an entry already exists.
    /// Drives the <c>GetOrSetAsyncCore</c> -&gt;
    /// <c>RetryOnStaleRoutingAsync</c> closure / delegate envelope.
    /// Surfaced as <c>microbench_point_get_or_set_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Point get or set")]
    public Task<byte[]?> PointGetOrSet()
    {
        var i = unchecked(_getOrSetCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.GetOrSetAsync(key, _value);
    }

    /// <summary>
    /// Single <see cref="ILattice.SetIfVersionAsync(string, byte[], HybridLogicalClock, CancellationToken)"/>
    /// against a rotating key drawn from the pre-seeded keyspace, always
    /// passing <see cref="HybridLogicalClock.Zero"/> as the expected
    /// version. The pre-seeded keys all carry a non-zero stamped HLC so
    /// every iteration returns <c>false</c> (CAS rejected). Same
    /// allocation rationale as <see cref="PointDelete"/>: the closure /
    /// delegate construction inside <c>SetIfVersionAsyncCore</c> -&gt;
    /// <c>RetryOnStaleRoutingAsync</c> is identical whether the CAS
    /// succeeds or fails - the failing path is chosen so per-iteration
    /// state does not drift. Surfaced as
    /// <c>microbench_point_set_if_version_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Point set if version")]
    public Task<bool> PointSetIfVersion()
    {
        var i = unchecked(_setIfVersionCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.SetIfVersionAsync(key, _value, HybridLogicalClock.Zero);
    }

    /// <summary>
    /// Single <see cref="ILattice.SetAsync(string, byte[], TimeSpan, CancellationToken)"/>
    /// against a rotating key drawn from the pre-seeded keyspace with a
    /// fixed 1-hour TTL. Drives the <c>SetAsyncTtlCore</c> -&gt;
    /// <c>RetryOnStaleRoutingAsync</c> envelope - distinct from the
    /// TTL-less <see cref="PointWrite"/> path, which uses a different
    /// inline retry shape. The 1-hour TTL is large enough that no entry
    /// expires during any fidelity's measurement window so the path is
    /// always cold-set / overwrite, never tombstone. Surfaced as
    /// <c>microbench_point_set_with_ttl_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Point set with ttl")]
    public Task PointSetWithTtl()
    {
        var i = unchecked(_ttlCursor++) & int.MaxValue;
        var key = _keys[i % _keys.Length];
        return _lattice.SetAsync(key, _value, SetWithTtlIdempotent);
    }

    /// <summary>
    /// Single <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], CancellationToken)"/>
    /// against a rotating key drawn from the pre-seeded keyspace, with
    /// mode <see cref="LatticeMergeMode.PnCounter"/> and a fixed
    /// single-replica single-increment <see cref="PnCounterDelta"/>
    /// payload (~40 UTF-8 bytes). Drives the
    /// <c>ApplyCrdtDeltaAsyncCore</c> -&gt;
    /// <c>RetryOnStaleRoutingAsync</c> envelope.
    /// <see cref="LatticeMergeMode.LwwRegister"/> is rejected by
    /// <c>ApplyCrdtDeltaAsync</c> by design (LWW writes use
    /// <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>);
    /// PnCounter is the cheapest CRDT mode whose delta payload
    /// survives leaf-side JSON decode. Identity-stable payload keeps
    /// the merge resolution deterministic across iterations so
    /// per-iteration allocation is dominated by the retry envelope,
    /// not by the merge algorithm. Surfaced as
    /// <c>microbench_point_apply_crdt_delta_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Point apply crdt delta")]
    public Task<HybridLogicalClock> PointApplyCrdtDelta()
    {
        var i = unchecked(_crdtDeltaCursor++) & int.MaxValue;
        var key = _crdtDeltaKeys[i % _crdtDeltaKeys.Length];
        return _lattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.PnCounter, _crdtDeltaPayload);
    }

    /// <summary>
    /// Single <see cref="MvRegister.Merge(MvRegister, MvRegister)"/> of two
    /// identity-stable concurrent-replica states pre-built in the field
    /// initialisers. Isolates the <see cref="MvRegister"/> CRDT merge
    /// algorithm - the per-write fold path (<c>MergeDelta</c> -&gt;
    /// <c>MergeFrom</c>) - from the grain dispatch and retry envelope the
    /// other CRDT workloads measure. The inputs are never mutated, so the
    /// measured per-iteration allocation is the steady-state cost of one
    /// register merge. Surfaced as
    /// <c>microbench_crdt_mvregister_merge_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Crdt mvregister merge")]
    public MvRegister CrdtMvRegisterMerge() =>
        MvRegister.Merge(_mvRegisterLeft, _mvRegisterRight);

    /// <summary>
    /// Single <see cref="CrdtShapeRegistry"/> MvRegister delta-coalescing
    /// combine of two identity-stable, disjoint-replica deltas through the
    /// shape's public <c>CombineDeltas</c> entry point. Drives
    /// <c>CrdtShapeRegistry.CombineMvRegisterDelta</c> - the pre-ship
    /// same-key delta-run coalescing path a sender uses to fold an arbitrary
    /// run of same-key deltas into a single shipped delta. Both inputs carry
    /// disjoint dots, so the post-merge register keeps every entry and the
    /// combined delta's live-entries list is at its widest: the case where
    /// the eliminated defensive <c>List&lt;MvRegisterEntry&gt;.ToArray()</c>
    /// copy allocated the most. The two deltas are boxed once into the
    /// <c>Func&lt;object, object, object&gt;</c> signature per iteration on
    /// both baseline and candidate, so that constant boxing cost cancels in
    /// the before/after delta and the measured difference isolates the copy
    /// elision. Inputs are never mutated, so the measured per-iteration
    /// allocation is the steady-state coalescing cost. Surfaced as
    /// <c>microbench_crdt_mvregister_combine_delta_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Crdt mvregister combine delta")]
    public object CrdtMvRegisterCombineDelta() =>
        _mvRegisterShape.CombineDeltas!(_mvDeltaLeft, _mvDeltaRight);

    /// <summary>
    /// Single <see cref="MvRegister.Values"/> read against a single-valued
    /// register - the steady state the read-path optimisation targets. The
    /// register is never mutated, so the measured per-iteration allocation
    /// is the steady-state cost of one Values() read: the optimisation
    /// short-circuits to a 1-element array, eliminating the former
    /// OrderBy.ThenBy.Select.ToArray LINQ pipeline (several iterator and
    /// comparer allocations plus the array). Surfaced as
    /// <c>microbench_crdt_mvregister_values_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Crdt mvregister values")]
    public IReadOnlyList<byte[]> CrdtMvRegisterValues() =>
        _mvRegisterSingle.Values();

    /// <summary>
    /// Single <see cref="MvRegister.Values"/> read against an 8-valued
    /// register - the transient concurrent-write state. Exercises the
    /// <c>Array.Sort</c> comparer path that replaces the former LINQ chain;
    /// the register is never mutated, so the measured per-iteration
    /// allocation is the steady-state cost of one multi-value Values() read.
    /// Surfaced as <c>microbench_crdt_mvregister_values_multi_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Crdt mvregister values multi")]
    public IReadOnlyList<byte[]> CrdtMvRegisterValuesMulti() =>
        _mvRegisterMulti.Values();

    /// <summary>
    /// Single <see cref="PnCounter.Merge(PnCounter, PnCounter)"/> of two
    /// identity-stable concurrent-replica states pre-built in the field
    /// initialisers. <see cref="PnCounter.Merge"/> clones the left side and
    /// folds the right in through <c>MergeFrom</c>; the clone uses the
    /// dictionary copy constructor, presizing each backing store to the
    /// source count exactly and bulk-copying rather than paying the
    /// incremental <c>Resize()</c> grows the previous entry-by-entry fill
    /// incurred. The inputs are never mutated, so the measured per-iteration
    /// allocation is the steady-state cost of one counter merge. Surfaced as
    /// <c>microbench_crdt_pncounter_merge_alloc_b</c>.
    /// </summary>
    [Benchmark(Description = "Crdt pncounter merge")]
    public PnCounter CrdtPnCounterMerge() =>
        PnCounter.Merge(_pnCounterLeft, _pnCounterRight);

    /// <summary>
    /// Single <see cref="ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], CancellationToken)"/>
    /// in <see cref="LatticeMergeMode.OrSet"/> against a key whose state was
    /// pre-seeded during <see cref="GlobalSetup"/> with <paramref name="n"/>
    /// distinct dot-tagged elements. Each iteration applies one fixed
    /// single-element add-delta; the leaf apply path re-serialises the entire
    /// post-merge OrSet state on every call
    /// (<c>BPlusLeafGrain.CrdtApply.cs</c>: <c>shape.SerializeState(typedState)</c>),
    /// so allocated-bytes-per-op grows O(N) with the seeded element count.
    /// <para>
    /// This is the empirical lane for the "defer per-apply full CRDT-state
    /// re-serialisation" hypothesis: unlike <see cref="PointApplyCrdtDelta"/>
    /// (a constant-size single-replica PnCounter whose re-serialise cost is
    /// O(1) and therefore cannot see the term), the growing OrSet makes the
    /// O(state) re-serialise dominate, so a candidate that elides or defers
    /// it has a metric that can move. The probe element's dot is
    /// identity-stable, so re-applying it is idempotent and the seeded state
    /// stays at ~N elements across every measurement iteration regardless of
    /// iteration count - keeping <c>_alloc_b</c> a clean function of N rather
    /// than of cursor drift. Surfaced as
    /// <c>microbench_crdt_apply_growstate_&lt;N&gt;_alloc_b</c> (primary) and
    /// <c>microbench_crdt_apply_growstate_&lt;N&gt;_mean_ns</c> (secondary).
    /// </para>
    /// </summary>
    [Benchmark(Description = "Crdt apply growstate")]
    [Arguments(64)]
    [Arguments(512)]
    [Arguments(4096)]
    public Task<HybridLogicalClock> CrdtApplyGrowstate(int n)
    {
        var key = _crdtGrowStateKeys[n];
        return _lattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.OrSet, _crdtGrowStateAddDelta);
    }

    /// <summary>
    /// Writer-path counterpart to <see cref="CrdtApplyGrowstate"/>. Identical
    /// per-iteration apply (one fixed OrSet add-delta against a key pre-seeded
    /// with <paramref name="n"/> distinct dot-tagged elements) EXCEPT the leaf
    /// backing the key resolves a non-null <see cref="ICommitLogWriter"/> from
    /// its grain context, so the apply takes the durable WRITER path.
    /// <para>
    /// This is the empirical lane for the "extend lazy CRDT post-merge row
    /// materialisation to the durable writer path" change. On the pre-change
    /// production code a writer-wired leaf serialises the whole O(state)
    /// post-merge OrSet on every apply (<c>shape.SerializeState</c>), so
    /// <c>_alloc_b</c> grows O(N); on the changed code the writer-wired leaf
    /// defers to the O(delta) streaming fold, so <c>_alloc_b</c> flattens.
    /// The writerless <see cref="CrdtApplyGrowstate"/> already deferred on
    /// both code versions and so cannot see the term. Surfaced as
    /// <c>microbench_crdt_apply_growstate_writer_&lt;N&gt;_alloc_b</c> (primary)
    /// and <c>microbench_crdt_apply_growstate_writer_&lt;N&gt;_mean_ns</c>
    /// (secondary).
    /// </para>
    /// </summary>
    [Benchmark(Description = "Crdt apply growstate writer")]
    [Arguments(64)]
    [Arguments(512)]
    [Arguments(4096)]
    public Task CrdtApplyGrowstateWriter(int n)
    {
        var key = _crdtGrowStateWriterKeys[n];
        return _crdtGrowStateWriterLattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.OrSet, _crdtGrowStateAddDelta);
    }

    /// <summary>
    /// Builds the <see cref="CrdtApplyGrowstate"/> fixture: for each N in
    /// <see cref="CrdtGrowStateSizes"/>, seeds a dedicated key's OrSet state
    /// with N distinct dot-tagged elements via the public apply surface (the
    /// seeding re-serialise cost is incurred here, inside
    /// <see cref="GlobalSetup"/>, and is excluded from the measured window).
    /// Also pre-builds the single fixed add-delta the benchmark applies each
    /// iteration. The add-delta's element and dot are identity-stable so the
    /// per-iteration merge is idempotent after the first apply, pinning the
    /// measured state size at ~N.
    /// </summary>
    private void BuildCrdtGrowStateFixture()
    {
        _crdtGrowStateKeys = new Dictionary<int, string>(CrdtGrowStateSizes.Length);

        // The per-iteration delta: a single OrSet add of a FIXED element with
        // a FIXED dot. Idempotent re-application keeps the seeded state stable
        // at ~N across all measurement iterations.
        var probeDelta = new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot
                {
                    Element = System.Text.Encoding.UTF8.GetBytes("growstate-probe-element"),
                    ReplicaId = "bench-probe",
                    Counter = 1,
                },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        _crdtGrowStateAddDelta = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(probeDelta);

        foreach (var n in CrdtGrowStateSizes)
        {
            var key = "crdt-grow-" + n.ToString("D8", CultureInfo.InvariantCulture);
            _crdtGrowStateKeys[n] = key;

            // Seed N distinct dot-tagged elements so the key's post-merge OrSet
            // state holds N live elements. Each add is its own delta so the
            // leaf folds them one at a time, mirroring how the state would have
            // grown under real active-active OrSet traffic.
            for (var i = 0; i < n; i++)
            {
                var seedDelta = new OrSetDelta
                {
                    Adds = new[]
                    {
                        new OrSetDeltaDot
                        {
                            Element = System.Text.Encoding.UTF8.GetBytes(
                                "seed-" + i.ToString("D8", CultureInfo.InvariantCulture)),
                            ReplicaId = "bench-seed",
                            Counter = i + 1,
                        },
                    },
                    Removes = Array.Empty<OrSetDeltaDot>(),
                };
                var seedBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(seedDelta);
                _lattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.OrSet, seedBytes)
                    .GetAwaiter().GetResult();
            }
        }
    }

    /// <summary>
    /// Builds the <see cref="CrdtApplyGrowstateWriter"/> fixture: a dedicated
    /// single-shard, root-is-leaf <see cref="ILattice"/> rooted at
    /// <see cref="WriterTreeName"/> whose leaf resolves a non-null no-op
    /// <see cref="ICommitLogWriter"/> (so the CRDT apply takes the durable
    /// writer path), and one pre-seeded key per N in
    /// <see cref="CrdtGrowStateSizes"/> whose OrSet state holds N distinct
    /// dot-tagged elements. Mirrors <see cref="BuildCrdtGrowStateFixture"/>
    /// element-for-element (same probe delta reused via
    /// <see cref="_crdtGrowStateAddDelta"/>, same per-element seed shape) so
    /// the only difference between the writerless and writer-wired lanes is
    /// the resolved commit-log writer. The writer is wired by flipping
    /// <see cref="_wireWriterForNewLeaves"/> only while this fixture's leaf is
    /// constructed, so no other tree's leaf inherits it.
    /// </summary>
    private void BuildCrdtGrowStateWriterFixture()
    {
        _crdtGrowStateWriterKeys = new Dictionary<int, string>(CrdtGrowStateSizes.Length);

        // Register the dedicated tree's structural pin (single shard,
        // root-is-leaf, MaxLeafKeys high enough that the 3 seeded keys never
        // trigger a split mid-seed), mirroring BuildCrossTreeParticipant.
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(WriterTreeName);
        registry.GetEntryAsync(WriterTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = 1,
            }));

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", WriterTreeName));
        var serviceProvider = AuthBench.CreateServiceProvider();
        var lattice = new LatticeGrain(
            context,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            serviceProvider,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(WriterTreeName).Returns(lattice);
        _crdtGrowStateWriterLattice = lattice;

        // Flip the writer-wiring flag for the duration of the seed phase only,
        // so the leaf this tree lazily creates resolves the no-op writer and
        // every apply (seed + measured) takes the durable writer path. The
        // leaf is cached on first construction, so resetting the flag after
        // seeding does not affect the benchmark's later calls.
        _wireWriterForNewLeaves = true;
        try
        {
            foreach (var n in CrdtGrowStateSizes)
            {
                var key = "crdt-grow-writer-" + n.ToString("D8", CultureInfo.InvariantCulture);
                _crdtGrowStateWriterKeys[n] = key;

                for (var i = 0; i < n; i++)
                {
                    var seedDelta = new OrSetDelta
                    {
                        Adds = new[]
                        {
                            new OrSetDeltaDot
                            {
                                Element = System.Text.Encoding.UTF8.GetBytes(
                                    "seed-" + i.ToString("D8", CultureInfo.InvariantCulture)),
                                ReplicaId = "bench-seed",
                                Counter = i + 1,
                            },
                        },
                        Removes = Array.Empty<OrSetDeltaDot>(),
                    };
                    var seedBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(seedDelta);
                    _crdtGrowStateWriterLattice.ApplyCrdtDeltaAsync(key, LatticeMergeMode.OrSet, seedBytes)
                        .GetAwaiter().GetResult();
                }
            }
        }
        finally
        {
            _wireWriterForNewLeaves = false;
        }
    }

    /// <summary>
    /// Builds the receiver CRDT batch-apply A/B fixture: a dedicated
    /// single-shard, root-is-leaf <see cref="ILattice"/> rooted at
    /// <see cref="ReceiverBatchTreeName"/>, plus, for each N in
    /// <see cref="CrdtReceiverBatchSizes"/>, two DISJOINT N-key sets (one per
    /// lane) and the pre-built batched-apply item list. A single fixed
    /// idempotent OrSet add-delta is reused for every key so re-application
    /// leaves the per-key state pinned at one element, keeping both lanes
    /// allocation-stable across measurement iterations. Both lanes' keys are
    /// seeded once here (outside the measured window) so the first measured
    /// iteration matches steady state.
    /// </summary>
    private void BuildCrdtReceiverBatchFixture()
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(ReceiverBatchTreeName);
        registry.GetEntryAsync(ReceiverBatchTreeName).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = 1,
            }));

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", ReceiverBatchTreeName));
        var serviceProvider = AuthBench.CreateServiceProvider();
        var lattice = new LatticeGrain(
            context,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            serviceProvider,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(ReceiverBatchTreeName).Returns(lattice);
        _crdtReceiverBatchLattice = lattice;

        // The single fixed, idempotent add-delta both lanes apply per key.
        _crdtReceiverAddDelta = new OrSetDelta
        {
            Adds = new[]
            {
                new OrSetDeltaDot
                {
                    Element = System.Text.Encoding.UTF8.GetBytes("receiver-batch-probe-element"),
                    ReplicaId = "bench-receiver",
                    Counter = 1,
                },
            },
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var deltaBytes = JsonLatticeSerializer<OrSetDelta>.Default.Serialize(_crdtReceiverAddDelta);
        var sourceHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };

        _crdtReceiverPerEntryKeys = new Dictionary<int, string[]>(CrdtReceiverBatchSizes.Length);
        _crdtReceiverBatchItems = new Dictionary<int, ApplyCrdtDeltaItem[]>(CrdtReceiverBatchSizes.Length);
        var applyGrain = (IReplicationApplyGrain)lattice;

        foreach (var n in CrdtReceiverBatchSizes)
        {
            var perEntryKeys = new string[n];
            var batchItems = new ApplyCrdtDeltaItem[n];
            for (var i = 0; i < n; i++)
            {
                var slot = i.ToString("D8", CultureInfo.InvariantCulture);
                perEntryKeys[i] = "rcv-pe-" + n.ToString("D8", CultureInfo.InvariantCulture) + "-" + slot;
                batchItems[i] = new ApplyCrdtDeltaItem
                {
                    Key = "rcv-ba-" + n.ToString("D8", CultureInfo.InvariantCulture) + "-" + slot,
                    Mode = LatticeMergeMode.OrSet,
                    Delta = deltaBytes,
                    SourceHlc = sourceHlc,
                    OriginClusterId = "bench-origin",
                    SourceVectorClock = null,
                };
            }

            _crdtReceiverPerEntryKeys[n] = perEntryKeys;
            _crdtReceiverBatchItems[n] = batchItems;

            // Seed both lanes once so steady state is established before the
            // first measured iteration.
            ApplyCrdtReceiverPerEntry(n).GetAwaiter().GetResult();
            applyGrain.ApplyCrdtDeltaManyAsync(batchItems).GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Baseline lane: applies N OrSet add-deltas to N distinct keys via the
    /// historical per-entry receiver path - one
    /// <see cref="ILattice.GetWithVersionAsync(string, CancellationToken)"/> +
    /// client-side OrSet deserialise/<see cref="OrSet.MergeDelta"/>/reserialise
    /// + <see cref="ILattice.SetIfVersionAsync(string, byte[], HybridLogicalClock, CancellationToken)"/>
    /// round trip each. Surfaced as
    /// <c>microbench_crdt_receiver_apply_per_entry_&lt;N&gt;_{mean_ns,alloc_b}</c>.
    /// </summary>
    [Benchmark(Description = "Crdt receiver apply per entry")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public Task CrdtReceiverApplyPerEntry(int n) => ApplyCrdtReceiverPerEntry(n);

    private async Task ApplyCrdtReceiverPerEntry(int n)
    {
        var keys = _crdtReceiverPerEntryKeys[n];
        var stateSerializer = JsonLatticeSerializer<OrSet>.Default;
        for (var i = 0; i < keys.Length; i++)
        {
            var key = keys[i];
            var versioned = await _crdtReceiverBatchLattice.GetWithVersionAsync(key);
            var existing = versioned.Value is null
                ? new OrSet()
                : stateSerializer.Deserialize(versioned.Value);
            existing.MergeDelta(_crdtReceiverAddDelta);
            var bytes = stateSerializer.Serialize(existing);
            await _crdtReceiverBatchLattice.SetIfVersionAsync(key, bytes, versioned.Version);
        }
    }

    /// <summary>
    /// Candidate lane: folds the same N OrSet add-deltas into N distinct keys
    /// in a single
    /// <see cref="IReplicationApplyGrain.ApplyCrdtDeltaManyAsync"/> grain turn,
    /// eliminating the per-entry optimistic read-merge-write round trips.
    /// Surfaced as
    /// <c>microbench_crdt_receiver_apply_batched_&lt;N&gt;_{mean_ns,alloc_b}</c>.
    /// </summary>
    [Benchmark(Description = "Crdt receiver apply batched")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public Task CrdtReceiverApplyBatched(int n) =>
        ((IReplicationApplyGrain)_crdtReceiverBatchLattice).ApplyCrdtDeltaManyAsync(_crdtReceiverBatchItems[n]);

    /// <summary>
    /// No-op in-memory <see cref="ICommitLogWriter"/> double used by the
    /// <see cref="CrdtApplyGrowstateWriter"/> fixture so the leaf resolves a
    /// non-null writer and takes the durable writer path WITHOUT performing
    /// any real WAL I/O. <see cref="AppendAsync"/> returns a cached completed
    /// task (the apply path discards the offset), so the writer contributes
    /// no per-apply allocation that could mask the O(state)-vs-O(delta)
    /// difference under measurement.
    /// </summary>
    private sealed class NoOpCommitLogWriter : ICommitLogWriter
    {
        private static readonly Task<long> CompletedZero = Task.FromResult(0L);

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default)
            => CompletedZero;

        public Task<IReadOnlyList<long>> AppendManyAsync(
            IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<long>>(new long[entries.Count]);
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
        var fanoutSp = AuthBench.CreateServiceProvider();
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

        // Predicate-scan fixture: seed a dedicated single-shard keyspace with
        // small JSON documents so the predicate evaluator's JsonDocument.Parse
        // + member-resolution path actually runs (random-byte values would
        // short-circuit on parse failure). The predicate `Age >= 50` matches
        // ~half the rows (Age = i % 100), exercising both the match and
        // non-match branches plus the integer member-resolution path. Seeded on
        // the single-shard _lattice under the disjoint "pred-" prefix.
        var predCount = Math.Min(PredicateScanWindowKeys, Math.Max(1, keyCount));
        var predKeys = new string[predCount];
        for (var i = 0; i < predCount; i++)
        {
            predKeys[i] = PredicateKeyPrefix + i.ToString("D8", CultureInfo.InvariantCulture);
            var age = i % 100;
            var json = "{\"Age\":" + age.ToString(CultureInfo.InvariantCulture)
                + ",\"Name\":\"user-" + i.ToString("D8", CultureInfo.InvariantCulture) + "\"}";
            var jsonBytes = System.Text.Encoding.UTF8.GetBytes(json);
            _lattice.SetAsync(predKeys[i], jsonBytes).GetAwaiter().GetResult();
        }

        _predicate = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(50)));
        _predScanStart = predKeys[0];
        _predScanEnd = predKeys[predCount - 1] + "\u0000";
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
    /// Paginated entry scan over the 4-shard fanout tree. Mirror of
    /// <see cref="KeyScan_PageOver4Shards"/> but exercises
    /// <see cref="ILattice.EntriesAsync(string, string, bool, bool?, CancellationToken)"/>,
    /// which uses a parallel per-shard cursor structure in
    /// LatticeGrain.Entries.cs. The symmetric attribution lane lets
    /// presize / structural optimisations targeting Entries.cs surface
    /// against the same scan-window shape as Keys.cs - the only
    /// difference between the two scan paths is the page payload shape
    /// (key strings vs key+value pairs) and the per-call dedup HashSet
    /// presize (which PR #209 added on Keys.cs but missed on Entries.cs).
    /// </summary>
    [Benchmark(Description = "Entry scan 4 shards")]
    public async Task EntryScan_PageOver4Shards()
    {
        await foreach (var _ in _fanoutLattice.EntriesAsync(_fanoutScanStart, _fanoutScanEnd))
        {
        }
    }

    /// <summary>
    /// Drains <see cref="ILattice.KeysWherePredicateAsync"/> over the
    /// JSON-seeded predicate keyspace, applying the server-side predicate
    /// <c>Age &gt;= 50</c> to each candidate row. No values cross the wire, so
    /// the measured allocation isolates the per-row
    /// <c>LatticePredicateEvaluator.Matches</c> cost - the
    /// <c>JsonDocument.Parse</c> + member-resolution path that the
    /// <c>Utf8JsonReader</c> fast-path targets. The predicate matches ~half the
    /// seeded rows, so both the match and non-match branches are exercised.
    /// </summary>
    [Benchmark(Description = "Predicate key scan")]
    public async Task PredicateKeyScan()
    {
        await foreach (var _ in _lattice.KeysWherePredicateAsync(_predicate, _predScanStart, _predScanEnd))
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
    /// Fixed-shape single-tree
    /// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// over a pinned 2-key batch. Unlike <see cref="SetManyAtomic"/> (whose
    /// batch size follows <c>BENCH_MICROBENCH_ATOMIC_BATCH</c>), this row is
    /// pinned to two keys so the published single-silo perf doc can compare
    /// single-tree against cross-tree (<see cref="CrossTreeAtomic_2Keys"/>)
    /// atomic-write throughput at a matched batch size. Surfaced as
    /// <c>microbench_set_many_atomic_2_keys_per_second</c>.
    /// </summary>
    [Benchmark(Description = "SetMany atomic (2 keys)")]
    public Task SetManyAtomic_2Keys()
    {
        return _atomicLattice.SetManyAtomicAsync(_atomicBatch2);
    }

    /// <summary>
    /// Fixed-shape single-tree
    /// <see cref="ILattice.SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>
    /// over a pinned 64-key batch - the single-tree counterpart to
    /// <see cref="CrossTreeAtomic_64Keys"/> at a matched batch size. Surfaced
    /// as <c>microbench_set_many_atomic_64_keys_per_second</c>.
    /// </summary>
    [Benchmark(Description = "SetMany atomic (64 keys)")]
    public Task SetManyAtomic_64Keys()
    {
        return _atomicLattice.SetManyAtomicAsync(_atomicBatch64);
    }

    /// <summary>
    /// Cross-tree (multi-tree) atomic write over a pinned 2-key batch: one key
    /// on each of two distinct single-shard trees, committed all-or-nothing
    /// through <see cref="LatticeCrossTreeAtomicWriteExtensions.BeginAtomicWrite(IGrainFactory, string)"/>
    /// and the <see cref="LatticeCrossTreeTxGrain"/> coordinator. Each iteration
    /// mints a fresh operationId so a brand-new coordinator + per-tree sub-saga
    /// pair runs end-to-end (prepare-and-pause on every tree, then a single
    /// global commit decision). Pairs with <see cref="SetManyAtomic_2Keys"/>
    /// so an operator can read the single-tree vs multi-tree overhead at a
    /// matched 2-key shape. Surfaced as
    /// <c>microbench_cross_tree_atomic_2_keys_per_second</c>.
    /// </summary>
    [Benchmark(Description = "Cross-tree atomic (2 keys, 2 trees)")]
    public Task<CrossTreeAtomicWriteOutcome> CrossTreeAtomic_2Keys()
    {
        return CommitCrossTreeAsync(_crossTreeKeysA2, _crossTreeKeysB2);
    }

    /// <summary>
    /// Cross-tree (multi-tree) atomic write over a pinned 64-key batch split
    /// 32 keys per tree across two distinct single-shard trees, committed
    /// all-or-nothing through the cross-tree coordinator. Pairs with
    /// <see cref="SetManyAtomic_64Keys"/> at a matched 64-key shape. Surfaced
    /// as <c>microbench_cross_tree_atomic_64_keys_per_second</c>.
    /// </summary>
    [Benchmark(Description = "Cross-tree atomic (64 keys, 2 trees)")]
    public Task<CrossTreeAtomicWriteOutcome> CrossTreeAtomic_64Keys()
    {
        return CommitCrossTreeAsync(_crossTreeKeysA64, _crossTreeKeysB64);
    }

    /// <summary>
    /// Stages and commits one cross-tree atomic write: every key in
    /// <paramref name="keysA"/> targets <see cref="CrossTreeNameA"/> and every
    /// key in <paramref name="keysB"/> targets <see cref="CrossTreeNameB"/>,
    /// all under a single freshly-minted operationId. The fluent builder +
    /// per-call <see cref="Guid"/> allocation is inherent to the cross-tree API
    /// surface under measurement (a stable idempotency key is mandatory for a
    /// multi-registry saga), so it is part of the cost this row reports.
    /// </summary>
    private Task<CrossTreeAtomicWriteOutcome> CommitCrossTreeAsync(string[] keysA, string[] keysB)
    {
        var operationId = Guid.NewGuid().ToString("N");
        var builder = _grainFactory.BeginAtomicWrite(operationId).ForTree(CrossTreeNameA);
        foreach (var key in keysA)
        {
            builder.Set(key, _value);
        }
        builder.ForTree(CrossTreeNameB);
        foreach (var key in keysB)
        {
            builder.Set(key, _value);
        }
        return builder.CommitAsync();
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
        var deepSp = AuthBench.CreateServiceProvider();
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
        var deeperSp = AuthBench.CreateServiceProvider();
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
        var atomicSp = AuthBench.CreateServiceProvider();
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
        var atomicFanoutSp = AuthBench.CreateServiceProvider();
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

    /// <summary>
    /// Builds the two fixed-shape single-tree atomic batches (2 keys and
    /// 64 keys) used by <see cref="SetManyAtomic_2Keys"/> /
    /// <see cref="SetManyAtomic_64Keys"/>. Both run against the same
    /// single-shard atomic tree (<see cref="_atomicLattice"/>) the env-sized
    /// <see cref="SetManyAtomic"/> bench uses, under disjoint
    /// "atomic2-"/"atomic64-" key prefixes so they never collide with the
    /// env-sized "atomic-" keyspace. Keys are pre-seeded so the first
    /// benchmarked saga overwrites existing leaf entries (LWW) rather than
    /// triggering a leaf split.
    /// </summary>
    private void BuildFixedAtomicBatches()
    {
        _atomicBatch2 = BuildSeededAtomicBatch(_atomicLattice, "atomic2-", FixedAtomicBatch2);
        _atomicBatch64 = BuildSeededAtomicBatch(_atomicLattice, "atomic64-", FixedAtomicBatch64);
    }

    /// <summary>
    /// Builds and pre-seeds a fixed-size atomic-write batch of
    /// <paramref name="count"/> shard-spreading keys (prefixed with
    /// <paramref name="keyPrefix"/>) against <paramref name="lattice"/>.
    /// The multiplicative-prefix entropy distributes keys across shards so a
    /// multi-shard tree exercises its terminal-broadcast fan-out.
    /// </summary>
    private List<KeyValuePair<string, byte[]>> BuildSeededAtomicBatch(ILattice lattice, string keyPrefix, int count)
    {
        var batch = new List<KeyValuePair<string, byte[]>>(count);
        for (var i = 0; i < count; i++)
        {
            var k = keyPrefix + ((i * 257) & 0xFFFF).ToString("X4", CultureInfo.InvariantCulture)
                + "-" + i.ToString("D8", CultureInfo.InvariantCulture);
            batch.Add(new(k, _value));
        }
        for (var i = 0; i < count; i++)
        {
            lattice.SetAsync(batch[i].Key, _value).GetAwaiter().GetResult();
        }
        return batch;
    }

    /// <summary>
    /// Builds the two participating single-shard trees for the cross-tree
    /// (multi-tree) atomic-write benchmarks and pre-seeds their key sets.
    /// Each tree is a sibling <see cref="ILattice"/> activation (mirroring
    /// <see cref="BuildAtomicTree"/>) routed through the same NSubstitute
    /// <see cref="IGrainFactory"/>; the cross-tree coordinator
    /// (<see cref="GetOrCreateCrossTreeTx"/>) and the per-tree sub-sagas
    /// (<see cref="GetOrCreateAtomicSaga"/>) drive the all-or-nothing commit.
    /// The 2-key shape places one key on each tree; the 64-key shape splits
    /// 32 keys per tree.
    /// </summary>
    private void BuildCrossTreeTrees()
    {
        _crossTreeLatticeA = BuildCrossTreeParticipant(CrossTreeNameA);
        _crossTreeLatticeB = BuildCrossTreeParticipant(CrossTreeNameB);

        // 2-key shape: one key per tree.
        _crossTreeKeysA2 = SeedCrossTreeKeys(_crossTreeLatticeA, "xa2-", 1);
        _crossTreeKeysB2 = SeedCrossTreeKeys(_crossTreeLatticeB, "xb2-", 1);

        // 64-key shape: 32 keys per tree.
        _crossTreeKeysA64 = SeedCrossTreeKeys(_crossTreeLatticeA, "xa64-", CrossTreeKeysPerTree64);
        _crossTreeKeysB64 = SeedCrossTreeKeys(_crossTreeLatticeB, "xb64-", CrossTreeKeysPerTree64);
    }

    /// <summary>
    /// Builds a single-shard, root-is-leaf <see cref="ILattice"/> activation
    /// rooted at <paramref name="treeId"/> and registers its registry +
    /// factory routes, mirroring <see cref="BuildAtomicTree"/>. Used as a
    /// participant in the cross-tree atomic-write benchmarks; the per-tree
    /// sub-saga resolves routing via <c>GetGrain&lt;ILattice&gt;(treeId)</c>,
    /// so each participant needs its own registered activation.
    /// </summary>
    private ILattice BuildCrossTreeParticipant(string treeId)
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(treeId);
        registry.GetEntryAsync(treeId).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = _maxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = 1,
            }));

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));
        var serviceProvider = AuthBench.CreateServiceProvider();
        var lattice = new LatticeGrain(
            context,
            _grainFactory,
            _optionsMonitor,
            _optionsResolver,
            serviceProvider,
            NullLogger<LatticeGrain>.Instance);
        _grainFactory.GetGrain<ILattice>(treeId).Returns(lattice);
        return lattice;
    }

    /// <summary>
    /// Builds and pre-seeds <paramref name="count"/> cross-tree keys (prefixed
    /// with <paramref name="keyPrefix"/>) against <paramref name="lattice"/> so
    /// the cross-tree saga overwrites existing leaf entries (LWW) instead of
    /// triggering a split mid-iteration. Returns the key array for the builder.
    /// </summary>
    private string[] SeedCrossTreeKeys(ILattice lattice, string keyPrefix, int count)
    {
        var keys = new string[count];
        for (var i = 0; i < count; i++)
        {
            keys[i] = keyPrefix + i.ToString("D8", CultureInfo.InvariantCulture);
            lattice.SetAsync(keys[i], _value).GetAwaiter().GetResult();
        }
        return keys;
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
    private AzureTableWalStorageProvider _walCompressingProvider = null!;
    private WalEntry[] _walEncodeEntries = null!;
    private string _walEncodePartitionKey = null!;

    // ===== Leaf-queue (leaf-write-batching investigation, #418) profiling fixture =====
    //
    // A real WalShardGrain backed by a latency-injecting storage
    // provider, used by the LeafQueue_* workloads to measure how the
    // leaf-grain commit-turn dispatch shape interacts with WAL-ack
    // latency. The grain's own TaskCompletionSource ack-chain is the
    // production pipelining ceiling; driving it three ways (serialized,
    // pipelined, batched) isolates the cost the non-reentrant leaf turn
    // pays versus the cost a reentrant / batched leaf would pay. See
    // BuildLeafQueueFixture for the full rationale.
    private WalShardGrain _leafQueueWal = null!;
    private LatencyInjectingWalStorageProvider _leafQueueStorage = null!;
    private WalRecord[] _leafQueueRecords = null!;
    private int _leafQueueNextOffset;

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
            // Pin the baseline to the uncompressed encode path. The option
            // default is now LatticeCompression.Zstd, so leaving this unset
            // makes the no-compressor constructor below throw at GlobalSetup.
            // The compressing sibling (_walCompressingProvider) is the only
            // row that should exercise Zstd.
            Compression = LatticeCompression.None,
        });
        _walProvider = new AzureTableWalStorageProvider(options, serializer);

        // Sibling provider with per-row Zstd payload compression enabled
        // and a zero threshold, so every encoded row exercises the
        // CompressPayload hot path. The EncodeWalBatch_AzureTable_Zstd
        // benchmark below pairs against EncodeWalBatch_AzureTable so the
        // added per-row compression CPU and allocation cost is the
        // measured delta. Raise BENCH_MICROBENCH_VALUE_BYTES well above
        // the 512-byte default threshold to surface the large-blob regime
        // where compression dominates; the small-value default regime
        // surfaces the fixed per-row overhead.
        var compressingOptions = Microsoft.Extensions.Options.Options.Create(new AzureTableWalStorageOptions
        {
            ConnectionString = "UseDevelopmentStorage=true",
            TableName = "BenchEncodeProbe",
            Compression = LatticeCompression.Zstd,
            CompressionMinPayloadBytes = 0,
        });
        _walCompressingProvider = new AzureTableWalStorageProvider(
            compressingOptions,
            serializer,
            saturationSignal: null,
            compressors: new ILatticeCompressor[] { new ZstdLatticeCompressor(LatticeAzureTableServiceCollectionExtensions.DefaultCompressionLevel) });

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

    /// <summary>
    /// Compression-enabled counterpart to
    /// <see cref="EncodeWalBatch_AzureTable"/>. Drives the same
    /// <see cref="AzureTableWalStorageProvider.EncodeEntriesForBatch"/>
    /// helper over the identical pre-built entries, but through a provider
    /// configured with per-row Zstd payload compression and a zero
    /// threshold so every row takes the <c>CompressPayload</c> path. The
    /// alloc-bytes/op and time/op delta against
    /// <see cref="EncodeWalBatch_AzureTable"/> at the same
    /// <paramref name="entryCount"/> is exactly the per-row compression
    /// cost the feature adds on the encode hot path. The
    /// <c>BENCH_MICROBENCH_VALUE_BYTES</c> environment variable sizes the
    /// payload: the default (128 bytes) measures the fixed per-row
    /// compression overhead on incompressible-sized rows, while a value
    /// well above the 512-byte default threshold (e.g. 4096) surfaces the
    /// large-blob regime where the Zstd ratio dominates.
    /// </summary>
    [Benchmark(Description = "WAL encode batch (Azure Table, Zstd)")]
    [Arguments(1)]
    [Arguments(10)]
    [Arguments(50)]
    [Arguments(99)]
    public List<global::Azure.Data.Tables.TableTransactionAction> EncodeWalBatch_AzureTable_Zstd(int entryCount)
    {
        var slice = new List<WalEntry>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            slice.Add(_walEncodeEntries[i]);
        }
        var actions = new List<global::Azure.Data.Tables.TableTransactionAction>(entryCount + 1);
        _walCompressingProvider.EncodeEntriesForBatch(_walEncodePartitionKey, slice, actions);
        return actions;
    }

    // ===== Leaf-queue commit-turn dispatch probe (leaf-write-batching, #418) =====
    //
    // The leaf-write-batching investigation (issue #418) is gated on a
    // profiling
    // pre-condition: only pursue in-grain write coalescing if the leaf
    // grain's own commit turn - not the WAL layer, not key skew - is the
    // bottleneck under realistic WAL-ack latency. These three workloads
    // answer that directly. They drive a real WalShardGrain (whose
    // TaskCompletionSource ack-chain is the production pipelining
    // ceiling) behind a latency-injecting storage provider, three ways:
    //
    //   * LeafQueue_SerializedAppends - awaits each AppendAsync before
    //     issuing the next. This is exactly what a NON-reentrant leaf
    //     grain does today: every foreground commit awaits its WAL ack
    //     inside the grain turn, so write N+1 cannot begin until write N
    //     has durably landed. Expected cost ~ N x latency.
    //
    //   * LeafQueue_PipelinedAppends - issues all N AppendAsync calls
    //     concurrently and awaits the batch. This models a [Reentrant]
    //     leaf (or any design that releases the turn before the ack)
    //     letting the WAL grain's ack-chain coalesce the N appends into
    //     a handful of flushes. Expected cost ~ 1-2 x latency.
    //
    //   * LeafQueue_BatchedAppend - one AppendBatchAsync(N). This models
    //     the existing SetManyAsync coalescing path already on the public
    //     surface. Expected cost ~ 1 x latency.
    //
    // Interpretation: if Pipelined / Batched dramatically beat
    // Serialized at a realistic latency (single-digit ms for a remote
    // WAL store), the leaf-turn serialization is the proven bottleneck
    // and the reentrant-leaf / FlushAsync / WriteMode work (#418) is
    // justified. If they are close, the WAL ack-chain already absorbs
    // the latency and the lower-risk remedy (sharding, key-design
    // guidance) wins. The probe deliberately uses ONE WalShardGrain so
    // the measured contention is the single hot-leaf case the issue
    // describes, not the post-split fanned-out steady state.
    private void BuildLeafQueueFixture()
    {
        var sp = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
        var serializer = (Serializer<WalRecord>)sp.GetService(typeof(Serializer<WalRecord>))!;
        var encoder = new OrleansBinaryWalRecordEncoder(serializer);

        // Simulated per-flush WAL-ack latency. Default 4 ms approximates a
        // same-region Azure Table round-trip; override via env for a
        // latency sweep without recompiling.
        var latencyMs = ReadIntEnv("BENCH_MICROBENCH_LEAFQUEUE_LATENCY_MS", 4);
        _leafQueueStorage = new LatencyInjectingWalStorageProvider(
            new InMemoryWalStorageProvider(),
            TimeSpan.FromMilliseconds(latencyMs));

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var grainContext = Substitute.For<IGrainContext>();
        grainContext.GrainId.Returns(GrainId.Create("wal", "leafqueue-tree/0"));
        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(string.Empty);

        _leafQueueWal = new WalShardGrain(
            grainContext,
            monitor,
            new LatticeOptionsResolver(_grainFactory, monitor),
            modeResolver,
            clusterIdResolver,
            encoder);
        _leafQueueWal
            .InitializeForTestingAsync("leafqueue-tree", 0, _leafQueueStorage, CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Pre-build a pool of distinct WalRecords so neither dispatch
        // shape pays record-construction cost inside the measured loop.
        const int PoolSize = 256;
        var valueBytes = ReadIntEnv("BENCH_MICROBENCH_VALUE_BYTES", 128);
        var payload = new byte[valueBytes];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i & 0xFF);
        }

        _leafQueueRecords = new WalRecord[PoolSize];
        var hlc = HybridLogicalClock.Zero;
        for (var i = 0; i < PoolSize; i++)
        {
            hlc = HybridLogicalClock.Tick(hlc);
            _leafQueueRecords[i] = new WalRecord
            {
                TreeId = "leafqueue-tree",
                Op = MutationKind.Set,
                Key = "k-" + i.ToString("D6", CultureInfo.InvariantCulture),
                Value = payload,
                Timestamp = hlc,
                OriginClusterId = "microbench-source",
            };
        }
        _leafQueueNextOffset = 0;
    }

    /// <summary>
    /// Returns the next pre-built <see cref="WalRecord"/> from the pool,
    /// cycling so the measured loop never constructs records. The pool is
    /// large enough that a single batch never reuses a record within one
    /// invocation.
    /// </summary>
    private WalRecord NextLeafQueueRecord()
    {
        var record = _leafQueueRecords[_leafQueueNextOffset % _leafQueueRecords.Length];
        _leafQueueNextOffset++;
        return record;
    }

    /// <summary>
    /// Serialized commit-turn dispatch: awaits each
    /// <see cref="WalShardGrain.AppendAsync"/> before issuing the next,
    /// modelling today's non-reentrant leaf grain where every foreground
    /// commit blocks the grain turn on its own WAL ack. Expected cost
    /// scales ~linearly with the batch size times the injected latency.
    /// </summary>
    [Benchmark(Description = "LeafQueue serialized appends (non-reentrant leaf)")]
    [Arguments(8)]
    [Arguments(32)]
    public async Task LeafQueue_SerializedAppends(int writes)
    {
        for (var i = 0; i < writes; i++)
        {
            await _leafQueueWal.AppendAsync(NextLeafQueueRecord(), CancellationToken.None);
        }
    }

    /// <summary>
    /// Pipelined commit-turn dispatch: issues all
    /// <paramref name="writes"/> appends concurrently and awaits the
    /// batch, modelling a reentrant / turn-releasing leaf that lets the
    /// WAL grain's ack-chain coalesce the appends into a handful of
    /// flushes. Expected to be near-constant in the batch size up to the
    /// configured pending-flush cap.
    /// </summary>
    [Benchmark(Description = "LeafQueue pipelined appends (reentrant leaf)")]
    [Arguments(8)]
    [Arguments(32)]
    public async Task LeafQueue_PipelinedAppends(int writes)
    {
        var tasks = new Task<long>[writes];
        for (var i = 0; i < writes; i++)
        {
            tasks[i] = _leafQueueWal.AppendAsync(NextLeafQueueRecord(), CancellationToken.None);
        }
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Batched commit-turn dispatch: one
    /// <see cref="WalShardGrain.AppendBatchAsync"/> of
    /// <paramref name="writes"/> records, modelling the existing
    /// SetManyAsync coalescing path already on the public surface.
    /// Expected to pay a single flush latency regardless of batch size
    /// (subject to the per-flush entry cap).
    /// </summary>
    [Benchmark(Description = "LeafQueue batched append (SetMany path)")]
    [Arguments(8)]
    [Arguments(32)]
    public async Task LeafQueue_BatchedAppend(int writes)
    {
        var batch = new WalRecord[writes];
        for (var i = 0; i < writes; i++)
        {
            batch[i] = NextLeafQueueRecord();
        }
        await _leafQueueWal.AppendBatchAsync(batch, CancellationToken.None);
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
    // acceptance row's small-payload-dominated and
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

    // ===== VersionVector primitive micro-suite =====
    // Pure-compute CRDT fixtures that route through NO mock IGrainFactory and
    // NO BDN-driven async chain - so the EventPipe profiler attributes every
    // allocation directly to Orleans.Lattice.VersionVector frames
    // with zero mock/Castle contamination. Two same-size operand vectors with
    // a controllable overlap fraction exercise the pointwise-max Merge/Clone
    // allocation surface. Sized by BENCH_MICROBENCH_VV_ENTRIES (default 16),
    // which mirrors a typical replica-fan vector in the bidirectional-
    // replication scenario (a handful of clusters x a few shards each).
    private VersionVector _vvLeft = null!;
    private VersionVector _vvRight = null!;

    // Dedicated operand for the VersionVector_Tick micro-benchmark. Kept
    // separate from _vvLeft/_vvRight so the tick loop's clock advancement does
    // not perturb the Merge/Clone/MergeFrom operands (whose overlap clocks must
    // stay strictly ordered). Pre-seeded with the same replica fan so every
    // tick hits the dominant existing-entry path.
    private VersionVector _vvTick = null!;
    private string[] _vvTickReplicaIds = null!;

    // ===== OrMap primitive micro-suite operands =====
    // Two same-shape OrMap<string, PnCounter> operands. PnCounter is an
    // alloc-free value CRDT with a parameterless ctor, so the suite measures
    // OrMap's own structural allocation (the per-key Adds/Tombstones lists and
    // the backing dictionaries) rather than value-CRDT noise. Sized by
    // BENCH_MICROBENCH_ORMAP_KEYS (default 16).
    private OrMap<string, PnCounter> _orMapLeft = null!;
    private OrMap<string, PnCounter> _orMapRight = null!;
    private OrSet _orSetLeft = null!;
    private OrSet _orSetRight = null!;

    // ===== OrSet element-operation micro-suite operands =====
    // A pre-seeded OrSet plus the element array used to drive the
    // Contains read path (now allocation-free via the span-keyed lookup)
    // and the bulk-Add path. Sized by BENCH_MICROBENCH_ORSET_KEYS.
    private OrSet _orSetContainsTarget = null!;
    private byte[][] _orSetProbeElements = null!;
    private int _orSetProbeCursor;

    // ===== OrMap bulk-load micro-suite operands =====
    // Pre-built key strings and a shared alloc-free value so OrMap_BulkSet
    // isolates the per-Set NextCounter cost (O(1) counter cache) from key
    // and value allocation. Sized by BENCH_MICROBENCH_ORMAP_BULK_KEYS.
    private string[] _orMapBulkSetKeys = null!;
    private readonly PnCounter _orMapBulkSetValue = new();

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
    /// payload-bytes-dominated extremes the tracked feature calls for.
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

    /// <summary>
    /// Builds the two operand version vectors for the
    /// <see cref="VersionVector_Merge"/> / <see cref="VersionVector_Clone"/> /
    /// <see cref="VersionVector_MergeFrom"/> micro-suite. Both vectors carry
    /// <c>BENCH_MICROBENCH_VV_ENTRIES</c> entries (default 16). The right
    /// vector shares half its replica ids with the left (so the pointwise-max
    /// merge exercises both the "key present, compare clocks" and "key absent,
    /// insert" branches) and carries strictly newer clocks on the overlap so
    /// the comparison branch actually replaces.
    /// </summary>
    private void BuildVersionVectorFixture()
    {
        var entries = ReadIntEnv("BENCH_MICROBENCH_VV_ENTRIES", 16);
        if (entries < 1) entries = 1;

        _vvLeft = new VersionVector();
        _vvRight = new VersionVector();

        // Left replicas: replica-00 .. replica-(n-1), each ticked once.
        for (var i = 0; i < entries; i++)
        {
            _vvLeft.Tick($"replica-{i:D2}");
        }

        // Right replicas: the upper half overlaps the left key range (forcing
        // the compare-and-replace branch with a strictly newer clock), the
        // lower half is fresh (forcing the insert branch).
        var overlapStart = entries / 2;
        for (var i = overlapStart; i < overlapStart + entries; i++)
        {
            var id = $"replica-{i:D2}";
            // Tick twice so overlap clocks are strictly greater than the left's
            // single tick, guaranteeing Merge takes the replace path.
            _vvRight.Tick(id);
            _vvRight.Tick(id);
        }

        // Tick operand: the same replica fan as the left vector, each id
        // ticked once so it already exists. The benchmark re-ticks every id,
        // exercising the per-write existing-entry mutator path the
        // single-probe Tick optimises (one hash + bucket walk per tick).
        _vvTick = new VersionVector();
        _vvTickReplicaIds = new string[entries];
        for (var i = 0; i < entries; i++)
        {
            var id = $"replica-{i:D2}";
            _vvTickReplicaIds[i] = id;
            _vvTick.Tick(id);
        }
    }

    /// <summary>
    /// Allocating pointwise-max merge: returns a fresh <see cref="VersionVector"/>
    /// whose <c>Entries</c> dictionary is populated from both operands. This is
    /// the allocation surface the replication apply path pays on every digest
    /// reconcile; isolating it here attributes the dictionary-grow and
    /// per-vector allocation directly to the primitive with zero mock noise.
    /// </summary>
    [Benchmark(Description = "VersionVector merge (allocating)")]
    public VersionVector VersionVector_Merge() => VersionVector.Merge(_vvLeft, _vvRight);

    /// <summary>
    /// In-place pointwise-max merge: folds the right operand into a fresh clone
    /// of the left without allocating a result vector. The clone is the control
    /// for the <see cref="VersionVector_Merge"/> allocation - it keeps the
    /// benchmark idempotent (each iteration mutates a throwaway copy, not the
    /// shared fixture) while measuring the <c>MergeFrom</c> per-entry cost.
    /// </summary>
    [Benchmark(Description = "VersionVector mergeFrom (in-place)")]
    public VersionVector VersionVector_MergeFrom()
    {
        var target = _vvLeft.Clone();
        target.MergeFrom(_vvRight);
        return target;
    }

    /// <summary>
    /// Deep copy: the per-entry dictionary-fill allocation the apply path pays
    /// when snapshotting a vector before a speculative merge.
    /// </summary>
    [Benchmark(Description = "VersionVector clone")]
    public VersionVector VersionVector_Clone() => _vvLeft.Clone();

    /// <summary>
    /// Per-write vector-clock mutator: re-ticks every replica id already in the
    /// vector, so each <see cref="VersionVector.Tick(string)"/> call hits the
    /// dominant existing-entry path. This is the hot mutator the
    /// <see cref="VersionVector"/> accessor runs on every CRDT vector-clock
    /// write; isolating it here measures the single-probe lookup cost (one hash
    /// + bucket walk per tick) with zero allocation and no mock noise. The loop
    /// only advances clocks on a fixed-size dictionary, so the operand's
    /// structure stays stable across iterations.
    /// </summary>
    [Benchmark(Description = "VersionVector tick (existing replica)")]
    public HybridLogicalClock VersionVector_Tick()
    {
        var ids = _vvTickReplicaIds;
        var last = default(HybridLogicalClock);
        for (var i = 0; i < ids.Length; i++)
        {
            last = _vvTick.Tick(ids[i]);
        }
        return last;
    }

    /// <summary>
    /// Builds the two operand OR-maps for the <see cref="OrMap_Merge"/> /
    /// <see cref="OrMap_Clone"/> / <see cref="OrMap_MergeFrom"/> micro-suite.
    /// Both maps carry <c>BENCH_MICROBENCH_ORMAP_KEYS</c> keys (default 16),
    /// each with a single live dot. The right map shares the upper half of its
    /// key range with the left (forcing the per-key list-union branch in
    /// <see cref="OrMap{TKey, TValue}.MergeFrom"/>) and the lower half is fresh
    /// (forcing the whole-list copy branch). Values are alloc-free
    /// <see cref="PnCounter"/>s so the measured allocation is OrMap's own
    /// structural state (per-key lists + the Adds/Tombstones dictionaries),
    /// not value-CRDT churn.
    /// </summary>
    private void BuildOrMapFixture()
    {
        var keys = ReadIntEnv("BENCH_MICROBENCH_ORMAP_KEYS", 16);
        if (keys < 1) keys = 1;

        _orMapLeft = new OrMap<string, PnCounter>();
        _orMapRight = new OrMap<string, PnCounter>();

        for (var i = 0; i < keys; i++)
        {
            var counter = new PnCounter();
            counter.Increment("replica-left", i + 1);
            _orMapLeft.Set($"key-{i:D4}", "replica-left", counter);
        }

        // Right map: upper half of the key range overlaps the left (distinct
        // author dot => the union branch keeps both entries), lower half is a
        // fresh key range (=> the new-key whole-list-copy branch).
        var overlapStart = keys / 2;
        for (var i = overlapStart; i < overlapStart + keys; i++)
        {
            var counter = new PnCounter();
            counter.Increment("replica-right", i + 1);
            _orMapRight.Set($"key-{i:D4}", "replica-right", counter);
        }
    }

    /// <summary>
    /// Allocating lattice merge: <see cref="OrMap{TKey, TValue}.Merge"/> clones
    /// the left operand (the locus under test - the per-key list + dictionary
    /// fill) then folds the right in. This is the allocation surface the
    /// replication apply path pays on every OR-map reconcile.
    /// </summary>
    [Benchmark(Description = "OrMap merge (allocating)")]
    public OrMap<string, PnCounter> OrMap_Merge() => OrMap<string, PnCounter>.Merge(_orMapLeft, _orMapRight);

    /// <summary>
    /// In-place merge: folds the right operand into a fresh clone of the left.
    /// The clone is the control for the <see cref="OrMap_Merge"/> allocation;
    /// each iteration mutates a throwaway copy, not the shared fixture.
    /// </summary>
    [Benchmark(Description = "OrMap mergeFrom (in-place)")]
    public OrMap<string, PnCounter> OrMap_MergeFrom()
    {
        var target = _orMapLeft.Clone();
        target.MergeFrom(_orMapRight);
        return target;
    }

    /// <summary>
    /// Deep copy: the per-key list-fill + Adds/Tombstones dictionary-fill
    /// allocation that <see cref="OrMap{TKey, TValue}.Merge"/> pays on every
    /// fold via its internal <c>left.Clone()</c>.
    /// </summary>
    [Benchmark(Description = "OrMap clone")]
    public OrMap<string, PnCounter> OrMap_Clone() => _orMapLeft.Clone();

    /// <summary>
    /// Builds the two operand OR-sets for the <see cref="OrSet_Merge"/>
    /// micro-suite. Both sets carry <c>BENCH_MICROBENCH_ORSET_KEYS</c>
    /// elements (default 16), each with a single live dot - the common
    /// 1-2-concurrent-add steady state. The right set shares the upper half
    /// of its element range with the left (each element a distinct author
    /// dot, forcing the per-key list-union dedup branch in
    /// <see cref="OrSet.MergeFrom"/>) and the lower half is fresh (the
    /// whole-list copy branch). With one dot per element the dot lists stay
    /// under the linear-scan threshold, so the merge takes the
    /// HashSet-free path this change exists to measure.
    /// </summary>
    private void BuildOrSetFixture()
    {
        var keys = ReadIntEnv("BENCH_MICROBENCH_ORSET_KEYS", 16);
        if (keys < 1) keys = 1;

        _orSetLeft = new OrSet();
        _orSetRight = new OrSet();

        for (var i = 0; i < keys; i++)
        {
            _orSetLeft.Add(new byte[] { (byte)i }, "replica-left", i + 1);
        }
        var overlapStart = keys / 2;
        for (var i = overlapStart; i < overlapStart + keys; i++)
        {
            _orSetRight.Add(new byte[] { (byte)i }, "replica-right", i + 1);
        }
    }

    /// <summary>
    /// Allocating lattice merge: <see cref="OrSet.Merge"/> clones the left
    /// operand and folds the right in. With single-dot elements the per-key
    /// union takes the linear-scan branch and allocates no transient
    /// HashSet per element - the allocation surface the replication apply
    /// path pays on every OR-set reconcile.
    /// </summary>
    [Benchmark(Description = "Crdt orset merge")]
    public OrSet OrSet_Merge() => OrSet.Merge(_orSetLeft, _orSetRight);

    /// <summary>
    /// Deep copy: the per-key dot-list-fill + Adds/Tombstones dictionary-fill
    /// allocation that <see cref="OrSet.Merge"/> pays on every fold via its
    /// internal <c>left.Clone()</c>. Presizing the two backing dictionaries to
    /// the source key counts removes the intermediate rehash grows the former
    /// entry-by-entry fill paid, which this benchmark isolates from the fold.
    /// </summary>
    [Benchmark(Description = "Crdt orset clone")]
    public OrSet OrSet_Clone() => _orSetLeft.Clone();

    /// <summary>
    /// Builds the pre-seeded OR-set and probe-element array for the
    /// <see cref="OrSet_Contains"/> / <see cref="OrSet_Add"/> micro-suite.
    /// Elements are 24 bytes (a realistic content-addressed key size whose
    /// base64 encoding still keys through the stack buffer), sized by
    /// <c>BENCH_MICROBENCH_ORSET_KEYS</c> (default 16). This isolates the
    /// per-operation base64 key handling that the span-keyed lookup makes
    /// allocation-free on the read path.
    /// </summary>
    private void BuildOrSetOpsFixture()
    {
        var keys = ReadIntEnv("BENCH_MICROBENCH_ORSET_KEYS", 16);
        if (keys < 1) keys = 1;

        _orSetContainsTarget = new OrSet();
        _orSetProbeElements = new byte[keys][];
        for (var i = 0; i < keys; i++)
        {
            var element = new byte[24];
            element[0] = (byte)i;
            element[1] = (byte)(i >> 8);
            element[2] = (byte)(i >> 16);
            element[3] = (byte)(i >> 24);
            _orSetProbeElements[i] = element;
            _orSetContainsTarget.Add(element, "replica", i + 1);
        }
    }

    /// <summary>
    /// Builds the pre-computed key strings for the <see cref="OrMap_BulkSet"/>
    /// micro-suite. Sized by <c>BENCH_MICROBENCH_ORMAP_BULK_KEYS</c>
    /// (default 256) so the O(1)-per-Set counter cache is exercised across
    /// a bulk load large enough that the former O(total-dots) rescan showed
    /// as O(N^2).
    /// </summary>
    private void BuildOrMapBulkSetFixture()
    {
        var keys = ReadIntEnv("BENCH_MICROBENCH_ORMAP_BULK_KEYS", 256);
        if (keys < 1) keys = 1;

        _orMapBulkSetKeys = new string[keys];
        for (var i = 0; i < keys; i++)
        {
            _orMapBulkSetKeys[i] = $"key-{i:D6}";
        }
    }

    /// <summary>
    /// Read-path instrument: a single <see cref="OrSet.Contains(byte[])"/>
    /// against a pre-seeded set. The span-keyed alternate lookup encodes
    /// the element's base64 into a stack buffer, so the probe no longer
    /// allocates a fresh key string per read - the allocation reduction
    /// this benchmark exists to quantify.
    /// </summary>
    [Benchmark(Description = "OrSet contains")]
    public bool OrSet_Contains()
    {
        var elements = _orSetProbeElements;
        var i = unchecked(_orSetProbeCursor++) & int.MaxValue;
        return _orSetContainsTarget.Contains(elements[i % elements.Length]);
    }

    /// <summary>
    /// Write-path instrument: bulk <see cref="OrSet.Add(byte[], string, long)"/>
    /// of every probe element into a fresh set. Each distinct element still
    /// materialises one backing key string (unavoidable - it is stored), but
    /// the base64 is now encoded once via <c>TryToBase64Chars</c> rather than
    /// through an intermediate throwaway string on every call.
    /// </summary>
    [Benchmark(Description = "OrSet add (bulk)")]
    public OrSet OrSet_Add()
    {
        var elements = _orSetProbeElements;
        var set = new OrSet();
        for (var i = 0; i < elements.Length; i++)
        {
            set.Add(elements[i], "replica", i + 1);
        }
        return set;
    }

    /// <summary>
    /// Bulk-load instrument: <c>N</c> sequential
    /// <see cref="OrMap{TKey, TValue}.Set(TKey, string, TValue)"/> calls on
    /// one replica into a fresh map. With the per-replica counter cache each
    /// Set mints its dot in O(1), so the whole load is O(N); before the cache
    /// every Set rescanned every existing dot, making the load O(N^2). A
    /// shared alloc-free value keeps the measurement on the counter path.
    /// </summary>
    [Benchmark(Description = "OrMap bulk set (O(N) counter)")]
    public OrMap<string, PnCounter> OrMap_BulkSet()
    {
        var keys = _orMapBulkSetKeys;
        var value = _orMapBulkSetValue;
        var map = new OrMap<string, PnCounter>();
        for (var i = 0; i < keys.Length; i++)
        {
            map.Set(keys[i], "replica", value);
        }
        return map;
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

/// <summary>
/// <see cref="IWalStorageProvider"/> decorator that injects a fixed
/// asynchronous delay on the flush write seam
/// (<see cref="AppendEncodedBatchAsync"/>) before forwarding to an inner
/// provider, and forwards every other member verbatim. Used by the
/// leaf-queue commit-turn dispatch probe (leaf-write-batching, #418) so a real
/// <c>WalShardGrain</c> pays a realistic per-flush WAL-ack latency while
/// keeping the rest of the storage contract (offset assignment, read,
/// reconcile) at the cheap in-memory cost. The delay models the round
/// trip to a remote durable store (Azure Table Storage, Cosmos DB) that
/// the in-memory provider does not otherwise pay, which is the latency
/// the leaf-turn serialization question turns on.
/// <para>
/// Only the write seam the grain's flush path actually calls
/// (<see cref="AppendEncodedBatchAsync"/>) is delayed. The grain never
/// invokes <see cref="AppendBatchAsync"/> on the provider directly - its
/// own <c>AppendBatchAsync</c> entry point still funnels through the
/// same flush ack-chain - so a single delayed seam is sufficient to make
/// all three dispatch shapes observe the same per-flush cost.
/// </para>
/// </summary>
internal sealed class LatencyInjectingWalStorageProvider(
    IWalStorageProvider inner,
    TimeSpan flushLatency) : IWalStorageProvider
{
    /// <inheritdoc />
    public Task AppendBatchAsync(
        string treeId,
        int shardIndex,
        IReadOnlyList<WalEntry> entries,
        CancellationToken cancellationToken)
        => inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

    /// <inheritdoc />
    public async Task AppendEncodedBatchAsync(
        string treeId,
        int shardIndex,
        ReadOnlyMemory<ArraySegment<byte>> encodedEntries,
        ReadOnlyMemory<long> offsets,
        IWalRecordEncoder encoder,
        CancellationToken cancellationToken)
    {
        if (flushLatency > TimeSpan.Zero)
        {
            await Task.Delay(flushLatency, cancellationToken).ConfigureAwait(false);
        }
        await inner.AppendEncodedBatchAsync(treeId, shardIndex, encodedEntries, offsets, encoder, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<WalEntry> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        int maxEntries,
        CancellationToken cancellationToken)
        => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

    /// <inheritdoc />
    public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
        => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
}
