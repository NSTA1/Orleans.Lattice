using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three allocation reductions made to the replication
/// ship -> apply pipeline, so the per-operation time and byte deltas are
/// measurable in the clear.
/// <para>
/// Each pair runs the prior shape against its replacement with no silo, no
/// transport and no storage in the loop, so the delta is precisely the work
/// the production change removes. The end-to-end replication benchmarks route
/// every batch through Orleans serialization, gRPC framing and persistence,
/// which buries a per-entry codec fold or a per-miss node allocation below
/// their run-to-run noise floor.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) <c>VectorClockCodec</c>, the per-WAL-entry causal-frontier codec that
/// runs on both the ship leg (<c>EncodeAbsolute</c> / <c>EncodeDelta</c>) and
/// the apply leg (<c>DecodeDelta</c>). The prior form grew every result
/// <c>VersionVector</c> from an empty dictionary, so each frontier wider than
/// three origins walked the 3/7/17/37/71/... rehash chain and abandoned each
/// intermediate bucket+entry array, and <c>DecodeDelta</c>'s pointwise max
/// hashed twice on every origin the predecessor already carried. The
/// replacement presizes exactly and folds the max onto a single
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>
/// probe. Both optimized lanes call the <b>real production code</b>;
/// (2) <c>ContentManifestPlanner</c>, the per-batch content-elision planner on
/// the ship path. <c>BuildManifest</c> grew its manifest list from empty
/// through the 4/8/16/.../1024 doubling chain and <c>ComputeElidedIndices</c>
/// did the same for its result set. The replacement pre-counts the eligible
/// entries so the list is allocated at its exact final size, and derives the
/// elided set's exact size from the manifest and missing counts. The optimized
/// lane calls the <b>real production code</b>;
/// (3) <c>ReceiverAppliedContentIndex</c>'s per-tree bounded LRU, maintained
/// once per applied entry on the receiver. At steady state the partition sits
/// at capacity, so the prior shape allocated a fresh
/// <c>LinkedListNode&lt;KeyHash&gt;</c> for every admitted key and immediately
/// dropped the evicted one on the floor. The replacement recycles the evicted
/// node in place. The partition type is private to the index, so the baseline
/// lane reproduces the prior shape exactly while the optimized lane drives the
/// <b>real production</b> <c>RecordSet</c>.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=replicationtrims</c> (or
/// <c>--suite replicationtrims</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReplicationShipApplyTrimBenchmarks
{
    // ---- (1) a per-shard causal frontier of the width a multi-region estate
    //      actually carries, plus the sparse advance a delta-encoded entry
    //      ships against it ----
    private VersionVector _frontier = null!;
    private VersionVector _predecessor = null!;
    private VersionVector _delta = null!;

    // ---- (2) one drained ship batch, overwhelmingly eligible point-Sets as a
    //      steady-state LWW workload produces ----
    private WalRecord[] _batch = null!;
    private IReadOnlyList<ContentManifestEntry> _manifest = null!;
    private int[] _missing = null!;

    // ---- (3) the receiver's per-applied-entry LRU traffic: a working set
    //      wider than the partition bound, so the partition sits at capacity
    //      and every admission evicts ----
    private string[] _lruKeys = null!;
    private const int LruCapacity = 512;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // (1) 24 origins is a realistic wide-estate frontier: a dozen regions
        // each contributing a couple of distinct shipping origins.
        const int origins = 24;
        _frontier = new VersionVector();
        _predecessor = new VersionVector();
        _delta = new VersionVector();
        for (var i = 0; i < origins; i++)
        {
            var origin = "region-" + i.ToString("D2") + "/shard-" + (i % 4).ToString("D1");
            var clock = new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000L + i, Counter = i };
            _frontier.Entries[origin] = clock;
            // The predecessor trails the frontier on every origin, so the
            // pointwise max raises each slot - the branch the fold serves.
            _predecessor.Entries[origin] = new HybridLogicalClock
            {
                WallClockTicks = clock.WallClockTicks - 1_000,
                Counter = 0,
            };
            // A real delta is sparse: only the origins that actually advanced
            // between two consecutive entries on one shard.
            if (i % 3 == 0)
            {
                _delta.Entries[origin] = clock;
            }
        }

        // (2) 512-entry drain batch of value-carrying point Sets, with the
        // handful of deletes and prepared entries a real batch mixes in (those
        // are manifest-ineligible, so the exact pre-count matters).
        const int batchSize = 512;
        _batch = new WalRecord[batchSize];
        var payload = new byte[128];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)i;
        }

        for (var i = 0; i < batchSize; i++)
        {
            var isDelete = i % 32 == 0;
            _batch[i] = new WalRecord
            {
                TreeId = "orders",
                Op = isDelete ? MutationKind.Delete : MutationKind.Set,
                Key = "customer/" + i.ToString("D6"),
                Value = isDelete ? null : payload,
                Timestamp = new HybridLogicalClock
                {
                    WallClockTicks = 638_000_000_000_000_000L + i,
                    Counter = 0,
                },
                OriginClusterId = "region-00",
            };
        }

        _manifest = ContentManifestPlanner.BuildManifest(_batch);
        // A warm receiver already holds most of the content: only a small tail
        // of the manifest comes back as missing, which is the steady state the
        // elision path exists to serve.
        var missing = new List<int>();
        for (var i = 0; i < _manifest.Count; i += 16)
        {
            missing.Add(_manifest[i].EntryIndex);
        }
        _missing = missing.ToArray();

        // (3) 2048 distinct keys cycling through a 512-key partition, so after
        // the fill every subsequent admission is an evict-and-admit.
        _lruKeys = new string[2048];
        for (var i = 0; i < _lruKeys.Length; i++)
        {
            _lruKeys[i] = "customer/" + i.ToString("D6");
        }
    }

    // ========================================================================
    // (1) per-WAL-entry causal-frontier codec
    // ========================================================================

    /// <summary>
    /// The prior ship-leg shape: both the absolute snapshot and the sparse
    /// delta grew their result dictionary from empty.
    /// </summary>
    [Benchmark]
    public int VectorClockEncode_Baseline_GrowFromEmpty()
    {
        var absolute = new VersionVector();
        foreach (var (id, clock) in _frontier.Entries)
        {
            absolute.Entries[id] = clock;
        }

        var delta = new VersionVector();
        foreach (var (id, clock) in _frontier.Entries)
        {
            var prior = _predecessor.GetClock(id);
            if (clock > prior)
            {
                delta.Entries[id] = clock;
            }
        }

        return absolute.Entries.Count + delta.Entries.Count;
    }

    /// <summary>
    /// The shipped ship-leg shape, calling the real production codec: an
    /// exactly-presized bulk copy for the absolute snapshot and a
    /// source-bounded presize for the delta.
    /// </summary>
    [Benchmark]
    public int VectorClockEncode_Optimized_Presized()
        => VectorClockCodec.EncodeAbsolute(_frontier).Entries.Count
            + VectorClockCodec.EncodeDelta(_frontier, _predecessor).Entries.Count;

    /// <summary>
    /// The prior apply-leg shape: a grown-from-empty predecessor snapshot
    /// followed by a pointwise max that hashed twice on every origin the
    /// predecessor already carried.
    /// </summary>
    [Benchmark]
    public int VectorClockDecode_Baseline_DoubleProbe()
    {
        var result = new VersionVector();
        foreach (var (id, clock) in _predecessor.Entries)
        {
            result.Entries[id] = clock;
        }

        foreach (var (id, clock) in _delta.Entries)
        {
            if (result.Entries.TryGetValue(id, out var existing))
            {
                if (clock > existing)
                {
                    result.Entries[id] = clock;
                }
            }
            else
            {
                result.Entries[id] = clock;
            }
        }

        return result.Entries.Count;
    }

    /// <summary>
    /// The shipped apply-leg shape, calling the real production codec:
    /// presized snapshot, reserved merge headroom, single-probe max fold.
    /// </summary>
    [Benchmark]
    public int VectorClockDecode_Optimized_SingleProbe()
        => VectorClockCodec.DecodeDelta(_delta, _predecessor).Entries.Count;

    // ========================================================================
    // (2) per-batch content-elision planner
    // ========================================================================

    /// <summary>
    /// The prior shape: the manifest list and the elided set both grew from
    /// empty, abandoning every intermediate backing array on the way.
    /// </summary>
    [Benchmark]
    public int ContentManifest_Baseline_GrowFromEmpty()
    {
        // Read through the same IReadOnlyList<WalRecord> indexer the production
        // planner is handed, so the A/B does not credit the baseline with a
        // direct-array indexing advantage the shipped path cannot have.
        IReadOnlyList<WalRecord> batch = _batch;
        List<ContentManifestEntry>? manifest = null;
        for (var i = 0; i < batch.Count; i++)
        {
            var record = batch[i];
            if (record.Op != MutationKind.Set
                || record.IsPrepared
                || record.AtomicBatchSize != 0
                || record.Timestamp == HybridLogicalClock.Zero)
            {
                continue;
            }

            (manifest ??= new List<ContentManifestEntry>()).Add(new ContentManifestEntry
            {
                EntryIndex = i,
                Key = record.Key ?? string.Empty,
                ContentHash = ReplicationContentHash.Compute(in record),
                Hlc = record.Timestamp,
            });
        }

        var built = manifest ?? (IReadOnlyList<ContentManifestEntry>)Array.Empty<ContentManifestEntry>();

        var missing = _missing.Length == 0 ? null : new HashSet<int>(_missing);
        var elided = new HashSet<int>();
        for (var i = 0; i < built.Count; i++)
        {
            var index = built[i].EntryIndex;
            if (missing is null || !missing.Contains(index))
            {
                elided.Add(index);
            }
        }

        return built.Count + elided.Count;
    }

    /// <summary>
    /// The shipped shape, calling the real production planner: an exact
    /// eligible pre-count sizes the manifest, and the elided set is allocated
    /// at its exact final size.
    /// </summary>
    [Benchmark]
    public int ContentManifest_Optimized_ExactSized()
    {
        var built = ContentManifestPlanner.BuildManifest(_batch);
        return built.Count + ContentManifestPlanner.ComputeElidedIndices(built, _missing).Count;
    }

    // ========================================================================
    // (3) receiver-side per-applied-entry bounded LRU
    // ========================================================================

    /// <summary>
    /// The prior shape, run through a faithful mirror of the production
    /// index's outer shell (the same argument validation, lock, per-tree
    /// partition lookup and capacity floor) so the only difference measured
    /// against the optimized lane is the inner admission body: a fresh linked
    /// list node per admitted key, with the evicted node dropped on the floor.
    /// The shell is reproduced here because the partition type is private to
    /// the production index.
    /// </summary>
    [Benchmark]
    public int ReceiverLru_Baseline_NodePerMiss()
    {
        var index = new BaselineReceiverAppliedContentIndex();
        for (var i = 0; i < _lruKeys.Length; i++)
        {
            index.RecordSet("orders", _lruKeys[i], (ulong)i, LruCapacity);
        }

        return index.CountForTree("orders");
    }

    /// <summary>
    /// The shipped shape, driving the real production index: once the
    /// partition is at capacity the evicted node is recycled in place, so an
    /// admission allocates nothing.
    /// </summary>
    [Benchmark]
    public int ReceiverLru_Optimized_RecycledNode()
    {
        var index = new ReceiverAppliedContentIndex();
        for (var i = 0; i < _lruKeys.Length; i++)
        {
            index.RecordSet("orders", _lruKeys[i], (ulong)i, LruCapacity);
        }

        return index.CountForTree("orders");
    }

    /// <summary>
    /// A copy of <c>ReceiverAppliedContentIndex</c> as it stood before this
    /// change, kept structurally identical to the production type apart from
    /// the admission body under test, so the A/B isolates the node-recycling
    /// change rather than the surrounding shell.
    /// </summary>
    private sealed class BaselineReceiverAppliedContentIndex
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, BaselineTreePartition> _trees = new(StringComparer.Ordinal);

        public void RecordSet(string treeId, string key, ulong contentHash, int capacity)
        {
            ArgumentNullException.ThrowIfNull(treeId);
            ArgumentNullException.ThrowIfNull(key);

            var bounded = capacity < 1 ? 1 : capacity;
            lock (_gate)
            {
                if (!_trees.TryGetValue(treeId, out var partition))
                {
                    partition = new BaselineTreePartition();
                    _trees[treeId] = partition;
                }
                partition.Set(key, contentHash, bounded);
            }
        }

        public int CountForTree(string treeId)
        {
            ArgumentNullException.ThrowIfNull(treeId);
            lock (_gate)
            {
                return _trees.TryGetValue(treeId, out var partition) ? partition.Count : 0;
            }
        }

        private sealed class BaselineTreePartition
        {
            private readonly LinkedList<KeyHash> _order = new();
            private readonly Dictionary<string, LinkedListNode<KeyHash>> _index = new(StringComparer.Ordinal);

            public int Count => _order.Count;

            public void Set(string key, ulong contentHash, int capacity)
            {
                if (_index.TryGetValue(key, out var existing))
                {
                    existing.Value = new KeyHash(key, contentHash);
                    _order.Remove(existing);
                    _order.AddLast(existing);
                }
                else
                {
                    _index[key] = _order.AddLast(new KeyHash(key, contentHash));
                }

                while (_order.Count > capacity)
                {
                    var lru = _order.First!;
                    _index.Remove(lru.Value.Key);
                    _order.RemoveFirst();
                }
            }

            private readonly record struct KeyHash(string Key, ulong Hash);
        }
    }
}
