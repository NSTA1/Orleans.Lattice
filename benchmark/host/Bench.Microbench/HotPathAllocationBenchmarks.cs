using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three steady-state allocation trims made to the
/// <c>Orleans.Lattice</c> grain hot paths so their per-operation byte deltas
/// are measurable in the clear. The full end-to-end cluster benchmarks
/// (<see cref="LatticeMicroBenchmarks"/>) route each operation through Orleans
/// serialization, persistence, and task machinery and therefore allocate on the
/// order of tens of kilobytes to megabytes per op - so a sub-kilobyte trim sits
/// well below their run-to-run noise floor and cannot be attributed there. Each
/// benchmark below reproduces exactly one optimized code shape against its prior
/// shape with no cluster in the loop, so the <c>Allocated</c> column is
/// deterministic and the baseline-vs-optimized delta is precisely the heap the
/// production change removes.
/// <para>
/// The three pairs mirror the production edits verbatim:
/// (1) <see cref="Orleans.Lattice.BPlusTree"/> shard-root batch-write guard -
/// iterating entry keys by index instead of a <c>Select(e =&gt; e.Key)</c>
/// projection removes one <c>SelectListIterator</c> per guarded call;
/// (2) the batched CRDT receiver fold - a <see langword="readonly"/>
/// <see langword="struct"/> ambient scope replaces three nested heap
/// <c>IDisposable</c> scopes per item, removing 3N scope objects per batch of N;
/// (3) the atomic-write saga prepare - deriving the sorted touched-shard set
/// from the shard-bucket dictionary keys removes one <c>HashSet&lt;int&gt;</c>
/// plus a full second key-resolution pass.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=hotpath</c> (or <c>--suite hotpath</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is
/// fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence
/// intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class HotPathAllocationBenchmarks
{
    // ---- (1) shard-root batch-write guard: Select projection vs index loop ----

    private Dictionary<int, List<KeyValuePair<string, byte[]>>> _guardEntries = null!;

    // A field-gated, never-taken branch inside a non-inlined consumer keeps the
    // baseline's Select iterator observably live (the JIT cannot prove it dead
    // and elide the allocation), exactly as the production call site did - the
    // projection was always constructed even though the guard early-returns
    // without enumerating in the steady (no-split) state.
    private bool _splitInProgress;

    // ---- (2) batched CRDT receiver ambient scope: class scopes vs struct ----

    private const string ReceiverOrigin = "bench-origin";
    private HybridLogicalClock _receiverHlc;

    // ---- (3) atomic saga prepare: double-resolve HashSet vs single-pass ----

    private Dictionary<int, string[]> _atomicKeys = null!;
    private const int AtomicShardCount = 4;

    /// <summary>Builds the per-size inputs shared by all three benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _splitInProgress = false;
        _receiverHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };

        _guardEntries = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        foreach (var n in new[] { 4, 64 })
        {
            var list = new List<KeyValuePair<string, byte[]>>(n);
            for (var i = 0; i < n; i++)
            {
                list.Add(new KeyValuePair<string, byte[]>(
                    "guard-" + i.ToString("D8", CultureInfo.InvariantCulture), System.Array.Empty<byte>()));
            }

            _guardEntries[n] = list;
        }

        _atomicKeys = new Dictionary<int, string[]>();
        foreach (var n in new[] { 1, 2, 64 })
        {
            var keys = new string[n];
            for (var i = 0; i < n; i++)
            {
                keys[i] = "atomic-" + i.ToString("D8", CultureInfo.InvariantCulture);
            }

            _atomicKeys[n] = keys;
        }
    }

    // ------------------------------------------------------------------
    // (1) Shard-root batch-write guard
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior call shape <c>ThrowIfRejectedForAnyKey(entries.Select(e =&gt; e.Key))</c>.
    /// The <c>Select</c> eagerly allocates a <c>SelectListIterator</c> at the
    /// call site on every guarded batch write, even though the steady-state
    /// guard returns without enumerating it.
    /// </summary>
    [Benchmark(Description = "Guard key projection: Select (baseline)")]
    [Arguments(4)]
    [Arguments(64)]
    public int GuardKeyProjection_Select(int n)
    {
        var entries = _guardEntries[n];
        return ConsumeGuard(System.Linq.Enumerable.Select(entries, e => e.Key));
    }

    /// <summary>
    /// Optimized: the <c>List&lt;KeyValuePair&lt;string, byte[]&gt;&gt;</c>
    /// guard overload iterates <c>entries[i].Key</c> by index, allocating no
    /// projection iterator.
    /// </summary>
    [Benchmark(Description = "Guard key projection: index loop (optimized)")]
    [Arguments(4)]
    [Arguments(64)]
    public int GuardKeyProjection_IndexLoop(int n)
    {
        var entries = _guardEntries[n];
        return ConsumeGuardByIndex(entries);
    }

    // Non-inlined guard stand-ins: model ThrowIfRejectedForAnyKey's steady-state
    // early return. The gated foreach is never taken (split not in progress) but
    // keeps the enumerable observably reachable so the baseline's iterator
    // allocation is retained rather than optimized away.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private int ConsumeGuard(IEnumerable<string> keys)
    {
        if (!_splitInProgress) return 0;
        var count = 0;
        foreach (var key in keys)
        {
            count += key.Length;
        }

        return count;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private int ConsumeGuardByIndex(List<KeyValuePair<string, byte[]>> entries)
    {
        if (!_splitInProgress) return 0;
        var count = 0;
        for (var i = 0; i < entries.Count; i++)
        {
            count += entries[i].Key.Length;
        }

        return count;
    }

    // ------------------------------------------------------------------
    // (2) Batched CRDT receiver ambient scope
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: three nested <see cref="LatticeOriginContext.With(string?)"/> /
    /// <see cref="LatticeVectorClockContext.With(VersionVector?)"/> /
    /// <see cref="LatticeHlcOverrideContext.With(HybridLogicalClock?)"/> scopes
    /// per item, each allocating a heap <c>IDisposable</c> - 3N scope objects per
    /// batch of N.
    /// </summary>
    [Benchmark(Description = "Receiver ambient scope: class scopes (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public void ReceiverAmbientScope_ClassScopes(int n)
    {
        for (var i = 0; i < n; i++)
        {
            using (LatticeOriginContext.With(ReceiverOrigin))
            using (LatticeVectorClockContext.With(null))
            using (LatticeHlcOverrideContext.With(_receiverHlc))
            {
            }
        }
    }

    /// <summary>
    /// Optimized: a single <see langword="readonly"/> <see langword="struct"/>
    /// scope per item that saves and restores all three ambient slots on the
    /// stack (identical to the production <c>CrdtReceiverAmbientScope</c>),
    /// allocating no heap scope object.
    /// </summary>
    [Benchmark(Description = "Receiver ambient scope: struct scope (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public void ReceiverAmbientScope_StructScope(int n)
    {
        for (var i = 0; i < n; i++)
        {
            using (LocalAmbientScope.Enter(ReceiverOrigin, null, _receiverHlc))
            {
            }
        }
    }

    // Local mirror of the production private CrdtReceiverAmbientScope struct so
    // the optimized lane measures the identical stack-only save/restore shape.
    private readonly struct LocalAmbientScope : System.IDisposable
    {
        private readonly string? _previousOrigin;
        private readonly VersionVector? _previousVectorClock;
        private readonly HybridLogicalClock? _previousHlc;

        private LocalAmbientScope(
            string? previousOrigin, VersionVector? previousVectorClock, HybridLogicalClock? previousHlc)
        {
            _previousOrigin = previousOrigin;
            _previousVectorClock = previousVectorClock;
            _previousHlc = previousHlc;
        }

        public static LocalAmbientScope Enter(
            string originClusterId, VersionVector? sourceVectorClock, HybridLogicalClock? sourceHlc)
        {
            var previousOrigin = LatticeOriginContext.Current;
            var previousVectorClock = LatticeVectorClockContext.Current;
            var previousHlc = LatticeHlcOverrideContext.Current;
            LatticeOriginContext.Current = originClusterId;
            LatticeVectorClockContext.Current = sourceVectorClock;
            LatticeHlcOverrideContext.Current = sourceHlc;
            return new LocalAmbientScope(previousOrigin, previousVectorClock, previousHlc);
        }

        public void Dispose()
        {
            LatticeOriginContext.Current = _previousOrigin;
            LatticeVectorClockContext.Current = _previousVectorClock;
            LatticeHlcOverrideContext.Current = _previousHlc;
        }
    }

    // ------------------------------------------------------------------
    // (3) Atomic saga prepare touched-shard set
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: resolve every key once into a <c>HashSet&lt;int&gt;</c> to
    /// collect the distinct touched shards, then resolve every key a second time
    /// while bucketing - one extra set allocation plus a redundant resolution
    /// pass per saga prepare.
    /// </summary>
    [Benchmark(Description = "Atomic touched shards: double resolve (baseline)")]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(64)]
    public int AtomicTouchedShards_DoubleResolve(int n)
    {
        var keys = _atomicKeys[n];

        var touched = new HashSet<int>();
        for (var i = 0; i < keys.Length; i++)
        {
            touched.Add(Resolve(keys[i]));
        }

        var shardBuckets = new Dictionary<int, List<(string Key, int Index)>>(touched.Count);
        for (var i = 0; i < keys.Length; i++)
        {
            var shardIndex = Resolve(keys[i]);
            if (!shardBuckets.TryGetValue(shardIndex, out var bucket))
            {
                bucket = new List<(string, int)>();
                shardBuckets[shardIndex] = bucket;
            }

            bucket.Add((keys[i], i));
        }

        var touchedSorted = new List<int>(touched);
        touchedSorted.Sort();
        return touchedSorted.Count + shardBuckets.Count;
    }

    /// <summary>
    /// Optimized: bucket every key in a single resolution pass and derive the
    /// sorted touched-shard set directly from the bucket dictionary keys, which
    /// already are the distinct touched shards.
    /// </summary>
    [Benchmark(Description = "Atomic touched shards: single pass (optimized)")]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(64)]
    public int AtomicTouchedShards_SinglePass(int n)
    {
        var keys = _atomicKeys[n];

        var shardBuckets = new Dictionary<int, List<(string Key, int Index)>>();
        for (var i = 0; i < keys.Length; i++)
        {
            var shardIndex = Resolve(keys[i]);
            if (!shardBuckets.TryGetValue(shardIndex, out var bucket))
            {
                bucket = new List<(string, int)>();
                shardBuckets[shardIndex] = bucket;
            }

            bucket.Add((keys[i], i));
        }

        var touchedSorted = new List<int>(shardBuckets.Keys);
        touchedSorted.Sort();
        return touchedSorted.Count + shardBuckets.Count;
    }

    // Deterministic stand-in for routing.Map.Resolve(key): a pure hash-to-shard
    // fold. Both lanes call it identically, so only the extra baseline pass and
    // its HashSet show up in the allocation delta.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Resolve(string key) => (int)((uint)key.GetHashCode() % AtomicShardCount);
}
