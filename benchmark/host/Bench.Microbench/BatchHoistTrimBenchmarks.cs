using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three per-entry costs that a batched write or read wave used to pay
/// once per entry for a result that is invariant across the whole batch, so the
/// byte and time deltas are measurable in the clear rather than buried under a
/// silo, a transport and a storage provider.
/// <para>
/// (1) <b>Batch event publication.</b> <c>LatticeGrain</c>'s three post-commit
/// publication loops (<c>SetManyAsync</c>, <c>SetManyWhereAsync</c>,
/// <c>ApplyCrdtDeltaManyAsync</c>) check the events gate once and then call a
/// per-event publish helper for every written key. That helper re-resolved the
/// keyed <c>IStreamProvider</c> out of DI, rebuilt a <see cref="StreamId"/>
/// (which UTF8-encodes the namespace and the tree id into fresh byte arrays),
/// re-derived the per-tree stream handle, re-read the ambient
/// <c>operationId</c> from <see cref="RequestContext"/>, and rebuilt the
/// tree/tenant metric tags - all identical for every entry in the batch. The
/// shipped shape resolves that once per batch via
/// <c>LatticeEventPublisher.CreateBatch</c> and leaves only
/// <c>OnNextAsync</c> in the loop. <b>Both arms call the real shipped
/// publisher</b>: the baseline arm drives the unchanged per-event
/// <c>CreateEvent</c> + <c>PublishAsync</c> pair, which is still production
/// code and still used by the ~30 single-event call sites.
/// </para>
/// <para>
/// (2) <b>WAL route task shape.</b> <c>WalCommitLogWriter.RouteAsync</c> was
/// declared <c>async Task&lt;(WalRecord, int, int, LatticeOptions)&gt;</c>, and
/// <c>AppendManyAsync</c> awaits it once per entry. Its only await -
/// the resolver's <c>GetWalPartitionsAsync</c> - completes synchronously on
/// every cache hit, so the method never suspended in steady state yet
/// <c>AsyncTaskMethodBuilder&lt;T&gt;.SetResult</c> still allocated a
/// <c>Task&lt;ValueTuple&lt;...&gt;&gt;</c> per call (the builder only caches
/// completed tasks for a few primitive results, never for a 4-tuple). The
/// shipped shape splits a synchronous fast path out behind a
/// <c>ValueTask&lt;T&gt;</c>, mirroring
/// <c>ShardRootGrain.GetRoutingTableSnapshotAsync</c>. <c>RouteAsync</c> is
/// <c>private</c> and so unreachable even under <c>InternalsVisibleTo</c>;
/// this lane therefore models the two return shapes over the <b>real</b>
/// <see cref="WalRecord"/> and <see cref="LatticeOptions"/> payload types, with
/// both arms sharing an identical body and differing only in the awaited
/// return type - which is precisely the shipped delta.
/// </para>
/// <para>
/// (3) <b>Leaf cache double probe.</b> <c>LeafCacheGrain.GetManyAsync</c>
/// walked its key list twice: a partition pass that probed the payload cache
/// for every key to decide whether the key must be delegated to the primary
/// leaf, then a serve pass that probed the very same key again to read the
/// value out. In the steady state (nothing pending, nothing migrated, nothing
/// payload-evicted) nothing delegates, so every key paid two dictionary
/// lookups for one answer. The shipped shape fuses the passes and reinstates
/// the two-pass form only once a key actually delegates. <b>Both arms drive
/// the real shipped <c>LeafPayloadCache</c></b> - the type whose lookups are
/// being halved - through <c>TryPeek</c> and <c>RecordHit</c>.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=batchhoisttrims</c> (or
/// <c>--suite batchhoisttrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class BatchHoistTrimBenchmarks
{
    private const int BatchSize = 256;
    private const string TreeId = "bench-tree";

    // ---- (1) one post-commit publication wave over a mid-sized batch ----
    private IServiceProvider _services = null!;
    private LatticeOptions _publishOptions = null!;
    private string[] _publishKeys = null!;

    // ---- (2) one AppendManyAsync-sized routing wave ----
    private WalRecord[] _walEntries = null!;
    private LatticeOptions _walOptions = null!;

    // ---- (3) one GetManyAsync-sized read batch against a warm cache ----
    private LeafPayloadCache _payloadCache = null!;
    private List<string> _cacheKeys = null!;

    [GlobalSetup]
    public void Setup()
    {
        // (1) A hand-rolled stream provider rather than a substitute: an
        // interception framework's per-call bookkeeping would dwarf the
        // per-event resolution cost this lane exists to measure. OnNextAsync
        // returns a completed task, which prices the downstream queue write at
        // zero - correct here, because this change removes per-event
        // *resolution* work and does not remove or overlap any round trip, so
        // a completed store isolates exactly what moved.
        _publishOptions = new LatticeOptions
        {
            PublishEvents = true,
            EventStreamProviderName = "Default",
        };
        _services = new ServiceCollection()
            .AddKeyedSingleton<IStreamProvider>("Default", new CompletedStreamProvider())
            .BuildServiceProvider();
        _publishKeys = new string[BatchSize];
        for (var i = 0; i < BatchSize; i++)
        {
            _publishKeys[i] = $"key-{i:D6}";
        }

        // (2) Real WAL records over the real per-tree options object.
        _walOptions = new LatticeOptions();
        _walEntries = new WalRecord[BatchSize];
        for (var i = 0; i < BatchSize; i++)
        {
            _walEntries[i] = new WalRecord
            {
                TreeId = TreeId,
                Key = $"key-{i:D6}",
                Op = MutationKind.Set,
                Value = new byte[16],
                Timestamp = HybridLogicalClock.Tick(default),
            };
        }

        // (3) A warm cache holding every key of the batch as a live,
        // payload-resident, non-migrated entry - the steady-state shape in
        // which the old code probed twice and the new code probes once.
        _payloadCache = new LeafPayloadCache();
        _cacheKeys = new List<string>(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            var key = $"key-{i:D6}";
            _cacheKeys.Add(key);
            _payloadCache.Set(key, new LwwValue<byte[]>
            {
                Value = new byte[16],
                Timestamp = HybridLogicalClock.Tick(default),
                IsTombstone = false,
                ExpiresAtTicks = 0,
                IsMigrated = false,
            });
        }
    }

    // ─────────────────── (1) batch event publication ───────────────────

    /// <summary>
    /// Prior shape: every entry re-resolves the keyed stream provider, rebuilds
    /// the <see cref="StreamId"/> and stream handle, re-reads the ambient
    /// operation id, and rebuilds the metric tags. Calls the real, still-shipped
    /// per-event publisher entry points.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Publish/per-event resolution (prior)")]
    public async Task Publish_PerEventResolution()
    {
        for (var i = 0; i < _publishKeys.Length; i++)
        {
            var evt = LatticeEventPublisher.CreateEvent(
                LatticeTreeEventKind.Set, TreeId, _publishKeys[i]);
            await LatticeEventPublisher.PublishAsync(
                _services, _publishOptions, evt, NullLogger.Instance);
        }
    }

    /// <summary>
    /// Shipped shape: the batch-invariant publication state is resolved once and
    /// only <c>OnNextAsync</c> remains per entry. Calls the real, shipped
    /// <c>CreateBatch</c> seam.
    /// </summary>
    [Benchmark(Description = "Publish/batch-scoped resolution (shipped)")]
    public async Task Publish_BatchScopedResolution()
    {
        var batch = LatticeEventPublisher.CreateBatch(
            _services, _publishOptions, TreeId, NullLogger.Instance);
        for (var i = 0; i < _publishKeys.Length; i++)
        {
            await batch.PublishAsync(LatticeTreeEventKind.Set, _publishKeys[i]);
        }
    }

    // ─────────────────── (2) WAL route task shape ───────────────────

    /// <summary>
    /// Prior shape: <c>async Task&lt;4-tuple&gt;</c> awaited once per entry over
    /// a synchronously-completing partition lookup, so a task is allocated per
    /// entry despite the method never suspending.
    /// </summary>
    [Benchmark(Description = "WalRoute/async Task (prior)")]
    public async Task<int> WalRoute_AsyncTask()
    {
        var acc = 0;
        for (var i = 0; i < _walEntries.Length; i++)
        {
            var routed = await RouteAsTaskAsync(_walEntries[i]);
            acc += routed.Partition;
        }
        return acc;
    }

    /// <summary>
    /// Shipped shape: a <c>ValueTask&lt;4-tuple&gt;</c> whose synchronous fast
    /// path returns without an async state machine or a task allocation, with
    /// the suspending case kept behind a separate non-inlined slow method.
    /// </summary>
    [Benchmark(Description = "WalRoute/ValueTask fast path (shipped)")]
    public async Task<int> WalRoute_ValueTaskFastPath()
    {
        var acc = 0;
        for (var i = 0; i < _walEntries.Length; i++)
        {
            var routed = await RouteAsValueTaskAsync(_walEntries[i]);
            acc += routed.Partition;
        }
        return acc;
    }

    private async Task<(WalRecord Entry, int Partition, int WalPartitions, LatticeOptions PerTree)>
        RouteAsTaskAsync(WalRecord entry)
    {
        var partitions = await GetWalPartitionsAsync(entry.TreeId);
        return RouteCore(entry, partitions);
    }

    private ValueTask<(WalRecord Entry, int Partition, int WalPartitions, LatticeOptions PerTree)>
        RouteAsValueTaskAsync(WalRecord entry)
    {
        var partitionsTask = GetWalPartitionsAsync(entry.TreeId);
        if (partitionsTask.IsCompletedSuccessfully)
        {
            return new ValueTask<(WalRecord, int, int, LatticeOptions)>(
                RouteCore(entry, partitionsTask.Result));
        }
        return RouteSlowAsync(entry, partitionsTask);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private async ValueTask<(WalRecord Entry, int Partition, int WalPartitions, LatticeOptions PerTree)>
        RouteSlowAsync(WalRecord entry, ValueTask<int> partitionsTask)
        => RouteCore(entry, await partitionsTask);

    /// <summary>
    /// Stands in for the resolver's per-tree partition cache, which returns a
    /// synchronously-completed <see cref="ValueTask{TResult}"/> on every hit -
    /// i.e. on every append after a tree's first.
    /// </summary>
    private static ValueTask<int> GetWalPartitionsAsync(string treeId) => new(4);

    /// <summary>
    /// Body shared verbatim by both arms so the only measured difference is the
    /// awaited return type.
    /// </summary>
    private (WalRecord Entry, int Partition, int WalPartitions, LatticeOptions PerTree)
        RouteCore(WalRecord entry, int partitions)
    {
        var stamped = entry with { Key = entry.Key ?? string.Empty };
        var partition = WalPartitionHash.Compute(stamped.Key, partitions);
        return (stamped, partition, partitions, _walOptions);
    }

    // ─────────────────── (3) leaf cache probe fold ───────────────────

    /// <summary>
    /// Prior shape: a partition pass probes every key to decide delegation, then
    /// a serve pass probes the same key again to read its value out. Drives the
    /// real shipped <c>LeafPayloadCache</c>.
    /// </summary>
    [Benchmark(Description = "LeafCache/double probe (prior)")]
    public int LeafCache_DoubleProbe()
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        List<string>? delegated = null;
        HashSet<string>? delegatedSet = null;

        foreach (var key in _cacheKeys)
        {
            bool mustDelegate;
            if (_payloadCache.TryPeek(key, out var probe)
                && !probe.IsTombstone
                && !probe.IsExpired(nowTicks))
            {
                mustDelegate = probe.IsMigrated || probe.Value is null;
            }
            else
            {
                mustDelegate = false;
            }

            if (mustDelegate)
            {
                delegated ??= new List<string>();
                delegatedSet ??= new HashSet<string>();
                if (delegatedSet.Add(key)) delegated.Add(key);
            }
        }

        var result = new Dictionary<string, byte[]>(_cacheKeys.Count);
        var hits = 0;
        foreach (var key in _cacheKeys)
        {
            if (delegatedSet is not null && delegatedSet.Contains(key)) continue;
            if (_payloadCache.TryPeek(key, out var cached)
                && !cached.IsTombstone
                && !cached.IsExpired(nowTicks))
            {
                _payloadCache.RecordHit(key);
                result[key] = cached.Value!;
                hits++;
            }
        }
        return hits + result.Count;
    }

    /// <summary>
    /// Shipped shape: one probe per key serves the key directly, with the
    /// two-pass form reinstated only when a key actually delegates (which this
    /// steady-state batch never does). Drives the real shipped
    /// <c>LeafPayloadCache</c>.
    /// </summary>
    [Benchmark(Description = "LeafCache/fused single probe (shipped)")]
    public int LeafCache_FusedProbe()
    {
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        List<string>? delegated = null;
        HashSet<string>? delegatedSet = null;

        var result = new Dictionary<string, byte[]>(_cacheKeys.Count);
        var hits = 0;
        foreach (var key in _cacheKeys)
        {
            bool mustDelegate;
            LwwValue<byte[]> probe = default;
            var probeLive = false;
            if (_payloadCache.TryPeek(key, out probe)
                && !probe.IsTombstone
                && !probe.IsExpired(nowTicks))
            {
                mustDelegate = probe.IsMigrated || probe.Value is null;
                probeLive = !mustDelegate;
            }
            else
            {
                mustDelegate = false;
            }

            if (mustDelegate)
            {
                delegated ??= new List<string>();
                delegatedSet ??= new HashSet<string>();
                if (delegatedSet.Add(key)) delegated.Add(key);
                continue;
            }

            if (probeLive)
            {
                _payloadCache.RecordHit(key);
                result[key] = probe.Value!;
                hits++;
            }
        }

        if (delegated is null) return hits + result.Count;

        // Unreachable in this steady-state batch; present so the measured body
        // carries the same branch the shipped code does.
        result.Clear();
        return result.Count;
    }

    /// <summary>
    /// Minimal <see cref="IStreamProvider"/> whose streams complete
    /// <c>OnNextAsync</c> synchronously. Only the members the publisher actually
    /// reaches are implemented; the rest throw, so an unnoticed path change
    /// surfaces as a failure rather than as a silently different measurement.
    /// </summary>
    private sealed class CompletedStreamProvider : IStreamProvider
    {
        private readonly CompletedStream _stream = new();

        public string Name => "Default";

        public bool IsRewindable => false;

        public IAsyncStream<T> GetStream<T>(StreamId streamId)
            => (IAsyncStream<T>)(object)_stream;
    }

    private sealed class CompletedStream : IAsyncStream<LatticeTreeEvent>
    {
        public StreamId StreamId => default;

        public string ProviderName => "Default";

        public bool IsRewindable => false;

        public Task OnNextAsync(LatticeTreeEvent item, StreamSequenceToken? token = null)
            => Task.CompletedTask;

        public Task OnNextBatchAsync(
            IEnumerable<LatticeTreeEvent> batch, StreamSequenceToken? token = null)
            => Task.CompletedTask;

        public Task OnCompletedAsync() => Task.CompletedTask;

        public Task OnErrorAsync(Exception ex) => Task.CompletedTask;

        public Task<StreamSubscriptionHandle<LatticeTreeEvent>> SubscribeAsync(
            IAsyncObserver<LatticeTreeEvent> observer)
            => throw new NotSupportedException();

        public Task<StreamSubscriptionHandle<LatticeTreeEvent>> SubscribeAsync(
            IAsyncObserver<LatticeTreeEvent> observer,
            StreamSequenceToken? token,
            string? filterData = null)
            => throw new NotSupportedException();

        public Task<StreamSubscriptionHandle<LatticeTreeEvent>> SubscribeAsync(
            IAsyncBatchObserver<LatticeTreeEvent> observer)
            => throw new NotSupportedException();

        public Task<StreamSubscriptionHandle<LatticeTreeEvent>> SubscribeAsync(
            IAsyncBatchObserver<LatticeTreeEvent> observer, StreamSequenceToken? token)
            => throw new NotSupportedException();

        public Task<IList<StreamSubscriptionHandle<LatticeTreeEvent>>> GetAllSubscriptionHandles()
            => throw new NotSupportedException();

        public bool Equals(IAsyncStream<LatticeTreeEvent>? other) => ReferenceEquals(this, other);

        public int CompareTo(IAsyncStream<LatticeTreeEvent>? other) => 0;
    }
}
