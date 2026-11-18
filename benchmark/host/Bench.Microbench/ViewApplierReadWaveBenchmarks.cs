using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Evidence for three call reductions on the view and tag-index maintenance paths.
/// <para>
/// (1) <b>Numeric applier read wave.</b> <c>AggregationApplier</c> now issues the
/// intent, membership and accumulator-slot point reads of a numeric contribution
/// (intent and membership for a retraction) as one overlapping wave instead of
/// serial round trips. The baseline drives the <b>same shipped applier</b> through
/// a store wrapper that admits one read at a time, which reproduces the old serial
/// dispatch without a hand-written copy of the applier; because the shipped applier
/// also stopped re-reading the slot on a same-group overwrite, the baseline is a
/// lower bound on the old cost. Each read pays a fixed simulated hop.
/// </para>
/// <para>
/// (2) <b>Read cache in the buffered aggregation batch.</b>
/// <c>BufferingAggregationViewStore</c> now remembers what it read from the view
/// tree for the life of the batch. The maintainer is the view tree's only writer
/// and every write in the window lands in the overlay, so a row read once cannot
/// change before <c>Capture()</c>. The baseline is the overlay-only store as it
/// shipped before; both arms drive the real applier over the same contributions
/// and setup asserts they capture the identical slice.
/// </para>
/// <para>
/// (3) <b>Tag-index fingerprint shard reads.</b>
/// <c>TagIndexReconcileGrain.ComputeFingerprintAsync</c> resolved routing once and
/// then called <c>ILattice.GetLeafProjectionDigestAsync</c> per shard, each of which
/// re-paid a router hop and a forced registry read before reaching the shard root,
/// serially. It now reads each shard root directly against the routing it already
/// holds, as one <c>BoundedFanOut</c> wave (the shipped helper). Grain hops are
/// modelled as a fixed simulated hop; the lane reports the hop count it issues.
/// </para>
/// <para>
/// Run via <c>--suite viewreadwaves</c>. No silo is needed.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class ViewApplierReadWaveBenchmarks
{
    private const int HopMicroseconds = 200;

    private static readonly HybridLogicalClock Seed = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 1 };

    private AggregationContribution[] _batch = null!;
    private Dictionary<string, byte[]> _viewRows = null!;

    /// <summary>Shards a fingerprinted tree spans.</summary>
    [Params(8, 64)]
    public int Shards { get; set; }

    /// <summary>
    /// Seeds the view rows the batch lanes read and asserts that the cached and
    /// uncached buffering stores capture the identical slice, and that the serial
    /// and overlapped applier arms land the identical rows.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        // A batch of 256 contributions over 16 groups and 64 sources: every source
        // contributes four times, so the batch revisits rows it already read.
        var hlc = Seed;
        var batch = new List<AggregationContribution>();
        for (var round = 0; round < 4; round++)
        {
            for (var s = 0; s < 64; s++)
            {
                hlc = HybridLogicalClock.Tick(hlc);
                batch.Add(AggregationContribution.OfNumeric(
                    string.Create(CultureInfo.InvariantCulture, $"g{(s + round) % 16:D2}"),
                    string.Create(CultureInfo.InvariantCulture, $"src-{s:D4}"),
                    s + round,
                    hlc));
            }
        }

        _batch = [.. batch];

        // The live view tree before the batch: every source already contributed once.
        var seeded = new MemoryStore(Latency.None);
        var applier = new AggregationApplier(seeded, AggregationKind.Sum, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");
        hlc = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = Seed.WallClockTicks - 1_000_000 });
        for (var s = 0; s < 64; s++)
        {
            hlc = HybridLogicalClock.Tick(hlc);
            applier.ApplyAsync(AggregationContribution.OfNumeric(
                string.Create(CultureInfo.InvariantCulture, $"g{s % 16:D2}"),
                string.Create(CultureInfo.InvariantCulture, $"src-{s:D4}"),
                s,
                hlc)).GetAwaiter().GetResult();
        }

        _viewRows = seeded.Snapshot();

        var cachedInner = new MemoryStore(Latency.None, _viewRows);
        var cachedStore = new BufferingAggregationViewStore(cachedInner);
        ApplyBatch(cachedStore).GetAwaiter().GetResult();
        var cached = cachedStore.Capture();
        var uncachedInner = new MemoryStore(Latency.None, _viewRows);
        var uncachedStore = new UncachedBufferingStore(uncachedInner);
        ApplyBatch(uncachedStore).GetAwaiter().GetResult();
        var uncached = uncachedStore.Capture();
        if (!SameSlice(cached, uncached))
        {
            throw new InvalidOperationException("Cached and uncached buffering stores captured different slices.");
        }

        if (cachedInner.Reads >= uncachedInner.Reads)
        {
            throw new InvalidOperationException(
                $"The read cache must issue fewer underlying reads ({cachedInner.Reads} vs {uncachedInner.Reads}).");
        }

        Console.WriteLine(
            $"[viewreadwaves] buffered batch underlying reads: cached={cachedInner.Reads} uncached={uncachedInner.Reads}");

        var serialRows = RunApplier(serial: true, out var serialPeak);
        var waveRows = RunApplier(serial: false, out var wavePeak);
        if (!SameRows(serialRows, waveRows))
        {
            throw new InvalidOperationException("Serial and overlapped applier arms landed different rows.");
        }

        Console.WriteLine(
            $"[viewreadwaves] applier peak reads in flight: serial={serialPeak} wave={wavePeak}");
        Console.WriteLine(
            $"[viewreadwaves] fingerprint hops for {Shards} shards: per-shard-router={1 + (3 * Shards)} direct={1 + Shards}");
    }

    /// <summary>Old dispatch: the applier's point reads admitted one at a time.</summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("applier")]
    public Task Applier_SameGroupOverwrite_SerialReads() => ApplySameGroupOverwrite(serial: true);

    /// <summary>Shipped dispatch: intent, membership and slot read as one wave.</summary>
    [Benchmark]
    [BenchmarkCategory("applier")]
    public Task Applier_SameGroupOverwrite_ReadWave() => ApplySameGroupOverwrite(serial: false);

    /// <summary>Old buffering store: every miss in the overlay re-reads the tree.</summary>
    [Benchmark]
    [BenchmarkCategory("buffer")]
    public async Task<int> BufferedBatch_Uncached()
    {
        var inner = new MemoryStore(Latency.Yield, _viewRows);
        var store = new UncachedBufferingStore(inner);
        await ApplyBatch(store);
        return inner.Reads;
    }

    /// <summary>Shipped buffering store: a row read once is served from the cache.</summary>
    [Benchmark]
    [BenchmarkCategory("buffer")]
    public async Task<int> BufferedBatch_ReadCache()
    {
        var inner = new MemoryStore(Latency.Yield, _viewRows);
        var store = new BufferingAggregationViewStore(inner);
        await ApplyBatch(store);
        return inner.Reads;
    }

    /// <summary>
    /// Old fingerprint: per shard, a router hop, a forced registry read and the
    /// shard-root read, one shard after another.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public async Task<int> Fingerprint_PerShardViaRouter()
    {
        var hops = 1;
        await Hop();
        for (var shard = 0; shard < Shards; shard++)
        {
            await Hop();
            await Hop();
            await Hop();
            hops += 3;
        }

        return hops;
    }

    /// <summary>Shipped fingerprint: one routing read, then a bounded wave of shard-root reads.</summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public async Task<int> Fingerprint_DirectShardWave()
    {
        await Hop();
        var results = await BoundedFanOut.RunAsync(
            Shards,
            BoundedFanOut.DefaultWidth,
            async _ =>
            {
                await Hop();
                return 1;
            },
            CancellationToken.None);
        return 1 + results.Sum();
    }

    private static Task Hop() => SimulatedHop.Wait(HopMicroseconds);

    private async Task ApplySameGroupOverwrite(bool serial)
    {
        var inner = new MemoryStore(Latency.Hop, _viewRows);
        IAggregationViewStore store = serial ? new SerialReadStore(inner) : inner;
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g03", "src-0003", 42.0, HybridLogicalClock.Tick(Seed)));
    }

    private Dictionary<string, byte[]> RunApplier(bool serial, out int peak)
    {
        var inner = new MemoryStore(Latency.Yield, _viewRows);
        IAggregationViewStore store = serial ? new SerialReadStore(inner) : inner;
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");
        applier.ApplyAsync(AggregationContribution.OfNumeric("g03", "src-0003", 42.0, HybridLogicalClock.Tick(Seed)))
            .GetAwaiter().GetResult();
        peak = inner.PeakInFlight;
        return inner.Snapshot();
    }

    private async Task ApplyBatch(IAggregationViewStore store)
    {
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");
        foreach (var contribution in _batch)
        {
            await applier.ApplyAsync(contribution);
        }
    }

    private static bool SameSlice(
        (List<KeyValuePair<string, byte[]>> Upserts, List<string> Deletes) a,
        (List<KeyValuePair<string, byte[]>> Upserts, List<string> Deletes) b) =>
        SameRows(a.Upserts.ToDictionary(e => e.Key, e => e.Value, StringComparer.Ordinal),
                 b.Upserts.ToDictionary(e => e.Key, e => e.Value, StringComparer.Ordinal))
        && a.Deletes.Order(StringComparer.Ordinal).SequenceEqual(b.Deletes.Order(StringComparer.Ordinal));

    private static bool SameRows(Dictionary<string, byte[]> a, Dictionary<string, byte[]> b) =>
        a.Count == b.Count && a.All(e => b.TryGetValue(e.Key, out var v) && v.AsSpan().SequenceEqual(e.Value));

    private enum Latency
    {
        None,
        Yield,
        Hop,
    }

    /// <summary>A fixed-duration simulated grain hop that does not depend on timer resolution.</summary>
    private static class SimulatedHop
    {
        public static async Task Wait(int microseconds)
        {
            await Task.Yield();
            var until = System.Diagnostics.Stopwatch.GetTimestamp()
                + (System.Diagnostics.Stopwatch.Frequency * microseconds / 1_000_000);
            var spin = new SpinWait();
            while (System.Diagnostics.Stopwatch.GetTimestamp() < until)
            {
                spin.SpinOnce(sleep1Threshold: -1);
            }
        }
    }

    /// <summary>An in-memory view tree that counts reads and can price each one.</summary>
    private sealed class MemoryStore(Latency latency, Dictionary<string, byte[]>? seed = null) : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _map = seed is null
            ? new(StringComparer.Ordinal)
            : new(seed, StringComparer.Ordinal);

        private readonly object _gate = new();
        private int _inFlight;

        public int Reads { get; private set; }

        public int PeakInFlight { get; private set; }

        public Dictionary<string, byte[]> Snapshot()
        {
            lock (_gate)
            {
                return new Dictionary<string, byte[]>(_map, StringComparer.Ordinal);
            }
        }

        public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                Reads++;
                _inFlight++;
                PeakInFlight = Math.Max(PeakInFlight, _inFlight);
            }

            await Pay();
            lock (_gate)
            {
                _inFlight--;
                return _map.TryGetValue(key, out var v) ? v : null;
            }
        }

        public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                Reads += keys.Count;
            }

            await Pay();
            lock (_gate)
            {
                var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (var key in keys)
                {
                    if (_map.TryGetValue(key, out var v))
                    {
                        result[key] = v;
                    }
                }

                return result;
            }
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                _map[key] = value;
            }

            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                _map.Remove(key);
            }

            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            lock (_gate)
            {
                foreach (var e in entries)
                {
                    _map[e.Key] = e.Value;
                }
            }

            return Task.CompletedTask;
        }

        private Task Pay() => latency switch
        {
            Latency.Yield => YieldOnce(),
            Latency.Hop => SimulatedHop.Wait(HopMicroseconds),
            _ => Task.CompletedTask,
        };

        private static async Task YieldOnce() => await Task.Yield();
    }

    /// <summary>Admits one point read at a time: the old serial dispatch shape.</summary>
    private sealed class SerialReadStore(IAggregationViewStore inner) : IAggregationViewStore
    {
        private readonly SemaphoreSlim _one = new(1, 1);

        public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            await _one.WaitAsync(cancellationToken);
            try
            {
                return await inner.GetAsync(key, cancellationToken);
            }
            finally
            {
                _one.Release();
            }
        }

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default) =>
            inner.GetManyAsync(keys, cancellationToken);

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default) =>
            inner.SetAsync(key, value, cancellationToken);

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default) =>
            inner.DeleteAsync(key, cancellationToken);

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default) =>
            inner.SetManyAtomicAsync(entries, operationId, cancellationToken);
    }

    /// <summary>The buffering store as it shipped before the read cache: overlay only.</summary>
    private sealed class UncachedBufferingStore(MemoryStore inner) : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]?> _overlay = new(StringComparer.Ordinal);

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            _overlay.TryGetValue(key, out var buffered)
                ? Task.FromResult(buffered)
                : inner.GetAsync(key, cancellationToken);

        public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            List<string>? misses = null;
            foreach (var key in keys)
            {
                if (_overlay.TryGetValue(key, out var buffered))
                {
                    if (buffered is not null)
                    {
                        result[key] = buffered;
                    }
                }
                else
                {
                    (misses ??= []).Add(key);
                }
            }

            if (misses is not null)
            {
                foreach (var (key, value) in await inner.GetManyAsync(misses, cancellationToken))
                {
                    result[key] = value;
                }
            }

            return result;
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            _overlay[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            _overlay[key] = null;
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            foreach (var entry in entries)
            {
                _overlay[entry.Key] = entry.Value;
            }

            return Task.CompletedTask;
        }

        public (List<KeyValuePair<string, byte[]>> Upserts, List<string> Deletes) Capture()
        {
            var upserts = new List<KeyValuePair<string, byte[]>>();
            var deletes = new List<string>();
            foreach (var (key, value) in _overlay)
            {
                if (value is null)
                {
                    deletes.Add(key);
                }
                else
                {
                    upserts.Add(new KeyValuePair<string, byte[]>(key, value));
                }
            }

            return (upserts, deletes);
        }
    }
}
