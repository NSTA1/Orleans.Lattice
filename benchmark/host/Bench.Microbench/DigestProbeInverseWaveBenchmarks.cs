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
/// Evidence for three grain-call reductions (perf run 162).
/// <para>
/// (1) <b>Replication digest probe.</b> <c>ReplicationDigestProbeGrain</c> resolved
/// the shard set once and then called <c>ILattice.GetLeafProjectionDigestAsync</c>
/// per shard; each forwarder call paid its own hop plus a forced routing read
/// before reaching the shard root. The probe now takes the physical tree id and the
/// shard set from its one routing read and reads each shard root directly. Hops
/// are a fixed simulated delay; the lane reports the hop count it issues.
/// </para>
/// <para>
/// (2) <b>Backup topology digests.</b> <c>LatticeBackupCaptureService.BuildTopologyAsync</c>
/// read every shard's digest through the tree's forwarder one shard after another.
/// The forwarder is kept (the topology runs under the caller's origin, so the
/// forwarder's uniform range-read gate must still apply), but the reads now overlap
/// in one <c>BoundedFanOut</c> wave (the shipped helper).
/// </para>
/// <para>
/// (3) <b>Inverse / fold applier prefetch.</b> <c>AggregationApplier</c>'s min / max /
/// set-union and fold contributions read the membership row and then, serially, the
/// target inverse shard. The target shard's key is a function of the contribution
/// alone, so both now go out as one wave. The baseline drives the <b>same shipped
/// applier</b> through a store that admits one read at a time, reproducing the old
/// serial dispatch; setup asserts both arms land identical rows.
/// </para>
/// <para>
/// Run via <c>--suite digestprobewaves</c>. No silo is needed.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class DigestProbeInverseWaveBenchmarks
{
    private const int HopMicroseconds = 200;

    private static readonly HybridLogicalClock Seed = new() { WallClockTicks = 638_000_000_000_000_000, Counter = 1 };

    private Dictionary<string, byte[]> _inverseRows = null!;
    private Dictionary<string, byte[]> _foldRows = null!;
    private HybridLogicalClock _applyHlc;

    /// <summary>Physical shards the probed or captured tree spans.</summary>
    [Params(8, 64)]
    public int Shards { get; set; }

    /// <summary>
    /// Seeds the inverse and fold view rows and asserts the serial and wave
    /// applier arms land identical rows with the expected peak reads in flight.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _inverseRows = SeedRows(AggregationKind.Min);
        _foldRows = SeedRows(AggregationKind.Fold);

        // One pinned stamp, later than every seeded row, so every arm writes identical bytes.
        _applyHlc = HybridLogicalClock.Tick(Seed);

        foreach (var kind in new[] { AggregationKind.Min, AggregationKind.Fold })
        {
            var serialRows = RunOnce(kind, serial: true, Latency.Hop, out var serialPeak);
            var waveRows = RunOnce(kind, serial: false, Latency.Hop, out var wavePeak);
            if (!SameRows(serialRows, waveRows))
            {
                throw new InvalidOperationException($"Serial and wave {kind} applier arms landed different rows.");
            }

            if (wavePeak < 2 || serialPeak != 1)
            {
                throw new InvalidOperationException(
                    $"Unexpected {kind} peak reads in flight: serial={serialPeak} wave={wavePeak}.");
            }

            Console.WriteLine(
                $"[digestprobewaves] {kind} applier peak reads in flight: serial={serialPeak} wave={wavePeak}");
        }

        Console.WriteLine(
            $"[digestprobewaves] probe hops for {Shards} shards: via-forwarder={1 + (3 * Shards)} direct={1 + Shards}");
    }

    /// <summary>Old probe: per shard, a forwarder hop, a forced routing read and the shard-root read.</summary>
    [Benchmark]
    [BenchmarkCategory("probe")]
    public async Task<int> Probe_PerShardViaForwarder()
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

    /// <summary>Shipped probe: one routing read, then each shard root directly.</summary>
    [Benchmark]
    [BenchmarkCategory("probe")]
    public async Task<int> Probe_DirectShardRoots()
    {
        var hops = 1;
        await Hop();
        for (var shard = 0; shard < Shards; shard++)
        {
            await Hop();
            hops++;
        }

        return hops;
    }

    /// <summary>Old topology: each shard's forwarder digest read awaited in turn.</summary>
    [Benchmark]
    [BenchmarkCategory("topology")]
    public async Task<int> Topology_SerialForwarderReads()
    {
        await Hop();
        var digests = new List<int>(Shards);
        for (var shard = 0; shard < Shards; shard++)
        {
            digests.Add(await ForwarderDigest(shard));
        }

        return digests.Count;
    }

    /// <summary>Shipped topology: the same forwarder reads, overlapped in one bounded wave.</summary>
    [Benchmark]
    [BenchmarkCategory("topology")]
    public async Task<int> Topology_ForwarderReadWave()
    {
        await Hop();
        var digests = await BoundedFanOut.RunAsync(
            Shards,
            BoundedFanOut.DefaultWidth,
            ForwarderDigest,
            CancellationToken.None);
        return digests.Length;
    }

    /// <summary>Old dispatch: membership, then the target inverse shard.</summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("inverse")]
    public Task Inverse_Contribution_SerialReads() => Contribute(AggregationKind.Min, serial: true);

    /// <summary>Shipped dispatch: membership and target inverse shard in one wave.</summary>
    [Benchmark]
    [BenchmarkCategory("inverse")]
    public Task Inverse_Contribution_ReadWave() => Contribute(AggregationKind.Min, serial: false);

    /// <summary>Old dispatch: membership, then the target fold-inverse shard.</summary>
    [Benchmark]
    [BenchmarkCategory("fold")]
    public Task Fold_Contribution_SerialReads() => Contribute(AggregationKind.Fold, serial: true);

    /// <summary>Shipped dispatch: membership and target fold-inverse shard in one wave.</summary>
    [Benchmark]
    [BenchmarkCategory("fold")]
    public Task Fold_Contribution_ReadWave() => Contribute(AggregationKind.Fold, serial: false);

    private static Task Hop() => SimulatedHop.Wait(HopMicroseconds);

    private static async Task<int> ForwarderDigest(int shard)
    {
        // Forwarder hop, its forced routing read, and the shard-root read.
        await Hop();
        await Hop();
        await Hop();
        return shard;
    }

    private static AggregationApplier NewApplier(IAggregationViewStore store, AggregationKind kind) =>
        kind == AggregationKind.Fold
            ? new AggregationApplier(
                store,
                AggregationKind.Fold,
                fanout: 4,
                maxGroupEntries: 0,
                operationEpoch: "e1",
                fold: new LatticeFoldProjection(_ => "g", () => [], (acc, _, value, _) => [.. acc, .. value], "v1"))
            : new AggregationApplier(store, kind, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");

    private static AggregationContribution Contribution(AggregationKind kind, string group, string source, int value, HybridLogicalClock hlc) =>
        kind == AggregationKind.Fold
            ? AggregationContribution.Fold(group, source, [(byte)value], hlc)
            : AggregationContribution.OfNumeric(group, source, value, hlc);

    private static Dictionary<string, byte[]> SeedRows(AggregationKind kind)
    {
        var store = new MemoryStore(Latency.None);
        var applier = NewApplier(store, kind);
        var hlc = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = Seed.WallClockTicks - 1_000_000 });
        for (var s = 0; s < 64; s++)
        {
            hlc = HybridLogicalClock.Tick(hlc);
            applier.ApplyAsync(Contribution(
                kind,
                string.Create(CultureInfo.InvariantCulture, $"g{s % 16:D2}"),
                string.Create(CultureInfo.InvariantCulture, $"src-{s:D4}"),
                s,
                hlc)).GetAwaiter().GetResult();
        }

        return store.Snapshot();
    }

    private Task Contribute(AggregationKind kind, bool serial) =>
        ContributeCore(kind, serial, Latency.Hop, kind == AggregationKind.Fold ? _foldRows : _inverseRows, _applyHlc);

    private Dictionary<string, byte[]> RunOnce(AggregationKind kind, bool serial, Latency latency, out int peak)
    {
        var rows = kind == AggregationKind.Fold ? _foldRows : _inverseRows;
        var inner = new MemoryStore(latency, rows);
        IAggregationViewStore store = serial ? new SerialReadStore(inner) : inner;
        Apply(store, kind, _applyHlc).GetAwaiter().GetResult();
        peak = inner.PeakInFlight;
        return inner.Snapshot();
    }

    private static async Task ContributeCore(AggregationKind kind, bool serial, Latency latency, Dictionary<string, byte[]> rows, HybridLogicalClock hlc)
    {
        var inner = new MemoryStore(latency, rows);
        IAggregationViewStore store = serial ? new SerialReadStore(inner) : inner;
        await Apply(store, kind, hlc);
    }

    // A same-group re-contribution from an existing member: the steady-state shape.
    private static Task Apply(IAggregationViewStore store, AggregationKind kind, HybridLogicalClock hlc) =>
        NewApplier(store, kind).ApplyAsync(Contribution(kind, "g03", "src-0003", 42, hlc));

    private static bool SameRows(Dictionary<string, byte[]> a, Dictionary<string, byte[]> b) =>
        a.Count == b.Count && a.All(e => b.TryGetValue(e.Key, out var v) && v.AsSpan().SequenceEqual(e.Value));

    private enum Latency
    {
        None,
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

    /// <summary>An in-memory view tree that records peak concurrent reads and can price each one.</summary>
    private sealed class MemoryStore(Latency latency, Dictionary<string, byte[]>? seed = null) : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _map = seed is null
            ? new(StringComparer.Ordinal)
            : new(seed, StringComparer.Ordinal);

        private readonly object _gate = new();
        private int _inFlight;

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
            Latency.Hop => SimulatedHop.Wait(HopMicroseconds),
            _ => Task.CompletedTask,
        };

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
}
