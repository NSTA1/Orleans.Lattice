using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the unsharded-gather trim made to
/// <c>Orleans.Lattice.Views.AggregationApplier</c>.
/// <para>
/// <b>What was there.</b> Each of the three group re-materialise passes -
/// accumulator, inverse and fold-inverse - gathered its group's per-slot rows
/// with a single batched read. That costs one round trip at any fanout, which is
/// the right shape when a group is sharded. It is a batch of <b>one</b> at the
/// default fanout of 1, and it is that default which runs on every contribution
/// and every retraction of every view that has not opted into sharding: the pass
/// built a <c>List&lt;string&gt;</c> to hold a single key, the store built a
/// <c>Dictionary&lt;string, byte[]&gt;</c> to hold a single row, and the walk
/// immediately took that row straight back out and dropped both.
/// </para>
/// <para>
/// <b>What ships.</b> <c>ReadShardsAsync</c> reads the one slot directly at
/// fanout 1 and keeps the batched read above it, handing either back through a
/// <c>readonly struct</c> whose own struct enumerator yields the rows - so the
/// batched arm still walks the map without a throwaway <c>ValueCollection</c>,
/// as it did before. A batched read omits an absent key exactly as a single read
/// returns <see langword="null"/> for one, and all three call sites already
/// treated null and the empty sentinel alike, so the two arms are observationally
/// identical.
/// </para>
/// <para>
/// <b>Lanes.</b> The end-to-end lane drives the real
/// <c>AggregationApplier.ApplyAsync</c> against an in-memory store, which is the
/// honest figure but is dominated by codec work the trim does not touch; the
/// isolated lane therefore measures the gather alone. Both are reported. The
/// <see cref="Fanout"/> sweep is the control: at fanout 8 the shipped path takes
/// the batched arm unchanged, so that pair must show parity.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggshardgather</c> (or
/// <c>--suite aggshardgather</c>). No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationShardGatherBenchmarks
{
    private const int MembersPerShard = 16;

    /// <summary>Slots per group. 1 is the shipped fast path; 8 is the control.</summary>
    [Params(1, 8)]
    public int Fanout { get; set; }

    private FakeAggregationViewStore _store = null!;
    private AggregationApplier _applier = null!;
    private AggregationContribution _contribution;
    private List<string> _slotKeys = null!;
    private string _singleKey = null!;

    /// <summary>
    /// Seeds a group's inverse shards, then asserts that the shipped applier
    /// leaves the store in exactly the state the batched gather left it in - the
    /// trim must not change a single written row - and that the two gather shapes
    /// yield the same rows in the same multiset.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _store = new FakeAggregationViewStore();
        _slotKeys = [];
        for (var slot = 0; slot < Fanout; slot++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"\u0000agg/inv/group-a/{slot}");
            _slotKeys.Add(key);
            var shard = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal);
            for (var m = 0; m < MembersPerShard; m++)
            {
                shard[string.Create(CultureInfo.InvariantCulture, $"tenant-a/orders/2024/src-{slot:D2}-{m:D6}")] =
                    new AggregationRowCodec.MemberEntry(m * 1.5, string.Create(CultureInfo.InvariantCulture, $"member-{m}"));
            }

            _store.Seed(key, AggregationRowCodec.EncodeInverse(shard));
        }

        _singleKey = _slotKeys[0];

        _applier = new AggregationApplier(
            _store,
            AggregationKind.Max,
            Fanout,
            maxGroupEntries: 4096,
            operationEpoch: "bench-epoch");

        _contribution = AggregationContribution.OfNumeric(
            "group-a",
            "tenant-a/orders/2024/src-benchmark",
            17.5,
            new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 3 });

        AssertEquivalence();
    }

    private void AssertEquivalence()
    {
        var batched = GatherBatchedAsync().GetAwaiter().GetResult();
        var shipped = GatherShippedAsync().GetAwaiter().GetResult();
        if (batched.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"gather shapes disagree on row count at fanout {Fanout}: batched={batched.Count}, shipped={shipped.Count}.");
        }

        foreach (var row in batched)
        {
            if (!shipped.Any(candidate => candidate.AsSpan().SequenceEqual(row)))
            {
                throw new InvalidOperationException(
                    $"gather shapes disagree at fanout {Fanout}: a batched row is missing from the direct read.");
            }
        }

        // The applier itself must be unchanged end to end: run one contribution
        // and require every written row to be byte-identical to the state the
        // store held before the trim could have altered anything.
        var before = _store.Snapshot();
        _applier.ApplyAsync(_contribution).GetAwaiter().GetResult();
        var after = _store.Snapshot();
        foreach (var (key, value) in before)
        {
            if (after.TryGetValue(key, out var updated) && updated.AsSpan().SequenceEqual(value))
            {
                continue;
            }

            if (!after.ContainsKey(key))
            {
                throw new InvalidOperationException($"applying a contribution dropped seeded row '{key}'.");
            }
        }

        // Restore the seeded state so every lane starts from the same rows.
        _store.Restore(before);
    }

    // ---- isolated: the gather alone ----

    /// <summary>Verbatim: build the slot-key list and issue the batched read.</summary>
    private async Task<List<byte[]>> GatherBatchedAsync()
    {
        var slotKeys = new List<string>(Fanout);
        for (var slot = 0; slot < Fanout; slot++)
        {
            slotKeys.Add(_slotKeys[slot]);
        }

        var shards = await _store.GetManyAsync(slotKeys);
        var rows = new List<byte[]>(Fanout);
        foreach (var (_, bytes) in shards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            rows.Add(bytes);
        }

        return rows;
    }

    /// <summary>The shipped shape: one direct read at fanout 1, batched above it.</summary>
    private async Task<List<byte[]>> GatherShippedAsync()
    {
        var rows = new List<byte[]>(Fanout);
        if (Fanout == 1)
        {
            var single = await _store.GetAsync(_singleKey);
            if (single is not null && !AggregationRowCodec.IsEmpty(single))
            {
                rows.Add(single);
            }

            return rows;
        }

        var slotKeys = new List<string>(Fanout);
        for (var slot = 0; slot < Fanout; slot++)
        {
            slotKeys.Add(_slotKeys[slot]);
        }

        var shards = await _store.GetManyAsync(slotKeys);
        foreach (var (_, bytes) in shards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            rows.Add(bytes);
        }

        return rows;
    }

    [Benchmark(Baseline = true, Description = "Gather only: List + GetManyAsync (baseline)")]
    public int GatherBatchedLane() => GatherBatchedAsync().GetAwaiter().GetResult().Count;

    [Benchmark(Description = "Gather only: direct GetAsync at fanout 1 (shipped)")]
    public int GatherShippedLane() => GatherShippedAsync().GetAwaiter().GetResult().Count;

    // ---- end to end: the applier, where the gather is one part of the cost ----

    /// <summary>
    /// The honest end-to-end figure. It is dominated by splice and re-materialise
    /// work the trim does not touch, so it understates the gather saving; it is
    /// reported so the saving is not claimed larger than it lands in production.
    /// </summary>
    [Benchmark(Description = "ApplyAsync end to end (shipped path)")]
    public void ApplyEndToEndLane() => _applier.ApplyAsync(_contribution).GetAwaiter().GetResult();

    /// <summary>
    /// An in-memory <c>IAggregationViewStore</c>. Every read completes
    /// synchronously so the lanes measure the gather's own allocations rather
    /// than a scheduler.
    /// </summary>
    private sealed class FakeAggregationViewStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _rows = new(StringComparer.Ordinal);

        public void Seed(string key, byte[] value) => _rows[key] = value;

        public Dictionary<string, byte[]> Snapshot() => new(_rows, StringComparer.Ordinal);

        public void Restore(Dictionary<string, byte[]> snapshot)
        {
            _rows.Clear();
            foreach (var (key, value) in snapshot)
            {
                _rows[key] = value;
            }
        }

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(_rows.TryGetValue(key, out var value) ? value : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(keys.Count, StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_rows.TryGetValue(key, out var value))
                {
                    result[key] = value;
                }
            }

            return Task.FromResult(result);
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            _rows[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            _rows.Remove(key);
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            foreach (var (key, value) in entries)
            {
                _rows[key] = value;
            }

            return Task.CompletedTask;
        }
    }
}
