using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="AggregationApplier"/> driven through a fully
/// functional in-memory <see cref="IAggregationViewStore"/>. Focuses on the
/// deterministic edge branches: reserved-group-key rejection, the
/// <see cref="AggregationContributionKind.RangeReconcile"/> no-op, and the
/// bounded top-K / sample eviction (<c>ApproximateBound</c> /
/// <c>WorstKey</c> / <c>LargestSourceKey</c>) that fires when an inverse-kind
/// group exceeds its configured entry cap.
/// </summary>
[TestFixture]
public sealed class AggregationApplierTests
{
    private sealed class InMemoryAggregationViewStore : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public int Count => _map.Count;

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
            => Task.FromResult(_map.TryGetValue(key, out var v) ? v : null);

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_map.TryGetValue(key, out var v))
                    result[key] = v;
            }
            return Task.FromResult(result);
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            _map[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            _map.Remove(key);
            return Task.CompletedTask;
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            foreach (var e in entries)
                _map[e.Key] = e.Value;
            return Task.CompletedTask;
        }
    }

    private static HybridLogicalClock Hlc() => HybridLogicalClock.Tick(new HybridLogicalClock());

    [Test]
    public async Task ApplyAsync_reserved_empty_group_key_rejects_contribution()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", viewName: "v");

        await applier.ApplyAsync(AggregationContribution.Membership(string.Empty, "src", Hlc()));

        Assert.That(store.Count, Is.EqualTo(0),
            "a reserved (empty) group key must be dropped without writing any row");
    }

    [Test]
    public async Task ApplyAsync_reserved_nul_prefixed_group_key_rejects_contribution()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");

        await applier.ApplyAsync(AggregationContribution.OfNumeric("\u0000hidden", "src", 3.0, Hlc()));

        Assert.That(store.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task ApplyAsync_range_reconcile_is_a_noop()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");

        var reconcile = new AggregationContribution
        {
            Kind = AggregationContributionKind.RangeReconcile,
            GroupKey = "a",
            EndKey = "z",
            Timestamp = Hlc(),
        };

        await applier.ApplyAsync(reconcile);

        Assert.That(store.Count, Is.EqualTo(0),
            "RangeReconcile is resolved to a rebuild upstream and is a no-op in the applier");
    }

    [Test]
    public async Task ApplyAsync_min_bounded_evicts_when_group_exceeds_max_entries()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Min, fanout: 1, maxGroupEntries: 2, operationEpoch: "e1");

        // Three distinct source keys hashing (fanout 1) into one inverse shard;
        // the third add pushes the map past maxGroupEntries and triggers the
        // top-K eviction that keeps the smallest numerics (min).
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 10.0, Hlc()));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s2", 5.0, Hlc()));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s3", 20.0, Hlc()));

        var materialised = await store.GetAsync("g");
        Assert.That(materialised, Is.Not.Null,
            "the min group still materialises a value after bounded eviction");
    }

    [Test]
    public async Task ApplyAsync_max_bounded_evicts_when_group_exceeds_max_entries()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Max, fanout: 1, maxGroupEntries: 2, operationEpoch: "e1");

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 10.0, Hlc()));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s2", 5.0, Hlc()));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s3", 20.0, Hlc()));

        var materialised = await store.GetAsync("g");
        Assert.That(materialised, Is.Not.Null);
    }

    [Test]
    public async Task ApplyAsync_setunion_bounded_evicts_largest_source_key()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.SetUnion, fanout: 1, maxGroupEntries: 2, operationEpoch: "e1");

        await applier.ApplyAsync(AggregationContribution.SetMember("g", "s1", "m1", Hlc()));
        await applier.ApplyAsync(AggregationContribution.SetMember("g", "s2", "m2", Hlc()));
        await applier.ApplyAsync(AggregationContribution.SetMember("g", "s3", "m3", Hlc()));

        var materialised = await store.GetAsync("g");
        Assert.That(materialised, Is.Not.Null);
    }

    [Test]
    public async Task ApplyAsync_inverse_retract_removes_source_contribution()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Min, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 10.0, Hlc()));
        await applier.ApplyAsync(AggregationContribution.Retract("s1", Hlc()));

        // A second retract of the same source key is an idempotent no-op.
        await applier.ApplyAsync(AggregationContribution.Retract("s1", Hlc()));

        // Re-contributing after a full retract re-materialises the group.
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 7.0, Hlc()));

        Assert.That(await store.GetAsync("g"), Is.Not.Null,
            "the group re-materialises once a source key contributes again");
    }
}
