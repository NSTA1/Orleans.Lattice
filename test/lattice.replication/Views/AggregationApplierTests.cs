using Orleans.Lattice.Replication.Views;

namespace Orleans.Lattice.Replication.Tests.Views;

/// <summary>
/// Unit tests for <see cref="AggregationApplier"/> - the reduce / retraction
/// engine - driven against an in-memory <see cref="IAggregationViewStore"/> so
/// the convergence properties are checked without a grain or cluster.
/// </summary>
[TestFixture]
public class AggregationApplierTests
{
    private sealed class InMemoryStore : IAggregationViewStore
    {
        public Dictionary<string, byte[]> Map { get; } = new(StringComparer.Ordinal);

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default) =>
            Task.FromResult(Map.TryGetValue(key, out var v) ? v : null);

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            Map[key] = value;
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            Map.Remove(key);
            return Task.CompletedTask;
        }
    }

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static long? Count(InMemoryStore store, string group) =>
        store.Map.TryGetValue(group, out var v) ? LatticeAggregationValue.DecodeInt64(v) : null;

    private static double? Number(InMemoryStore store, string group) =>
        store.Map.TryGetValue(group, out var v) ? LatticeAggregationValue.DecodeDouble(v) : null;

    private static AggregationApplier Applier(IAggregationViewStore store, AggregationKind kind, int fanout = 1, int maxEntries = 0) =>
        new(store, kind, fanout, maxEntries);

    [Test]
    public async Task Count_two_members_materialises_two()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Count);

        await applier.ApplyAsync(AggregationContribution.Membership("g", "a", Clock(1)));
        await applier.ApplyAsync(AggregationContribution.Membership("g", "b", Clock(2)));

        Assert.That(Count(store, "g"), Is.EqualTo(2));
    }

    [Test]
    public async Task Count_delete_retracts_member()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Count);

        await applier.ApplyAsync(AggregationContribution.Membership("g", "a", Clock(1)));
        await applier.ApplyAsync(AggregationContribution.Membership("g", "b", Clock(2)));
        await applier.ApplyAsync(AggregationContribution.Retract("a", Clock(3)));

        Assert.That(Count(store, "g"), Is.EqualTo(1));
    }

    [Test]
    public async Task Sum_overwrite_retracts_prior_contribution()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "b", 5, Clock(2)));
        // Overwrite a: 10 -> 3. Sum must become 3 + 5 = 8, not 18.
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 3, Clock(3)));

        Assert.That(Number(store, "g"), Is.EqualTo(8));
    }

    [Test]
    public async Task Sum_delete_retracts_contribution()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "b", 5, Clock(2)));
        await applier.ApplyAsync(AggregationContribution.Retract("a", Clock(3)));

        Assert.That(Number(store, "g"), Is.EqualTo(5));
    }

    [Test]
    public async Task Sum_regroup_moves_contribution_between_groups()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g1", "a", 10, Clock(1)));
        // a re-groups from g1 to g2 with a new value.
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g2", "a", 4, Clock(2)));

        Assert.That(Number(store, "g1"), Is.Null, "g1 should be emptied");
        Assert.That(Number(store, "g2"), Is.EqualTo(4));
    }

    [Test]
    public async Task Min_redrives_after_deleting_current_extremum()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Min);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 5, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "b", 2, Clock(2)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "c", 9, Clock(3)));
        Assert.That(Number(store, "g"), Is.EqualTo(2));

        // Delete the current minimum: it must re-derive to the next survivor (5).
        await applier.ApplyAsync(AggregationContribution.Retract("b", Clock(4)));

        Assert.That(Number(store, "g"), Is.EqualTo(5));
    }

    [Test]
    public async Task Max_redrives_after_deleting_current_extremum()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Max);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 5, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "b", 9, Clock(2)));
        Assert.That(Number(store, "g"), Is.EqualTo(9));

        await applier.ApplyAsync(AggregationContribution.Retract("b", Clock(3)));

        Assert.That(Number(store, "g"), Is.EqualTo(5));
    }

    [Test]
    public async Task SetUnion_counts_distinct_members()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.SetUnion);

        await applier.ApplyAsync(AggregationContribution.SetMember("g", "a", "x", Clock(1)));
        await applier.ApplyAsync(AggregationContribution.SetMember("g", "b", "y", Clock(2)));
        await applier.ApplyAsync(AggregationContribution.SetMember("g", "c", "x", Clock(3)));

        Assert.That(Count(store, "g"), Is.EqualTo(2), "x and y are the only distinct members");
    }

    [Test]
    public async Task SetUnion_removing_one_source_drops_its_member()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.SetUnion);

        await applier.ApplyAsync(AggregationContribution.SetMember("g", "a", "x", Clock(1)));
        await applier.ApplyAsync(AggregationContribution.SetMember("g", "b", "y", Clock(2)));
        await applier.ApplyAsync(AggregationContribution.Retract("b", Clock(3)));

        Assert.That(Count(store, "g"), Is.EqualTo(1));
    }

    [Test]
    public async Task Empty_group_deletes_materialised_value()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.Retract("a", Clock(2)));

        Assert.That(store.Map.ContainsKey("g"), Is.False);
    }

    [Test]
    public async Task Fanout_greater_than_one_matches_single_accumulator_result()
    {
        var single = new InMemoryStore();
        var sharded = new InMemoryStore();
        var applierSingle = Applier(single, AggregationKind.Sum, fanout: 1);
        var applierSharded = Applier(sharded, AggregationKind.Sum, fanout: 8);

        foreach (var i in Enumerable.Range(0, 50))
        {
            var contribution = AggregationContribution.OfNumeric("g", $"key-{i}", i, Clock(i + 1));
            await applierSingle.ApplyAsync(contribution);
            await applierSharded.ApplyAsync(contribution);
        }

        Assert.That(Number(sharded, "g"), Is.EqualTo(Number(single, "g")));
    }

    [Test]
    public async Task Fanout_min_matches_single_accumulator_result()
    {
        var single = new InMemoryStore();
        var sharded = new InMemoryStore();
        var applierSingle = Applier(single, AggregationKind.Min, fanout: 1);
        var applierSharded = Applier(sharded, AggregationKind.Min, fanout: 4);

        foreach (var i in Enumerable.Range(0, 30))
        {
            var contribution = AggregationContribution.OfNumeric("g", $"key-{i}", 100 - i, Clock(i + 1));
            await applierSingle.ApplyAsync(contribution);
            await applierSharded.ApplyAsync(contribution);
        }

        Assert.That(Number(sharded, "g"), Is.EqualTo(Number(single, "g")));
    }

    [Test]
    public async Task Approximate_mode_bounds_min_to_top_k_and_stays_exact_below_k()
    {
        // With maxEntries=3 and a single shard, min stays exact while <=3 live
        // members; this exercises the bounded-top-K seam without forcing eviction
        // of the live minimum.
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Min, fanout: 1, maxEntries: 3);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 5, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "b", 2, Clock(2)));
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "c", 8, Clock(3)));
        // Fourth larger member must be evicted (min keeps smallest), so min stays 2.
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "d", 99, Clock(4)));

        Assert.That(Number(store, "g"), Is.EqualTo(2));
    }

    [Test]
    public async Task Retract_unknown_source_key_is_noop()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);

        await applier.ApplyAsync(AggregationContribution.Retract("ghost", Clock(1)));

        Assert.That(store.Map, Is.Empty);
    }
}
