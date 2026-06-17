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
        private readonly HashSet<string> _seenOps = new(StringComparer.Ordinal);

        public Dictionary<string, byte[]> Map { get; } = new(StringComparer.Ordinal);

        /// <summary>Every operation id seen by <see cref="SetManyAtomicAsync"/>, in call order.</summary>
        public List<string> AtomicOps { get; } = [];

        /// <summary>
        /// When set, emulates the idempotent saga: a re-submitted operation id
        /// re-attaches to the completed flip and applies nothing. When unset, the
        /// flip always writes (the more conservative test, proving the applier's
        /// recompute-from-current-state self-corrects without any dedup).
        /// </summary>
        public bool DedupAtomic { get; init; }

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

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            AtomicOps.Add(operationId);
            if (DedupAtomic && !_seenOps.Add(operationId))
            {
                // Saga re-attach: the flip already committed, so apply nothing.
                return Task.CompletedTask;
            }

            foreach (var (key, value) in entries)
            {
                Map[key] = value;
            }

            return Task.CompletedTask;
        }
    }

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static long? Count(InMemoryStore store, string group) =>
        store.Map.TryGetValue(group, out var v) ? LatticeAggregationValue.DecodeInt64(v) : null;

    private static double? Number(InMemoryStore store, string group) =>
        store.Map.TryGetValue(group, out var v) ? LatticeAggregationValue.DecodeDouble(v) : null;

    private static AggregationApplier Applier(IAggregationViewStore store, AggregationKind kind, int fanout = 1, int maxEntries = 0, string epoch = "0") =>
        new(store, kind, fanout, maxEntries, epoch);

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

    // --- crash mid-drain + full-batch WAL replay regression ---------------
    //
    // WAL delivery is at-least-once and the maintainer checkpoints once per drain
    // batch, so a silo crash mid-drain replays the WHOLE batch from the last
    // checkpoint. The original count/sum path incremented a serialized accumulator
    // and wrote the membership pointer separately, so a replay whose membership
    // write was lost mid-crash double-counted. These tests apply an identical
    // contribution sequence twice against one store (no intervening checkpoint
    // advance) and assert the materialised view equals the single-apply result,
    // covering fresh contribute, overwrite, re-group, and delete for every kind.

    private static IEnumerable<AggregationContribution> ReplayScenario(AggregationKind kind)
    {
        AggregationContribution Contribute(string group, string source, double numeric, string member, long ticks) => kind switch
        {
            AggregationKind.Count => AggregationContribution.Membership(group, source, Clock(ticks)),
            AggregationKind.SetUnion => AggregationContribution.SetMember(group, source, member, Clock(ticks)),
            _ => AggregationContribution.OfNumeric(group, source, numeric, Clock(ticks)),
        };

        return
        [
            Contribute("g1", "a", 10, "ma", 1),   // fresh contribute
            Contribute("g1", "b", 5, "mb", 2),     // fresh contribute
            Contribute("g1", "a", 3, "ma2", 3),    // overwrite (value / member change)
            Contribute("g2", "a", 4, "ma3", 4),    // re-group (group key change)
            AggregationContribution.Retract("b", Clock(5)), // delete
        ];
    }

    private static Dictionary<string, string> MaterialisedView(InMemoryStore store) =>
        store.Map
            .Where(kv => !kv.Key.StartsWith('\u0000'))
            .ToDictionary(kv => kv.Key, kv => Convert.ToBase64String(kv.Value), StringComparer.Ordinal);

    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    [TestCase(AggregationKind.Min)]
    [TestCase(AggregationKind.Max)]
    [TestCase(AggregationKind.SetUnion)]
    public async Task Replaying_identical_batch_recomputes_self_corrects(AggregationKind kind)
    {
        var scenario = ReplayScenario(kind).ToList();

        // Reference: a single, crash-free apply of the batch.
        var reference = new InMemoryStore();
        var referenceApplier = Applier(reference, kind);
        foreach (var contribution in scenario)
        {
            await referenceApplier.ApplyAsync(contribution);
        }

        // Crash-replay: the same batch applied twice with no checkpoint advance.
        // The atomic store here does NOT dedup, so correctness rests entirely on
        // the applier recomputing a net-zero delta from the advanced membership.
        var replayed = new InMemoryStore();
        var replayApplier = Applier(replayed, kind);
        foreach (var contribution in scenario)
        {
            await replayApplier.ApplyAsync(contribution);
        }

        foreach (var contribution in scenario)
        {
            await replayApplier.ApplyAsync(contribution);
        }

        Assert.That(MaterialisedView(replayed), Is.Not.Empty, "the scenario should leave a live group");
        Assert.That(MaterialisedView(replayed), Is.EqualTo(MaterialisedView(reference)));
    }

    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    [TestCase(AggregationKind.Min)]
    [TestCase(AggregationKind.Max)]
    [TestCase(AggregationKind.SetUnion)]
    public async Task Replaying_identical_batch_dedups_via_deterministic_operation_id(AggregationKind kind)
    {
        var scenario = ReplayScenario(kind).ToList();

        var reference = new InMemoryStore();
        var referenceApplier = Applier(reference, kind);
        foreach (var contribution in scenario)
        {
            await referenceApplier.ApplyAsync(contribution);
        }

        // This store emulates the idempotent saga: a re-submitted operation id
        // applies nothing. Because the operation id is a deterministic function of
        // the contribution identity, the replay re-attaches and self-corrects.
        var replayed = new InMemoryStore { DedupAtomic = true };
        var replayApplier = Applier(replayed, kind);
        foreach (var contribution in scenario)
        {
            await replayApplier.ApplyAsync(contribution);
        }

        foreach (var contribution in scenario)
        {
            await replayApplier.ApplyAsync(contribution);
        }

        Assert.That(MaterialisedView(replayed), Is.EqualTo(MaterialisedView(reference)));
    }

    [Test]
    public async Task Count_replayed_batch_does_not_double_count()
    {
        // A focused guard on the exact bug: 3 members counted twice must stay 3.
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Count);
        var batch = new[]
        {
            AggregationContribution.Membership("g", "a", Clock(1)),
            AggregationContribution.Membership("g", "b", Clock(2)),
            AggregationContribution.Membership("g", "c", Clock(3)),
        };

        foreach (var contribution in batch)
        {
            await applier.ApplyAsync(contribution);
        }

        foreach (var contribution in batch)
        {
            await applier.ApplyAsync(contribution);
        }

        Assert.That(Count(store, "g"), Is.EqualTo(3));
    }

    [Test]
    public async Task Sum_replayed_delete_does_not_double_retract()
    {
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);
        var batch = new[]
        {
            AggregationContribution.OfNumeric("g", "a", 10, Clock(1)),
            AggregationContribution.OfNumeric("g", "b", 5, Clock(2)),
            AggregationContribution.Retract("a", Clock(3)),
        };

        foreach (var contribution in batch)
        {
            await applier.ApplyAsync(contribution);
        }

        foreach (var contribution in batch)
        {
            await applier.ApplyAsync(contribution);
        }

        Assert.That(Number(store, "g"), Is.EqualTo(5));
    }

    [Test]
    public async Task Numeric_emptied_slot_is_cleaned_up_after_retraction()
    {
        // The atomic flip can only flip an emptied slot to the empty sentinel; the
        // applier then opportunistically deletes it so the sentinel never leaks.
        var store = new InMemoryStore();
        var applier = Applier(store, AggregationKind.Sum);

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        await applier.ApplyAsync(AggregationContribution.Retract("a", Clock(2)));

        Assert.That(store.Map, Is.Empty, "every internal row (accumulator + membership) should be removed");
    }

    [Test]
    public async Task Bumped_operation_epoch_reaccumulates_against_a_retained_saga_registry()
    {
        // A rebuild clears the view rows but the completed pre-rebuild sagas are
        // retained. Re-using the same epoch's operation ids would re-attach and
        // apply nothing; a bumped epoch mints fresh ids and re-accumulates.
        var store = new InMemoryStore { DedupAtomic = true };

        var gen0 = Applier(store, AggregationKind.Sum, epoch: "0");
        await gen0.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        Assert.That(Number(store, "g"), Is.EqualTo(10));

        // Simulate the rebuild: clear the view rows, keep the dedup (saga) registry.
        store.Map.Clear();

        var sameEpoch = Applier(store, AggregationKind.Sum, epoch: "0");
        await sameEpoch.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        Assert.That(Number(store, "g"), Is.Null, "the retained saga dedups the same epoch, so nothing re-accumulates");

        var nextEpoch = Applier(store, AggregationKind.Sum, epoch: "1");
        await nextEpoch.ApplyAsync(AggregationContribution.OfNumeric("g", "a", 10, Clock(1)));
        Assert.That(Number(store, "g"), Is.EqualTo(10), "a fresh epoch re-accumulates from scratch");
    }
}
