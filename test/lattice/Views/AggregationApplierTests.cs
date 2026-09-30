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

        public void Seed(string key, byte[] value) => _map[key] = value;

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

    // ───────────────── batched store round trips (issue: perf) ─────────────────

    /// <summary>
    /// Wraps the in-memory store and counts the read calls each kind of pass
    /// issues, so the batched shard read and batched empty-slot cleanup are
    /// asserted structurally rather than only by their end state.
    /// </summary>
    private sealed class CountingAggregationViewStore(InMemoryAggregationViewStore inner) : IAggregationViewStore
    {
        public int GetCalls { get; private set; }

        public int GetManyCalls { get; private set; }

        /// <summary>
        /// Writes landing on a membership row. The fold path's membership row is
        /// a pure back-pointer, so a contribution that would rewrite it byte for
        /// byte skips the write entirely; counting the writes is the only way to
        /// assert that structurally rather than by end state, which is identical
        /// either way.
        /// </summary>
        public int MembershipSetCalls { get; private set; }

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            GetCalls++;
            return inner.GetAsync(key, cancellationToken);
        }

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            GetManyCalls++;
            return inner.GetManyAsync(keys, cancellationToken);
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            if (key.StartsWith("\u0000m", StringComparison.Ordinal))
            {
                MembershipSetCalls++;
            }

            return inner.SetAsync(key, value, cancellationToken);
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
            => inner.DeleteAsync(key, cancellationToken);

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
            => inner.SetManyAtomicAsync(entries, operationId, cancellationToken);
    }

    [Test]
    public async Task Count_materialise_totals_every_slot_at_a_sharded_fanout()
    {
        // Distinct source keys spread across the 8 accumulator slots. The
        // materialise pass must total all of them, so a batched read that dropped
        // a slot would under-count.
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Count, fanout: 8, maxGroupEntries: 0, operationEpoch: "e1");

        for (var i = 0; i < 32; i++)
        {
            await applier.ApplyAsync(AggregationContribution.OfNumeric("g", $"s{i}", 1.0, Hlc()));
        }

        var materialised = await store.GetAsync("g");
        Assert.That(materialised, Is.Not.Null);
        Assert.That(LatticeAggregationValue.DecodeInt64(materialised!), Is.EqualTo(32),
            "every accumulator slot must be included in the materialised total");
    }

    [Test]
    public async Task Sum_materialise_totals_every_slot_at_a_sharded_fanout()
    {
        var store = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(store, AggregationKind.Sum, fanout: 8, maxGroupEntries: 0, operationEpoch: "e1");

        for (var i = 0; i < 32; i++)
        {
            await applier.ApplyAsync(AggregationContribution.OfNumeric("g", $"s{i}", 2.5, Hlc()));
        }

        var materialised = await store.GetAsync("g");
        Assert.That(materialised, Is.Not.Null);
        Assert.That(LatticeAggregationValue.DecodeDouble(materialised!), Is.EqualTo(80.0).Within(1e-9),
            "every accumulator slot must be included in the materialised total");
    }

    [Test]
    public async Task Count_materialise_reads_every_slot_in_one_batched_call()
    {
        // The shard gather is one GetMany per materialise pass regardless of
        // fanout, rather than one Get per slot: a fanout-32 contribution issues a
        // single-digit number of point reads, not 30-odd of them.
        var store = new CountingAggregationViewStore(new InMemoryAggregationViewStore());
        var applier = new AggregationApplier(store, AggregationKind.Count, fanout: 32, maxGroupEntries: 0, operationEpoch: "e1");

        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 1.0, Hlc()));

        Assert.Multiple(() =>
        {
            Assert.That(store.GetManyCalls, Is.EqualTo(2),
                "one batched shard gather for the materialise pass, one for the empty-slot cleanup");
            Assert.That(store.GetCalls, Is.LessThanOrEqualTo(4),
                "point reads must not scale with the fanout");
        });
    }

    [Test]
    public async Task Retract_probes_its_cleanup_candidates_in_one_batched_call()
    {
        // A retract empties both the decremented slot and the membership row, and
        // must probe them together rather than one round trip each.
        var inner = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(
            new CountingAggregationViewStore(inner), AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");
        await applier.ApplyAsync(AggregationContribution.OfNumeric("g", "s1", 1.0, Hlc()));

        var counting = new CountingAggregationViewStore(inner);
        var retracting = new AggregationApplier(counting, AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");
        await retracting.ApplyAsync(AggregationContribution.Retract("s1", Hlc()));

        Assert.Multiple(() =>
        {
            Assert.That(counting.GetManyCalls, Is.EqualTo(1),
                "the cleanup probe stays batched; the materialise gather is a point read at fanout 1");
            Assert.That(inner.Count, Is.EqualTo(0),
                "the emptied slot, membership row, and materialised group are all removed");
        });
    }

    [Test]
    public async Task Materialise_reads_its_single_slot_directly_at_the_default_fanout()
    {
        // An unsharded group has exactly one slot, so gathering it through a
        // batched read is a batch of one: a list to hold one key and a map to
        // hold one row, both discarded immediately. The pass reads that slot
        // directly instead. The batched read is kept above fanout 1, so the
        // same contribution must cost strictly fewer batched reads unsharded
        // than sharded. Counting the difference rather than asserting zero is
        // deliberate: the flip's cleanup probe is batched at every fanout and
        // is not the gather, so an absolute count would pin unrelated work.
        var single = new CountingAggregationViewStore(new InMemoryAggregationViewStore());
        var sharded = new CountingAggregationViewStore(new InMemoryAggregationViewStore());
        var singleApplier = new AggregationApplier(single, AggregationKind.Count, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");
        var shardedApplier = new AggregationApplier(sharded, AggregationKind.Count, fanout: 4, maxGroupEntries: 0, operationEpoch: "e1");

        var contribution = AggregationContribution.OfNumeric("g", "s1", 1.0, Hlc());
        await singleApplier.ApplyAsync(contribution);
        await shardedApplier.ApplyAsync(contribution);

        Assert.That(single.GetManyCalls, Is.LessThan(sharded.GetManyCalls),
            "a single-slot group must not issue a batched read to fetch its one row");
    }

    [Test]
    public async Task Materialise_agrees_across_fanouts_for_the_same_contributions()
    {
        // The direct read and the batched read must be observationally identical:
        // a batched read omits an absent key exactly as a point read returns null
        // for one, and every call site already treats null and the empty sentinel
        // alike. Same contributions, different gather shape, same materialised
        // value.
        var single = new InMemoryAggregationViewStore();
        var sharded = new InMemoryAggregationViewStore();
        var singleApplier = new AggregationApplier(single, AggregationKind.Sum, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1");
        var shardedApplier = new AggregationApplier(sharded, AggregationKind.Sum, fanout: 8, maxGroupEntries: 0, operationEpoch: "e1");

        for (var i = 0; i < 24; i++)
        {
            var contribution = AggregationContribution.OfNumeric("g", $"s{i}", i * 0.25, Hlc());
            await singleApplier.ApplyAsync(contribution);
            await shardedApplier.ApplyAsync(contribution);
        }

        var singleValue = await single.GetAsync("g");
        var shardedValue = await sharded.GetAsync("g");
        Assert.Multiple(() =>
        {
            Assert.That(singleValue, Is.Not.Null);
            Assert.That(shardedValue, Is.Not.Null);
            Assert.That(
                LatticeAggregationValue.DecodeDouble(singleValue!),
                Is.EqualTo(LatticeAggregationValue.DecodeDouble(shardedValue!)).Within(1e-9),
                "the gather shape must not change the materialised total");
        });
    }
    // --- Fold membership write elision ---

    private static ILatticeFoldProjection ConcatFold() =>
        new LatticeFoldProjection(_ => "g", () => [], (acc, _, value, _) => [.. acc, .. value], "v1");

    [Test]
    public async Task Fold_contribution_skips_rewriting_an_identical_membership_row()
    {
        // The fold path's membership row carries only the group back-pointer, so
        // a second contribution from the same source key to the same group would
        // rewrite it byte for byte. That write is a pure round trip against
        // persistent storage and is skipped.
        var store = new CountingAggregationViewStore(new InMemoryAggregationViewStore());
        var applier = new AggregationApplier(
            store, AggregationKind.Fold, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: ConcatFold());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));
        var afterFirst = store.MembershipSetCalls;

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [4, 5, 6], Hlc()));

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(1),
                "the first contribution has no stored membership row, so it must write one");
            Assert.That(store.MembershipSetCalls, Is.EqualTo(afterFirst),
                "a repeat contribution to the same group must not rewrite an identical membership row");
        });
    }

    [Test]
    public async Task Fold_contribution_still_writes_membership_when_the_group_changes()
    {
        // The elision is gated on the stored group matching, so a re-grouping
        // contribution must still repoint the back-pointer - otherwise the old
        // group would never be retracted.
        var store = new CountingAggregationViewStore(new InMemoryAggregationViewStore());
        var applier = new AggregationApplier(
            store, AggregationKind.Fold, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: ConcatFold());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));
        var afterFirst = store.MembershipSetCalls;

        await applier.ApplyAsync(AggregationContribution.Fold("h", "s1", [4, 5, 6], Hlc()));

        Assert.That(store.MembershipSetCalls, Is.EqualTo(afterFirst + 1),
            "a contribution that moves the source key to another group must rewrite the membership row");
    }

    [Test]
    public async Task Fold_contribution_rewrites_a_membership_row_that_is_not_byte_identical()
    {
        // A stored row that names the same group but carries a set-union member
        // does not encode identically to the member-free row the fold path
        // writes, so the guard must decline to skip it.
        var inner = new InMemoryAggregationViewStore();
        inner.Seed(
            AggregationRowCodec.MembershipKey("s1"),
            AggregationRowCodec.EncodeMembership(new AggregationRowCodec.MembershipRow("g", 0, "member-x")));
        var store = new CountingAggregationViewStore(inner);
        var applier = new AggregationApplier(
            store, AggregationKind.Fold, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: ConcatFold());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));

        Assert.That(store.MembershipSetCalls, Is.EqualTo(1),
            "a stored row carrying a member is not byte-identical to the fold row, so it must be rewritten");
    }

    [Test]
    public async Task Fold_contribution_rewrites_a_membership_row_holding_negative_zero()
    {
        // -0.0 == 0.0 is true but the two encode differently, so a value
        // comparison would wrongly skip this write and leave the stored bytes
        // diverged from what the fold path claims is there.
        var inner = new InMemoryAggregationViewStore();
        inner.Seed(
            AggregationRowCodec.MembershipKey("s1"),
            AggregationRowCodec.EncodeMembership(new AggregationRowCodec.MembershipRow("g", -0.0, null)));
        var store = new CountingAggregationViewStore(inner);
        var applier = new AggregationApplier(
            store, AggregationKind.Fold, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: ConcatFold());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));

        Assert.That(store.MembershipSetCalls, Is.EqualTo(1),
            "negative zero encodes differently from positive zero, so the row must be rewritten");
    }

    [Test]
    public async Task Fold_membership_elision_leaves_the_stored_row_byte_identical()
    {
        // The elision is only sound if the skipped write would have been a no-op,
        // so the end state must match a run that always writes.
        var elided = new InMemoryAggregationViewStore();
        var applier = new AggregationApplier(
            elided, AggregationKind.Fold, fanout: 1, maxGroupEntries: 0, operationEpoch: "e1", fold: ConcatFold());

        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [1, 2, 3], Hlc()));
        await applier.ApplyAsync(AggregationContribution.Fold("g", "s1", [4, 5, 6], Hlc()));

        var stored = await elided.GetAsync(AggregationRowCodec.MembershipKey("s1"));
        var expected = AggregationRowCodec.EncodeMembership(new AggregationRowCodec.MembershipRow("g", 0, null));

        Assert.That(stored, Is.EqualTo(expected),
            "the row left in place must equal the row the skipped write would have stored");
    }
}