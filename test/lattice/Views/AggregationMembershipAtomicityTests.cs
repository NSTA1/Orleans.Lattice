using Orleans.Lattice.Views;
using static Orleans.Lattice.Views.AggregationRowCodec;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Observes numeric membership and accumulator consistency at every store boundary.</summary>
[TestFixture]
public sealed class AggregationMembershipAtomicityTests
{
    private sealed class ObservedStore(int fanout, bool deduplicate) : IAggregationViewStore
    {
        private readonly Dictionary<string, byte[]> _rows = new(StringComparer.Ordinal);
        private readonly HashSet<string> _operations = new(StringComparer.Ordinal);

        public int Observations { get; private set; }

        public bool LoseNextAtomicResponse { get; set; }

        public bool LoseNextGroupResponse { get; set; }

        public bool FailNextIntentRetirement { get; set; }

        public bool LoseNextIntentRetirementResponse { get; set; }

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
        {
            Observe();
            return Task.FromResult(_rows.GetValueOrDefault(key));
        }

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default)
        {
            Observe();
            return Task.FromResult(keys.Where(_rows.ContainsKey).ToDictionary(key => key, key => _rows[key]));
        }

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default)
        {
            _rows[key] = value;
            Observe();
            LoseGroupResponse(key);
            return Task.CompletedTask;
        }

        public Task DeleteAsync(string key, CancellationToken cancellationToken = default)
        {
            if (FailNextIntentRetirement && key.StartsWith("\u0000p", StringComparison.Ordinal))
            {
                FailNextIntentRetirement = false;
                throw new InvalidOperationException("intent retirement failed");
            }

            _rows.Remove(key);
            Observe();
            LoseGroupResponse(key);
            if (LoseNextIntentRetirementResponse && key.StartsWith("\u0000p", StringComparison.Ordinal))
            {
                LoseNextIntentRetirementResponse = false;
                throw new InvalidOperationException("intent deletion committed but response was lost");
            }

            return Task.CompletedTask;
        }

        private void LoseGroupResponse(string key)
        {
            if (LoseNextGroupResponse && key[0] != '\0')
            {
                LoseNextGroupResponse = false;
                throw new InvalidOperationException("group materialisation response was lost");
            }
        }

        public Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default)
        {
            Observe();
            if (!deduplicate || _operations.Add(operationId))
            {
                foreach (var (key, value) in entries)
                {
                    _rows[key] = value;
                }
            }

            Observe();
            if (LoseNextAtomicResponse)
            {
                LoseNextAtomicResponse = false;
                throw new InvalidOperationException("atomic flip committed but response was lost");
            }

            return Task.CompletedTask;
        }

        private void Observe()
        {
            Observations++;
            foreach (var group in new[] { "red", "blue" })
            {
                for (var slot = 0; slot < fanout; slot++)
                {
                    var expected = new AccumulatorRow(0, 0);
                    foreach (var key in new[] { "a", "b" })
                    {
                        if (Slot(key, fanout) != slot
                            || !_rows.TryGetValue(MembershipKey(key), out var bytes)
                            || IsEmpty(bytes))
                        {
                            continue;
                        }

                        var membership = DecodeMembership(bytes);
                        if (membership.GroupKey == group)
                        {
                            expected = new AccumulatorRow(expected.Count + 1, expected.Sum + membership.Numeric);
                        }
                    }

                    var actual = _rows.TryGetValue(AccumulatorKey(group, slot), out var row) && !IsEmpty(row)
                        ? DecodeAccumulator(row)
                        : new AccumulatorRow(0, 0);
                    Assert.That(actual, Is.EqualTo(expected),
                        $"membership and accumulator must agree at observation {Observations}, {group}/{slot}");
                    Assert.That(actual.Count, Is.GreaterThanOrEqualTo(0));
                }
            }
        }
    }

    [TestCase(AggregationKind.Count, 1, false)]
    [TestCase(AggregationKind.Count, 8, true)]
    [TestCase(AggregationKind.Sum, 1, true)]
    [TestCase(AggregationKind.Sum, 8, false)]
    [TestCase(AggregationKind.Count, 1, true)]
    [TestCase(AggregationKind.Count, 8, false)]
    [TestCase(AggregationKind.Sum, 1, false)]
    [TestCase(AggregationKind.Sum, 8, true)]
    public async Task ApplyAsync_regroup_retract_and_lost_response_keep_membership_atomic(
        AggregationKind kind, int fanout, bool deduplicate)
    {
        var store = new ObservedStore(fanout, deduplicate);
        AggregationApplier Create() => new(store, kind, fanout, 0, "epoch");
        static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };
        var initial = AggregationContribution.OfNumeric("red", "a", 3, Clock(1));
        var peer = AggregationContribution.OfNumeric("red", "b", 5, Clock(2));
        var regroup = AggregationContribution.OfNumeric("blue", "a", 7, Clock(3));
        var retract = AggregationContribution.Retract("a", Clock(4));

        await Create().ApplyAsync(initial);
        await Create().ApplyAsync(peer);
        foreach (var contribution in new[] { regroup, retract })
        {
            store.LoseNextAtomicResponse = true;
            Assert.That(
                () => Create().ApplyAsync(contribution),
                Throws.TypeOf<InvalidOperationException>().With.Message.Contains("response was lost"));
            await Create().ApplyAsync(contribution);
            await Create().ApplyAsync(contribution);
            Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Null,
                "an obligation retires only after every touched public group has been repaired");
            var remaining = await store.GetAsync("red");
            Assert.That(remaining, Is.Not.Null);
            Assert.That(
                kind == AggregationKind.Count
                    ? LatticeAggregationValue.DecodeInt64(remaining!)
                    : LatticeAggregationValue.DecodeDouble(remaining!),
                Is.EqualTo(kind == AggregationKind.Count ? 1 : 5),
                "replay must repair the old group even when membership now points at another group or is empty");
        }

        Assert.That(await store.GetAsync("blue"), Is.Null);
        var red = await store.GetAsync("red");
        Assert.That(red, Is.Not.Null);
        Assert.That(
            kind == AggregationKind.Count
                ? LatticeAggregationValue.DecodeInt64(red!)
                : LatticeAggregationValue.DecodeDouble(red!),
            Is.EqualTo(kind == AggregationKind.Count ? 1 : 5));
        Assert.That(store.Observations, Is.GreaterThan(20),
            "the detector must have observed intermediate store boundaries, not just final materialisation");
    }

    [TestCase("group")]
    [TestCase("retire-before")]
    [TestCase("retire-after")]
    public async Task ApplyAsync_failed_group_write_or_intent_retirement_replays_before_the_next_contribution(string failure)
    {
        var store = new ObservedStore(1, true);
        AggregationApplier Create() => new(store, AggregationKind.Sum, 1, 0, "epoch");
        static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };
        await Create().ApplyAsync(AggregationContribution.OfNumeric("red", "a", 3, Clock(1)));
        await Create().ApplyAsync(AggregationContribution.OfNumeric("red", "b", 5, Clock(2)));

        store.LoseNextGroupResponse = failure == "group";
        store.FailNextIntentRetirement = failure == "retire-before";
        store.LoseNextIntentRetirementResponse = failure == "retire-after";
        Assert.That(
            () => Create().ApplyAsync(AggregationContribution.OfNumeric("blue", "a", 7, Clock(3))),
            Throws.TypeOf<InvalidOperationException>());
        Assert.That(
            await store.GetAsync(NumericMaterialisationKey("a")),
            failure == "retire-after" ? Is.Null : Is.Not.Null,
            "only a committed retirement may erase an obligation; losing its response is safe");

        await Create().ApplyAsync(AggregationContribution.Retract("a", Clock(4)));
        Assert.That(await store.GetAsync("blue"), Is.Null);
        Assert.That(LatticeAggregationValue.DecodeDouble((await store.GetAsync("red"))!), Is.EqualTo(5));
        Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Null);
    }

    [Test]
    public async Task ApplyAsync_absent_member_retry_completes_a_failed_retraction_materialisation()
    {
        var store = new ObservedStore(1, true);
        AggregationApplier Create() => new(store, AggregationKind.Sum, 1, 0, "epoch");
        await Create().ApplyAsync(AggregationContribution.OfNumeric("red", "a", 3, new() { WallClockTicks = 1 }));
        var retract = AggregationContribution.Retract("a", new() { WallClockTicks = 2 });
        store.LoseNextGroupResponse = true;
        Assert.That(() => Create().ApplyAsync(retract), Throws.TypeOf<InvalidOperationException>());
        Assert.That(IsEmpty((await store.GetAsync(MembershipKey("a")))!), Is.True);
        Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Not.Null);

        await Create().ApplyAsync(retract);
        Assert.That(await store.GetAsync("red"), Is.Null);
        Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Null);
    }

    [TestCase("")]
    [TestCase("\0reserved")]
    public async Task ApplyAsync_rejected_numeric_regroup_completes_the_previous_durable_intent_then_retracts_the_member(string reserved)
    {
        var store = new ObservedStore(1, true);
        AggregationApplier Create() => new(store, AggregationKind.Sum, 1, 0, "epoch");
        await Create().ApplyAsync(AggregationContribution.OfNumeric("red", "a", 3, new() { WallClockTicks = 1 }));
        await Create().ApplyAsync(AggregationContribution.OfNumeric("red", "b", 5, new() { WallClockTicks = 2 }));
        store.LoseNextAtomicResponse = true;
        Assert.That(
            () => Create().ApplyAsync(AggregationContribution.OfNumeric("blue", "a", 7, new() { WallClockTicks = 3 })),
            Throws.TypeOf<InvalidOperationException>());

        // The reserved regroup first completes the pending blue intent (so red no
        // longer counts "a"), then retracts "a" from blue as a filter exit would
        // (#5236): "a" now belongs to no valid group.
        await Create().ApplyAsync(AggregationContribution.OfNumeric(reserved, "a", 9, new() { WallClockTicks = 4 }));
        Assert.That(LatticeAggregationValue.DecodeDouble((await store.GetAsync("red"))!), Is.EqualTo(5));
        Assert.That(await store.GetAsync("blue"), Is.Null);
        Assert.That(await store.GetAsync(MembershipKey("a")), Is.Null);
        Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Null);
    }

    [TestCase("red", null)]
    [TestCase("red", "blue")]
    [TestCase("r\0ed", "b\0lue")]
    public void NumericMaterialisation_codec_roundtrips_both_group_identities(string group, string? other)
    {
        var encoded = EncodeNumericMaterialisation(group, other);
        Assert.That(DecodeNumericMaterialisation(encoded), Is.EqualTo((group, other)));
        Assert.That(
            () => DecodeNumericMaterialisation(encoded[..^1]),
            Throws.TypeOf<InvalidDataException>());
    }

    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    public async Task CompletePendingNumericSourceAsync_repairs_a_source_even_when_projection_emits_nothing(AggregationKind kind)
    {
        var store = new ObservedStore(1, true);
        AggregationApplier Create() => new(store, kind, 1, 0, "epoch");
        await Create().ApplyAsync(AggregationContribution.OfNumeric("red", "a", 3, new() { WallClockTicks = 1 }));
        store.LoseNextAtomicResponse = true;
        Assert.That(
            () => Create().ApplyAsync(AggregationContribution.OfNumeric("blue", "a", 7, new() { WallClockTicks = 2 })),
            Throws.TypeOf<InvalidOperationException>());
        Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Not.Null);
        await Create().CompletePendingNumericSourceAsync("a");

        Assert.That(await store.GetAsync("red"), Is.Null);
        var blue = (await store.GetAsync("blue"))!;
        Assert.That(
            kind == AggregationKind.Count
                ? LatticeAggregationValue.DecodeInt64(blue)
                : LatticeAggregationValue.DecodeDouble(blue),
            Is.EqualTo(kind == AggregationKind.Count ? 1 : 7));
        Assert.That(await store.GetAsync(NumericMaterialisationKey("a")), Is.Null);
        await Create().CompletePendingNumericSourceAsync("a");
    }
}
