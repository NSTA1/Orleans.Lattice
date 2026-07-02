using System.Text;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for the built-in <see cref="LatticeFoldProjection"/>: the lowering
/// of source mutations into <see cref="AggregationKind.Fold"/> contributions, the
/// filter / delete / range retraction paths, the raw-byte <c>Initial</c> /
/// <c>Apply</c> fold, the typed <see cref="LatticeFoldProjection.Create{TValue, TAccumulator}"/>
/// bridge, and the <see cref="ILatticeAggregationProjection.ProjectionVersion"/>
/// sensitivity to a fold-logic tag change.
/// </summary>
[TestFixture]
public class LatticeFoldProjectionTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static string Group(byte[] v) => Encoding.UTF8.GetString(v).Split('|')[0];

    private static LatticeMutation Set(string key, byte[] value, HybridLogicalClock ts) => new()
    {
        TreeId = "src",
        Kind = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = ts,
        Category = MutationCategory.User,
    };

    private static LatticeFoldProjection Concat() => new(
        Group,
        () => [],
        (acc, _, value, _) =>
        {
            var result = new byte[acc.Length + value.Length];
            acc.CopyTo(result, 0);
            value.CopyTo(result, acc.Length);
            return result;
        },
        "concat-v1");

    [Test]
    public void Aggregation_is_fold()
    {
        Assert.That(Concat().Aggregation, Is.EqualTo(AggregationKind.Fold));
    }

    [Test]
    public void Project_set_emits_fold_contribution_carrying_value()
    {
        var projection = Concat();
        var value = Encoding.UTF8.GetBytes("g|payload");

        var contributions = projection.Project(Set("k", value, Clock(5))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Contribute));
        Assert.That(contributions[0].GroupKey, Is.EqualTo("g"));
        Assert.That(contributions[0].SourceKey, Is.EqualTo("k"));
        Assert.That(contributions[0].Value, Is.EqualTo(value));
        Assert.That(contributions[0].Timestamp, Is.EqualTo(Clock(5)));
    }

    [Test]
    public void Project_delete_emits_retract()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Delete,
            Key = "k",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var contributions = Concat().Project(mutation).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Retract));
        Assert.That(contributions[0].SourceKey, Is.EqualTo("k"));
    }

    [Test]
    public void Project_set_failing_filter_emits_retract()
    {
        var filter = LatticePredicateTranslator.Translate<Predicates.PredicatePerson>(p => p.Age >= 18);
        var projection = new LatticeFoldProjection(
            _ => "g",
            () => [],
            (acc, _, _, _) => acc,
            "fold-v1",
            filter);

        var minor = JsonLatticeSerializer<Predicates.PredicatePerson>.Default.Serialize(
            new Predicates.PredicatePerson("Bob", 12, true, 0.5, null, null));
        var contributions = projection.Project(Set("k", minor, Clock(3))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Retract));
    }

    [Test]
    public void Project_unconstrained_range_delete_emits_range_reconcile()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "m",
            Timestamp = Clock(7),
            Category = MutationCategory.User,
        };

        var contributions = Concat().Project(mutation).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.RangeReconcile));
        Assert.That(contributions[0].GroupKey, Is.EqualTo("a"));
        Assert.That(contributions[0].EndKey, Is.EqualTo("m"));
    }

    [Test]
    public void Project_matched_range_delete_emits_per_key_retractions()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "m",
            MatchedKeys = ["a", "b"],
            Timestamp = Clock(7),
            Category = MutationCategory.User,
        };

        var contributions = Concat().Project(mutation).ToList();

        Assert.That(contributions.Select(c => c.SourceKey), Is.EquivalentTo(new[] { "a", "b" }));
        Assert.That(contributions, Has.All.Matches<AggregationContribution>(c => c.Kind == AggregationContributionKind.Retract));
    }

    [Test]
    public void Initial_and_apply_fold_raw_bytes()
    {
        var projection = Concat();
        var acc = projection.Initial();
        acc = projection.Apply(acc, "a", Encoding.UTF8.GetBytes("X"), Clock(1));
        acc = projection.Apply(acc, "b", Encoding.UTF8.GetBytes("Y"), Clock(2));

        Assert.That(Encoding.UTF8.GetString(acc), Is.EqualTo("XY"));
    }

    [Test]
    public void ProjectionVersion_changes_with_fold_tag()
    {
        var v1 = Concat().ProjectionVersion;
        var v2 = new LatticeFoldProjection(Group, () => [], (acc, _, _, _) => acc, "concat-v2").ProjectionVersion;

        Assert.That(v1, Is.Not.EqualTo(v2));
    }

    [Test]
    public void Typed_create_bridges_domain_types_to_byte_accumulator()
    {
        // Sum the "n" of each event per group, but as a custom fold rather than the
        // built-in Sum reducer, to exercise the typed bridge end to end.
        var projection = LatticeFoldProjection.Create<Event, long>(
            groupKeySelector: e => e.Group,
            initial: () => 0L,
            apply: (acc, _, e, _) => acc + e.N,
            foldVersion: "sum-fold-v1");

        var value = JsonLatticeSerializer<Event>.Default.Serialize(new Event("g", 4));
        var contributions = projection.Project(Set("k", value, Clock(1))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].GroupKey, Is.EqualTo("g"));

        var acc = projection.Apply(projection.Initial(), "k", value, Clock(1));
        Assert.That(JsonLatticeSerializer<long>.Default.Deserialize(acc), Is.EqualTo(4));
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        Assert.That(() => new LatticeFoldProjection(null!, () => [], (a, _, _, _) => a, "v"), Throws.ArgumentNullException);
        Assert.That(() => new LatticeFoldProjection(Group, null!, (a, _, _, _) => a, "v"), Throws.ArgumentNullException);
        Assert.That(() => new LatticeFoldProjection(Group, () => [], null!, "v"), Throws.ArgumentNullException);
        Assert.That(() => new LatticeFoldProjection(Group, () => [], (a, _, _, _) => a, ""), Throws.ArgumentException);
    }

    private sealed record Event(string Group, long N);
}
