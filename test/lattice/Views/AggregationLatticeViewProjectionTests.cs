using System.Text;

namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for the built-in <see cref="AggregationLatticeViewProjection"/>.</summary>
[TestFixture]
public class AggregationLatticeViewProjectionTests
{
    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    // Test value layout: "group|numeric|member" UTF8.
    private static byte[] Value(string group, double numeric, string member) =>
        Encoding.UTF8.GetBytes($"{group}|{numeric.ToString(System.Globalization.CultureInfo.InvariantCulture)}|{member}");

    private static string Group(byte[] v) => Encoding.UTF8.GetString(v).Split('|')[0];

    private static double Numeric(byte[] v) =>
        double.Parse(Encoding.UTF8.GetString(v).Split('|')[1], System.Globalization.CultureInfo.InvariantCulture);

    private static string Member(byte[] v) => Encoding.UTF8.GetString(v).Split('|')[2];

    private static LatticeMutation Set(string key, byte[] value, HybridLogicalClock ts) => new()
    {
        TreeId = "src",
        Kind = MutationKind.Set,
        Key = key,
        Value = value,
        Timestamp = ts,
        Category = MutationCategory.User,
    };

    [Test]
    public void Project_count_set_emits_membership_contribution()
    {
        var projection = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v1");

        var contributions = projection.Project(Set("k", Value("g", 0, ""), Clock(5))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Contribute));
        Assert.That(contributions[0].GroupKey, Is.EqualTo("g"));
        Assert.That(contributions[0].SourceKey, Is.EqualTo("k"));
    }

    [Test]
    public void Project_sum_set_emits_numeric_contribution()
    {
        var projection = new AggregationLatticeViewProjection(
            AggregationKind.Sum, Group, "v1", valueSelector: Numeric);

        var contributions = projection.Project(Set("k", Value("g", 12.5, ""), Clock(5))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].GroupKey, Is.EqualTo("g"));
        Assert.That(contributions[0].Numeric, Is.EqualTo(12.5));
    }

    [Test]
    public void Project_set_union_set_emits_member_contribution()
    {
        var projection = new AggregationLatticeViewProjection(
            AggregationKind.SetUnion, Group, "v1", memberSelector: Member);

        var contributions = projection.Project(Set("k", Value("g", 0, "tag-a"), Clock(5))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Member, Is.EqualTo("tag-a"));
    }

    [Test]
    public void Project_delete_emits_retract()
    {
        var projection = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v1");
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.Delete,
            Key = "k",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var contributions = projection.Project(mutation).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Retract));
        Assert.That(contributions[0].SourceKey, Is.EqualTo("k"));
    }

    [Test]
    public void Project_set_failing_filter_emits_retract()
    {
        var filter = LatticePredicateTranslator.Translate<Predicates.PredicatePerson>(p => p.Age >= 18);
        var projection = new AggregationLatticeViewProjection(
            AggregationKind.Count,
            v => "g",
            "v1",
            filter: filter);

        var minor = JsonLatticeSerializer<Predicates.PredicatePerson>.Default.Serialize(
            new Predicates.PredicatePerson("Bob", 12, true, 0.5, null, null));

        var contributions = projection.Project(Set("k", minor, Clock(5))).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.Retract));
    }

    [Test]
    public void Project_delete_range_with_matched_keys_emits_per_key_retracts()
    {
        var projection = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v1");
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            MatchedKeys = ["a", "b", "c"],
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var contributions = projection.Project(mutation).ToList();

        Assert.That(contributions.Select(c => c.SourceKey), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(contributions, Has.All.Matches<AggregationContribution>(c => c.Kind == AggregationContributionKind.Retract));
    }

    [Test]
    public void Project_delete_range_without_matched_keys_emits_range_reconcile()
    {
        var projection = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v1");
        var mutation = new LatticeMutation
        {
            TreeId = "src",
            Kind = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            Timestamp = Clock(9),
            Category = MutationCategory.User,
        };

        var contributions = projection.Project(mutation).ToList();

        Assert.That(contributions, Has.Count.EqualTo(1));
        Assert.That(contributions[0].Kind, Is.EqualTo(AggregationContributionKind.RangeReconcile));
        Assert.That(contributions[0].GroupKey, Is.EqualTo("a"));
        Assert.That(contributions[0].EndKey, Is.EqualTo("z"));
    }

    [Test]
    public void Constructor_sum_without_value_selector_throws()
    {
        Assert.That(
            () => new AggregationLatticeViewProjection(AggregationKind.Sum, Group, "v1"),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_set_union_without_member_selector_throws()
    {
        Assert.That(
            () => new AggregationLatticeViewProjection(AggregationKind.SetUnion, Group, "v1"),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_null_group_selector_throws()
    {
        Assert.That(
            () => new AggregationLatticeViewProjection(AggregationKind.Count, null!, "v1"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ProjectionVersion_changes_when_kind_changes()
    {
        var count = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v1");
        var sum = new AggregationLatticeViewProjection(AggregationKind.Sum, Group, "v1", valueSelector: Numeric);

        Assert.That(count.ProjectionVersion, Is.Not.EqualTo(sum.ProjectionVersion));
    }

    [Test]
    public void ProjectionVersion_changes_when_selector_version_changes()
    {
        var a = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v1");
        var b = new AggregationLatticeViewProjection(AggregationKind.Count, Group, "v2");

        Assert.That(a.ProjectionVersion, Is.Not.EqualTo(b.ProjectionVersion));
    }
}
