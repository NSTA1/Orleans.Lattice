namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The single most important interop constraint in this package: an entry
/// payload must satisfy a predicate that was compiled from a lambda written
/// over the grain's own state type. These tests run the real translator and the
/// real server-side evaluator - the same
/// <see cref="LatticePredicateEvaluation.Matches(byte[], in LatticePredicateNode)"/>
/// call the leaf scan makes - against a real projected entry, so a change to
/// either side that broke the field-name contract would fail here rather than
/// silently returning no rows in production.
/// </summary>
[TestFixture]
public class GrainIndexPredicateInteropTests
{
    private static IndexedTestState State(int age = 17, string country = "GB") => new()
    {
        Age = age,
        Country = country,
        LastSeen = new DateTimeOffset(2026, 8, 31, 9, 45, 57, TimeSpan.Zero),
        IsActive = true,
        Score = 12.5,
        Status = TestStatus.Active,
        Tenant = Guid.Parse("11111111-2222-3333-4444-555555555555"),
    };

    private static byte[] Payload<TProperty>(
        string property,
        Func<IndexedTestState, TProperty> accessor,
        IndexedTestState state) =>
        IndexedTestIndex
            .Projector(IndexedTestIndex.SingleProperty(property, accessor))
            .Project("alice", state)
            .Entries[0]
            .Value;

    private static bool Matches(byte[] payload, System.Linq.Expressions.Expression<Func<IndexedTestState, bool>> predicate) =>
        LatticePredicateEvaluation.Matches(payload, LatticePredicateTranslator.Translate(predicate));

    [Test]
    public void An_integer_entry_satisfies_the_range_predicate_it_should()
    {
        var below = Payload("Age", static s => s.Age, State(age: 17));
        var above = Payload("Age", static s => s.Age, State(age: 18));

        Assert.That(Matches(below, s => s.Age >= 18), Is.False);
        Assert.That(Matches(above, s => s.Age >= 18), Is.True);
        Assert.That(Matches(above, s => s.Age > 18), Is.False);
        Assert.That(Matches(above, s => s.Age == 18), Is.True);
        Assert.That(Matches(above, s => s.Age != 18), Is.False);
        Assert.That(Matches(below, s => s.Age < 18), Is.True);
        Assert.That(Matches(below, s => s.Age <= 17), Is.True);
    }

    [Test]
    public void A_string_entry_satisfies_equality_and_the_string_methods()
    {
        var payload = Payload("Country", static s => s.Country, State(country: "GB"));

        Assert.That(Matches(payload, s => s.Country == "GB"), Is.True);
        Assert.That(Matches(payload, s => s.Country == "FR"), Is.False);
        Assert.That(Matches(payload, s => s.Country.StartsWith("G")), Is.True);
        Assert.That(Matches(payload, s => s.Country.EndsWith("B")), Is.True);
        Assert.That(Matches(payload, s => s.Country.Contains("R")), Is.False);
    }

    [Test]
    public void A_boolean_entry_satisfies_a_boolean_comparison()
    {
        var payload = Payload("IsActive", static s => s.IsActive, State());

        Assert.That(Matches(payload, s => s.IsActive == true), Is.True);
        Assert.That(Matches(payload, s => s.IsActive == false), Is.False);
    }

    [Test]
    public void A_floating_point_entry_satisfies_a_numeric_comparison()
    {
        var payload = Payload("Score", static s => s.Score, State());

        Assert.That(Matches(payload, s => s.Score >= 12.5), Is.True);
        Assert.That(Matches(payload, s => s.Score > 12.5), Is.False);
    }

    [Test]
    public void An_enum_entry_satisfies_an_equality_predicate_through_the_payload_scan()
    {
        var payload = Payload("Status", static s => s.Status, State());

        Assert.That(Matches(payload, s => s.Status == TestStatus.Active), Is.True);
        Assert.That(Matches(payload, s => s.Status == TestStatus.Retired), Is.False);
    }

    [Test]
    public void A_guid_entry_satisfies_an_equality_predicate_through_the_payload_scan()
    {
        var state = State();
        var payload = Payload("Tenant", static s => s.Tenant, state);
        var tenant = state.Tenant;
        var other = Guid.Parse("99999999-9999-9999-9999-999999999999");

        Assert.That(Matches(payload, s => s.Tenant == tenant), Is.True);
        Assert.That(Matches(payload, s => s.Tenant == other), Is.False);
    }

    [Test]
    public void A_null_property_value_satisfies_only_a_null_comparison()
    {
        var state = State();
        state.LastSeen = null;
        var payload = Payload("LastSeen", static s => s.LastSeen, state);

        Assert.That(Matches(payload, s => s.LastSeen == null), Is.True);
        Assert.That(Matches(payload, s => s.LastSeen != null), Is.False);
    }

    [Test]
    public void A_conjunction_over_the_indexed_property_and_a_constant_still_folds()
    {
        var payload = Payload("Age", static s => s.Age, State(age: 21));

        Assert.That(Matches(payload, s => s.Age >= 18 && s.Age < 65), Is.True);
        Assert.That(Matches(payload, s => s.Age >= 18 && s.Age < 21), Is.False);
        Assert.That(Matches(payload, s => s.Age < 18 || s.Age > 20), Is.True);
        Assert.That(Matches(payload, s => !(s.Age >= 18)), Is.False);
    }

    [Test]
    public void A_predicate_over_a_property_this_entry_does_not_carry_never_matches()
    {
        // Each entry carries exactly one property, so a predicate naming another
        // one is strictly subtractive rather than accidentally true. The query
        // side must therefore drive from a property the index actually carries.
        var payload = Payload("Age", static s => s.Age, State(country: "GB"));

        Assert.That(Matches(payload, s => s.Country == "GB"), Is.False);
        Assert.That(Matches(payload, s => s.Age >= 0 && s.Country == "GB"), Is.False);
    }

    [Test]
    public void The_metadata_fields_cannot_be_reached_by_a_state_member_path()
    {
        // '$grain' and '$property' are not valid C# identifiers, so no lambda
        // over the state type can name them - which is exactly why they were
        // chosen. Their presence must not perturb an ordinary predicate.
        var payload = Payload("Age", static s => s.Age, State(age: 18));

        Assert.That(Matches(payload, s => s.Age == 18), Is.True);
        Assert.That(System.Text.Encoding.UTF8.GetString(payload), Does.Contain(GrainIndexEntryValue.GrainKeyField));
    }

    [Test]
    public void An_empty_or_malformed_payload_never_matches()
    {
        var predicate = LatticePredicateTranslator.Translate<IndexedTestState>(s => s.Age >= 0);

        Assert.That(LatticePredicateEvaluation.Matches(null, predicate), Is.False);
        Assert.That(LatticePredicateEvaluation.Matches([], predicate), Is.False);
        Assert.That(LatticePredicateEvaluation.Matches("not json"u8.ToArray(), predicate), Is.False);
    }
}
