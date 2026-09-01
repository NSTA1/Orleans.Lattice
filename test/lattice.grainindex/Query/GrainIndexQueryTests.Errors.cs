namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The error paths: a predicate the index cannot answer must fail loudly when it
/// is planned, never as a quietly wrong result set.
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public void Where_rejects_a_null_predicate()
    {
        var index = Populated();

        Assert.Throws<ArgumentNullException>(() => index.Index.Where(null!));
    }

    [Test]
    public void Where_rejects_a_predicate_over_an_unprojected_property()
    {
        var index = Populated();

        var exception = Assert.Throws<GrainIndexPropertyNotIndexedException>(
            () => index.Index.Where(s => s.Secret == "classified"));

        Assert.Multiple(() =>
        {
            Assert.That(exception!.IndexName, Is.EqualTo("Subjects"));
            Assert.That(exception.PropertyName, Is.EqualTo("Secret"));
            Assert.That(exception.IndexedProperties, Is.EqualTo(new[] { "Age", "Country", "LastSeen", "Status" }));
            Assert.That(exception.Message, Does.Contain("Secret").And.Contain("Age, Country, LastSeen, Status"));
        });
    }

    [Test]
    public void Where_rejects_an_unsupported_construct_with_the_core_dialect_error()
    {
        var index = Populated();

        var exception = Assert.Throws<NotSupportedException>(
            () => index.Index.Where(s => s.Country.ToUpperInvariant() == "GB"));

        Assert.That(exception!.Message, Does.Contain("Unsupported predicate construct"));
    }

    [Test]
    public void Where_rejects_nested_member_access()
    {
        var index = Populated();

        var exception = Assert.Throws<NotSupportedException>(
            () => index.Index.Where(s => s.Country.Length > 2));

        Assert.That(exception!.Message, Does.Contain("nested member access").And.Contain("Country.Length"));
    }

    [Test]
    public void Where_rejects_a_clause_spanning_two_projected_properties()
    {
        var index = QueryTestIndex.Create(
            new GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState>(
                "Subjects",
                StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
                [
                    IndexedTestIndex.Property<string>("Country", static s => s.Country),
                    IndexedTestIndex.Property<string>("Secret", static s => s.Secret),
                ]));

        var exception = Assert.Throws<NotSupportedException>(
            () => index.Index.Where(s => s.Country == s.Secret));

        Assert.That(exception!.Message, Does.Contain("more than one projected property"));
    }

    [Test]
    public void Where_rejects_a_comparison_between_a_property_and_itself()
    {
        var index = Populated();

        var exception = Assert.Throws<NotSupportedException>(
            () => index.Index.Where(s => s.Age == s.Age));

        Assert.That(exception!.Message, Does.Contain("between two state members"));
    }

    [Test]
    public void Where_rejects_a_predicate_that_expands_past_the_disjunction_ceiling()
    {
        var index = Populated();

        var exception = Assert.Throws<NotSupportedException>(
            () => index.Index.Where(s =>
                (s.Age == 1 || s.Age == 2)
                && (s.Age == 3 || s.Age == 4)
                && (s.Age == 5 || s.Age == 6)
                && (s.Age == 7 || s.Age == 8)
                && (s.Age == 9 || s.Age == 10)
                && (s.Age == 11 || s.Age == 12)
                && (s.Age == 13 || s.Age == 14)));

        Assert.That(exception!.Message, Does.Contain("disjunctions"));
    }

    [Test]
    public async Task Where_routes_a_string_method_whose_target_is_the_constant()
    {
        var index = Populated();

        // The member is the argument rather than the target, so no key range can
        // be derived and the whole property range is scanned with the predicate
        // pushed down.
        var keys = await KeysAsync(index.Index.Where(s => "GBR".StartsWith(s.Country)));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public void Where_rejects_an_index_that_projects_nothing()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => QueryTestIndex.Create(IndexedTestIndex.Empty()));

        Assert.That(exception!.Message, Does.Contain("projects no properties"));
    }
}
