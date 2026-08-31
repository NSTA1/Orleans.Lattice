namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Predicate coverage: the shapes the planner routes differently - string
/// methods, conjunction across properties, disjunction, negation, null, the
/// payload-scan fallback for a type with no order-preserving key, and the
/// floating-point cases where the key order and C# disagree.
/// </summary>
public sealed partial class GrainIndexQueryTests
{
    [Test]
    public async Task Where_starts_with_narrows_to_the_prefix_range()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country.StartsWith("G")));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_starts_with_an_empty_prefix_matches_every_present_value()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country.StartsWith(string.Empty)));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_ends_with_falls_back_to_the_payload_scan()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country.EndsWith("E")));

        Assert.That(keys, Is.EquivalentTo(new[] { "dave" }));
    }

    [Test]
    public async Task Where_contains_falls_back_to_the_payload_scan()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country.Contains("R")));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob" }));
    }

    [Test]
    public async Task Where_string_equals_matches_the_exact_value()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Country.Equals("GB")));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_conjunction_across_properties_intersects_the_two_scans()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Where_conjunction_across_three_properties_intersects_all_of_them()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(
            s => s.Age >= 18 && s.Country == "GB" && s.Status == TestStatus.Active));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Where_conjunction_whose_second_clause_matches_nothing_is_empty()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 && s.Country == "ZZ"));

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task Where_disjunction_unions_the_per_clause_scans()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 40 || s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol", "dave" }));
    }

    [Test]
    public async Task Where_disjunction_yields_a_grain_matching_both_branches_once()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 || s.Country == "GB"));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_disjunction_over_the_same_property_still_yields_each_grain_once()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Age >= 18 || s.Age >= 30));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_distributes_a_conjunction_over_a_disjunction()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(
            s => (s.Country == "GB" || s.Country == "DE") && s.Age >= 18));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_negated_comparison_is_the_complement_of_its_range()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => !(s.Age >= 18)));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice" }));
    }

    [Test]
    public async Task Where_negated_relational_comparison_keeps_the_null_rows()
    {
        var index = Populated();

        // C# makes every relational comparison with a null operand false, so
        // negating one has to keep the grain whose LastSeen is null.
        var keys = await KeysAsync(index.Index.Where(s => !(s.LastSeen > Epoch)));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob" }));
    }

    [Test]
    public async Task Where_negated_conjunction_expands_by_de_morgan()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => !(s.Age >= 18 && s.Country == "GB")));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "dave" }));
    }

    [Test]
    public async Task Where_equality_against_null_selects_the_null_slot()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.LastSeen == null));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice" }));
    }

    [Test]
    public async Task Where_inequality_against_null_selects_every_present_value()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.LastSeen != null));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_date_range_is_served_from_the_key_range()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.LastSeen >= Epoch.AddDays(10)));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol", "dave" }));
    }

    [Test]
    public async Task Where_over_a_type_with_no_ordered_key_uses_the_payload_scan()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => s.Status == TestStatus.Active));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_a_bare_boolean_member_reads_as_equality_with_true()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => s.IsActive));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "carol" }));
    }

    [Test]
    public async Task Where_a_negated_boolean_member_reads_as_equality_with_false()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => !s.IsActive));

        Assert.That(keys, Is.EquivalentTo(new[] { "bob", "nan" }));
    }

    [Test]
    public async Task Where_floating_point_range_excludes_not_a_number()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => s.Score < 5.0));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol" }));
    }

    [Test]
    public async Task Where_floating_point_greater_than_excludes_not_a_number()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => s.Score > -1.0));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol" }));
    }

    [Test]
    public async Task Where_equality_with_zero_matches_negative_zero()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => s.Score == 0.0));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob" }));
    }

    [Test]
    public async Task Where_equality_with_not_a_number_matches_nothing()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => s.Score == double.NaN));

        Assert.That(keys, Is.Empty);
    }

    [Test]
    public async Task Where_inequality_with_not_a_number_matches_everything()
    {
        var index = Wide();

        var keys = await KeysAsync(index.Index.Where(s => s.Score != double.NaN));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "nan" }));
    }

    [Test]
    public async Task Where_a_widened_literal_is_converted_to_the_property_type()
    {
        var index = Wide();

        // The literal is an int against a double property; the compiler widens
        // the comparison but the captured literal is still an int.
        var keys = await KeysAsync(index.Index.Where(s => s.Score >= 1));

        Assert.That(keys, Is.EquivalentTo(new[] { "carol" }));
    }

    [Test]
    public async Task Where_a_constant_true_predicate_enumerates_every_indexed_grain()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => true));

        Assert.That(keys, Is.EquivalentTo(new[] { "alice", "bob", "carol", "dave" }));
    }

    [Test]
    public async Task Where_a_constant_false_predicate_matches_nothing()
    {
        var index = Populated();

        var keys = await KeysAsync(index.Index.Where(s => false));

        Assert.That(keys, Is.Empty);
    }

    private static QueryTestIndex Wide() => QueryTestIndex.Create(
        WideDefinition(),
        ("alice", QueryTestIndex.State(country: "GB", isActive: true, score: 0.0)),
        ("bob", QueryTestIndex.State(country: "FR", isActive: false, score: -0.0)),
        ("carol", QueryTestIndex.State(country: "GB", isActive: true, score: 4.5)),
        ("nan", QueryTestIndex.State(country: "DE", isActive: false, score: double.NaN)));

    private static GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState> WideDefinition() =>
        new(
            "Subjects",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [
                IndexedTestIndex.Property<int>("Age", static s => s.Age),
                IndexedTestIndex.Property<string>("Country", static s => s.Country),
                IndexedTestIndex.Property<bool>("IsActive", static s => s.IsActive),
                IndexedTestIndex.Property<double>("Score", static s => s.Score),
            ]);
}
