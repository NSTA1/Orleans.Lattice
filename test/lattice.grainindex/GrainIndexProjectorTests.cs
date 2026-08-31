using System.Text.Json;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The projector's core contract: one entry per declared property, each keyed
/// so it points back at exactly one grain, and each carrying a payload the
/// query side can read.
/// </summary>
[TestFixture]
public class GrainIndexProjectorTests
{
    private static IndexedTestState State() => new()
    {
        Age = 17,
        Country = "GB",
        LastSeen = new DateTimeOffset(2026, 8, 31, 9, 45, 57, TimeSpan.Zero),
        IsActive = true,
        Score = 12.5,
        Status = TestStatus.Active,
        Tenant = Guid.Parse("11111111-2222-3333-4444-555555555555"),
        Secret = "do not index me",
    };

    [Test]
    public void Project_produces_one_entry_per_declared_property()
    {
        var projection = IndexedTestIndex.Projector().Project("alice", State());

        Assert.That(projection.GrainKey, Is.EqualTo("alice"));
        Assert.That(projection.Entries, Has.Count.EqualTo(4));
        Assert.That(
            projection.Entries.Select(e =>
            {
                GrainIndexKeyEncoder.TryParseKey(e.Key, out var name, out _, out _);
                return name;
            }),
            Is.EqualTo(new[] { "Age", "Country", "LastSeen", "Status" }));
    }

    [Test]
    public void Project_omits_a_property_the_index_did_not_include()
    {
        var projection = IndexedTestIndex.Projector().Project("alice", State());

        Assert.That(
            projection.Entries.Any(e => System.Text.Encoding.UTF8.GetString(e.Value).Contains("Secret", StringComparison.Ordinal)),
            Is.False);
    }

    [Test]
    public void Every_projected_entry_points_back_at_exactly_one_grain()
    {
        var projection = IndexedTestIndex.Projector().Project("alice", State());

        foreach (var entry in projection.Entries)
        {
            Assert.That(GrainIndexKeyEncoder.TryParseKey(entry.Key, out _, out _, out var grainKey), Is.True);
            Assert.That(grainKey, Is.EqualTo("alice"));

            using var document = JsonDocument.Parse(entry.Value);
            Assert.That(
                document.RootElement.GetProperty(GrainIndexEntryValue.GrainKeyField).GetString(),
                Is.EqualTo("alice"));
        }
    }

    [Test]
    public void Two_grains_with_the_same_value_project_to_distinct_keys()
    {
        var projector = IndexedTestIndex.Projector();
        var alice = projector.Project("alice", State());
        var bob = projector.Project("bob", State());

        Assert.That(
            alice.Entries.Select(e => e.Key).Intersect(bob.Entries.Select(e => e.Key), StringComparer.Ordinal),
            Is.Empty);
    }

    [Test]
    public void The_payload_names_the_value_field_after_the_property_and_carries_the_metadata_fields()
    {
        var projection = IndexedTestIndex.Projector().Project("alice", State());

        using var document = JsonDocument.Parse(IndexedTestIndex.EntryFor(projection, "Age").Value);
        var root = document.RootElement;

        Assert.That(root.GetProperty("Age").GetInt32(), Is.EqualTo(17));
        Assert.That(root.GetProperty(GrainIndexEntryValue.GrainKeyField).GetString(), Is.EqualTo("alice"));
        Assert.That(root.GetProperty(GrainIndexEntryValue.PropertyField).GetString(), Is.EqualTo("Age"));
        Assert.That(root.EnumerateObject().Count(), Is.EqualTo(3));
    }

    [Test]
    public void The_payload_field_name_matches_the_member_path_the_translator_derives()
    {
        var node = LatticePredicateTranslator.Translate<IndexedTestState>(s => s.Age >= 18);
        var member = FindMemberPath(node);
        Assert.That(member, Is.EqualTo("Age"));

        var projection = IndexedTestIndex.Projector().Project("alice", State());
        using var document = JsonDocument.Parse(IndexedTestIndex.EntryFor(projection, "Age").Value);

        Assert.That(document.RootElement.TryGetProperty(member!, out _), Is.True);
    }

    [Test]
    public void A_null_property_value_is_written_as_json_null()
    {
        var state = State();
        state.LastSeen = null;

        var projection = IndexedTestIndex.Projector().Project("alice", state);
        using var document = JsonDocument.Parse(IndexedTestIndex.EntryFor(projection, "LastSeen").Value);

        Assert.That(document.RootElement.GetProperty("LastSeen").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public void An_enum_property_is_written_as_its_underlying_number()
    {
        var projection = IndexedTestIndex.Projector().Project("alice", State());
        using var document = JsonDocument.Parse(IndexedTestIndex.EntryFor(projection, "Status").Value);

        Assert.That(document.RootElement.GetProperty("Status").GetInt64(), Is.EqualTo((long)TestStatus.Active));
    }

    [Test]
    public void A_bool_property_is_written_as_a_json_boolean()
    {
        var projector = IndexedTestIndex.Projector(
            IndexedTestIndex.SingleProperty("IsActive", static s => s.IsActive));

        var projection = projector.Project("alice", State());
        using var document = JsonDocument.Parse(projection.Entries[0].Value);

        Assert.That(document.RootElement.GetProperty("IsActive").ValueKind, Is.EqualTo(JsonValueKind.True));
    }

    [Test]
    public void A_non_finite_double_is_written_as_the_named_literal_rather_than_failing()
    {
        var projector = IndexedTestIndex.Projector(
            IndexedTestIndex.SingleProperty("Score", static s => s.Score));

        foreach (var (value, expected) in new[]
        {
            (double.NaN, "NaN"),
            (double.PositiveInfinity, "Infinity"),
            (double.NegativeInfinity, "-Infinity"),
        })
        {
            var state = State();
            state.Score = value;

            using var document = JsonDocument.Parse(projector.Project("alice", state).Entries[0].Value);
            Assert.That(document.RootElement.GetProperty("Score").GetString(), Is.EqualTo(expected));
        }
    }

    [Test]
    public void A_guid_property_is_written_as_the_text_a_predicate_constant_would_capture()
    {
        var projector = IndexedTestIndex.Projector(
            IndexedTestIndex.SingleProperty("Tenant", static s => s.Tenant));
        var state = State();

        using var document = JsonDocument.Parse(projector.Project("alice", state).Entries[0].Value);

        Assert.That(
            document.RootElement.GetProperty("Tenant").GetString(),
            Is.EqualTo(state.Tenant.ToString()));
    }

    [Test]
    public void Projecting_the_same_state_twice_produces_identical_entries()
    {
        var projector = IndexedTestIndex.Projector();

        Assert.That(
            projector.Project("alice", State()).Entries,
            Is.EqualTo(projector.Project("alice", State()).Entries));
    }

    [Test]
    public void Projecting_an_index_with_no_properties_yields_an_empty_projection()
    {
        var projection = IndexedTestIndex.Projector(IndexedTestIndex.Empty()).Project("alice", State());

        Assert.That(projection.Entries, Is.Empty);
        Assert.That(projection.GrainKey, Is.EqualTo("alice"));
    }

    [Test]
    public void Project_accepts_an_empty_grain_key()
    {
        var projection = IndexedTestIndex.Projector().Project(string.Empty, State());

        Assert.That(projection.Entries, Has.Count.EqualTo(4));
        foreach (var entry in projection.Entries)
        {
            Assert.That(GrainIndexKeyEncoder.TryParseKey(entry.Key, out _, out _, out var grainKey), Is.True);
            Assert.That(grainKey, Is.Empty);
        }
    }

    [Test]
    public void Project_from_a_grain_id_encodes_the_identity_with_the_definition_codec()
    {
        var projector = IndexedTestIndex.Projector();
        var grainId = Orleans.Runtime.GrainId.Create("test", "alice");

        Assert.That(
            projector.Project(grainId, State()).Entries,
            Is.EqualTo(projector.Project("alice", State()).Entries));
    }

    [Test]
    public void Plan_from_a_grain_id_matches_the_string_key_overload()
    {
        var projector = IndexedTestIndex.Projector();
        var grainId = Orleans.Runtime.GrainId.Create("test", "alice");
        var previous = GrainIndexProjection.Empty("alice");

        Assert.That(
            projector.Plan(previous, grainId, State()).Projection.Entries,
            Is.EqualTo(projector.Plan(previous, "alice", State()).Projection.Entries));
    }

    [Test]
    public void The_projector_exposes_the_definition_it_projects()
    {
        var definition = IndexedTestIndex.Definition("Named");

        Assert.That(new GrainIndexProjector<ITestStringKeyedGrain, IndexedTestState>(definition).Definition,
            Is.SameAs(definition));
    }

    [Test]
    public void The_projector_rejects_a_null_definition()
    {
        Assert.That(
            () => new GrainIndexProjector<ITestStringKeyedGrain, IndexedTestState>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Project_rejects_null_arguments()
    {
        var projector = IndexedTestIndex.Projector();

        Assert.That(() => projector.Project(null!, State()), Throws.ArgumentNullException);
        Assert.That(() => projector.Project("alice", null!), Throws.ArgumentNullException);
        Assert.That(() => projector.Plan(null!, "alice", State()), Throws.ArgumentNullException);
        Assert.That(() => projector.Plan(GrainIndexProjection.Empty("alice"), (string)null!, State()), Throws.ArgumentNullException);
    }

    private static string? FindMemberPath(in LatticePredicateNode node)
    {
        if (node.Kind == LatticePredicateNodeKind.Member)
            return node.MemberPath;

        var children = node.Children;
        if (children is null)
            return null;

        foreach (var child in children)
        {
            var found = FindMemberPath(child);
            if (found is not null)
                return found;
        }

        return null;
    }
}
