namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The reconciliation contract, which is where a grain index earns its keep:
/// full projection, a single-property change, a multi-property change, the
/// tombstoning of an entry whose value moved, and an idempotent re-projection
/// that costs nothing.
/// </summary>
[TestFixture]
public class GrainIndexUpdatePlanTests
{
    private static IndexedTestState State(int age = 17, string country = "GB", TestStatus status = TestStatus.Active) =>
        new()
        {
            Age = age,
            Country = country,
            LastSeen = new DateTimeOffset(2026, 8, 31, 9, 45, 57, TimeSpan.Zero),
            Status = status,
        };

    private static GrainIndexProjector<ITestStringKeyedGrain, IndexedTestState> Projector() =>
        IndexedTestIndex.Projector();

    [Test]
    public void A_first_projection_writes_every_entry_and_tombstones_nothing()
    {
        var projector = Projector();
        var plan = projector.Plan(GrainIndexProjection.Empty("alice"), "alice", State());

        Assert.That(plan.Upserts, Has.Count.EqualTo(4));
        Assert.That(plan.Deletes, Is.Empty);
        Assert.That(plan.IsEmpty, Is.False);
        Assert.That(
            plan.Upserts.Select(u => u.Key),
            Is.EquivalentTo(plan.Projection.Entries.Select(e => e.Key)));
    }

    [Test]
    public void Re_projecting_unchanged_state_is_a_no_op()
    {
        var projector = Projector();
        var first = projector.Project("alice", State());

        var plan = projector.Plan(first, "alice", State());

        Assert.That(plan.IsEmpty, Is.True);
        Assert.That(plan.Upserts, Is.Empty);
        Assert.That(plan.Deletes, Is.Empty);
        Assert.That(plan.Projection.Entries, Is.EqualTo(first.Entries));
    }

    [Test]
    public void Re_projecting_unchanged_state_repeatedly_stays_a_no_op()
    {
        var projector = Projector();
        var projection = projector.Project("alice", State());

        for (var i = 0; i < 3; i++)
        {
            var plan = projector.Plan(projection, "alice", State());
            Assert.That(plan.IsEmpty, Is.True, $"iteration {i}");
            projection = plan.Projection;
        }
    }

    [Test]
    public void A_single_property_change_moves_only_that_property_entry()
    {
        var projector = Projector();
        var before = projector.Project("alice", State(age: 17));

        var plan = projector.Plan(before, "alice", State(age: 18));

        Assert.That(plan.Upserts, Has.Count.EqualTo(1));
        Assert.That(plan.Deletes, Has.Count.EqualTo(1));
        Assert.That(PropertyOf(plan.Upserts[0].Key), Is.EqualTo("Age"));
        Assert.That(PropertyOf(plan.Deletes[0]), Is.EqualTo("Age"));
    }

    [Test]
    public void A_multi_property_change_moves_exactly_the_properties_that_changed()
    {
        var projector = Projector();
        var before = projector.Project("alice", State(age: 17, country: "GB"));

        var plan = projector.Plan(before, "alice", State(age: 18, country: "FR"));

        Assert.That(plan.Upserts.Select(u => PropertyOf(u.Key)), Is.EquivalentTo(new[] { "Age", "Country" }));
        Assert.That(plan.Deletes.Select(PropertyOf), Is.EquivalentTo(new[] { "Age", "Country" }));
    }

    [Test]
    public void A_value_that_moved_has_its_old_key_tombstoned_and_its_new_key_written()
    {
        var projector = Projector();
        var before = projector.Project("alice", State(age: 17));
        var after = projector.Project("alice", State(age: 18));

        var plan = GrainIndexUpdatePlan.Between(before, after);
        var staleKey = IndexedTestIndex.EntryFor(before, "Age").Key;
        var freshKey = IndexedTestIndex.EntryFor(after, "Age").Key;

        Assert.That(staleKey, Is.Not.EqualTo(freshKey));
        Assert.That(plan.Deletes, Does.Contain(staleKey));
        Assert.That(plan.Upserts.Select(u => u.Key), Does.Contain(freshKey));
        Assert.That(plan.Upserts.Select(u => u.Key), Does.Not.Contain(staleKey));
    }

    [Test]
    public void A_stale_entry_is_gone_from_the_property_range_after_the_plan_is_applied()
    {
        var projector = Projector();
        var before = projector.Project("alice", State(age: 17));
        var plan = projector.Plan(before, "alice", State(age: 18));

        // The tree state a scan would see: previous keys, minus the tombstones,
        // plus the upserts.
        var live = before.Entries.Select(e => e.Key)
            .Except(plan.Deletes, StringComparer.Ordinal)
            .Union(plan.Upserts.Select(u => u.Key), StringComparer.Ordinal)
            .Where(k => string.Equals(PropertyOf(k), "Age", StringComparison.Ordinal))
            .ToArray();

        Assert.That(live, Has.Length.EqualTo(1));
        Assert.That(live[0], Is.EqualTo(IndexedTestIndex.EntryFor(plan.Projection, "Age").Key));
    }

    [Test]
    public void An_unordered_property_updates_in_place_with_no_tombstone_when_its_value_moves()
    {
        var projector = Projector();
        var before = projector.Project("alice", State(status: TestStatus.Active));

        var plan = projector.Plan(before, "alice", State(status: TestStatus.Retired));

        Assert.That(plan.Deletes, Is.Empty, "an unordered property's key never moves");
        Assert.That(plan.Upserts, Has.Count.EqualTo(1));
        Assert.That(PropertyOf(plan.Upserts[0].Key), Is.EqualTo("Status"));
        Assert.That(
            plan.Upserts[0].Key,
            Is.EqualTo(IndexedTestIndex.EntryFor(before, "Status").Key));
    }

    [Test]
    public void A_property_dropped_from_the_definition_has_its_entry_tombstoned()
    {
        var wide = IndexedTestIndex.Projector();
        var narrow = IndexedTestIndex.Projector(IndexedTestIndex.SingleProperty("Age", static s => s.Age));

        var plan = GrainIndexUpdatePlan.Between(
            wide.Project("alice", State()),
            narrow.Project("alice", State()));

        Assert.That(plan.Upserts, Is.Empty, "the surviving property's entry is unchanged");
        Assert.That(plan.Deletes.Select(PropertyOf), Is.EquivalentTo(new[] { "Country", "LastSeen", "Status" }));
    }

    [Test]
    public void A_property_added_to_the_definition_is_written_without_disturbing_the_others()
    {
        var narrow = IndexedTestIndex.Projector(IndexedTestIndex.SingleProperty("Age", static s => s.Age));
        var wide = IndexedTestIndex.Projector();

        var plan = GrainIndexUpdatePlan.Between(
            narrow.Project("alice", State()),
            wide.Project("alice", State()));

        Assert.That(plan.Deletes, Is.Empty);
        Assert.That(plan.Upserts.Select(u => PropertyOf(u.Key)), Is.EquivalentTo(new[] { "Country", "LastSeen", "Status" }));
    }

    [Test]
    public void A_null_valued_property_that_gains_a_value_moves_its_entry()
    {
        var projector = IndexedTestIndex.Projector(
            IndexedTestIndex.SingleProperty("LastSeen", static s => s.LastSeen));

        var absent = new IndexedTestState { LastSeen = null };
        var present = new IndexedTestState { LastSeen = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero) };

        var plan = GrainIndexUpdatePlan.Between(
            projector.Project("alice", absent),
            projector.Project("alice", present));

        Assert.That(plan.Deletes, Has.Count.EqualTo(1));
        Assert.That(plan.Upserts, Has.Count.EqualTo(1));
        Assert.That(plan.Deletes[0], Is.Not.EqualTo(plan.Upserts[0].Key));
    }

    [Test]
    public void Removing_tombstones_every_entry_and_writes_nothing()
    {
        var projection = Projector().Project("alice", State());

        var plan = GrainIndexUpdatePlan.Removing(projection);

        Assert.That(plan.Upserts, Is.Empty);
        Assert.That(plan.Deletes, Is.EqualTo(projection.Entries.Select(e => e.Key).ToArray()));
        Assert.That(plan.Projection.Entries, Is.Empty);
        Assert.That(plan.Projection.GrainKey, Is.EqualTo("alice"));
        Assert.That(plan.IsEmpty, Is.False);
    }

    [Test]
    public void Removing_a_grain_that_was_never_indexed_is_an_empty_plan()
    {
        var plan = GrainIndexUpdatePlan.Removing(GrainIndexProjection.Empty("alice"));

        Assert.That(plan.IsEmpty, Is.True);
        Assert.That(plan.Projection.GrainKey, Is.EqualTo("alice"));
    }

    [Test]
    public void Between_a_projection_and_an_empty_one_tombstones_everything()
    {
        var projection = Projector().Project("alice", State());

        var plan = GrainIndexUpdatePlan.Between(projection, GrainIndexProjection.Empty("alice"));

        Assert.That(plan.Upserts, Is.Empty);
        Assert.That(plan.Deletes, Is.EquivalentTo(projection.Entries.Select(e => e.Key)));
    }

    [Test]
    public void Between_two_empty_projections_is_an_empty_plan()
    {
        var plan = GrainIndexUpdatePlan.Between(
            GrainIndexProjection.Empty("alice"),
            GrainIndexProjection.Empty("alice"));

        Assert.That(plan.IsEmpty, Is.True);
    }

    [Test]
    public void The_plan_carries_the_projection_that_becomes_the_next_baseline()
    {
        var projector = Projector();
        var first = projector.Plan(GrainIndexProjection.Empty("alice"), "alice", State(age: 17));
        var second = projector.Plan(first.Projection, "alice", State(age: 18));
        var third = projector.Plan(second.Projection, "alice", State(age: 18));

        Assert.That(second.IsEmpty, Is.False);
        Assert.That(third.IsEmpty, Is.True);
    }

    [Test]
    public void A_plan_constructed_directly_exposes_what_it_was_given()
    {
        var projection = Projector().Project("alice", State());
        var upserts = new[] { new KeyValuePair<string, byte[]>("k", [1]) };
        var deletes = new[] { "stale" };

        var plan = new GrainIndexUpdatePlan(projection, upserts, deletes);

        Assert.That(plan.Projection, Is.SameAs(projection));
        Assert.That(plan.Upserts, Is.EqualTo(upserts));
        Assert.That(plan.Deletes, Is.EqualTo(deletes));
        Assert.That(plan.IsEmpty, Is.False);
    }

    [Test]
    public void A_plan_rejects_null_arguments()
    {
        var projection = GrainIndexProjection.Empty("alice");
        var upserts = Array.Empty<KeyValuePair<string, byte[]>>();
        var deletes = Array.Empty<string>();

        Assert.That(() => new GrainIndexUpdatePlan(null!, upserts, deletes), Throws.ArgumentNullException);
        Assert.That(() => new GrainIndexUpdatePlan(projection, null!, deletes), Throws.ArgumentNullException);
        Assert.That(() => new GrainIndexUpdatePlan(projection, upserts, null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexUpdatePlan.Between(null!, projection), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexUpdatePlan.Between(projection, null!), Throws.ArgumentNullException);
        Assert.That(() => GrainIndexUpdatePlan.Removing(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Between_projections_from_different_property_orderings_reconciles_by_key_scan()
    {
        // When property ordering differs between before and after, the index-aligned
        // probe fails and IndexOfKey is called to find the entry by its key (line 202).
        var state = State();
        var before = IndexedTestIndex.Projector().Project("alice", state);

        // After projector has properties in a different order: Country first.
        var reorderedDef = new GrainIndexDefinition<ITestStringKeyedGrain, IndexedTestState>(
            "Subjects",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [
                IndexedTestIndex.Property<string>("Country", static s => s.Country),
                IndexedTestIndex.Property<int>("Age", static s => s.Age),
                IndexedTestIndex.Property<DateTimeOffset?>("LastSeen", static s => s.LastSeen),
                IndexedTestIndex.Property<TestStatus>("Status", static s => s.Status),
            ]);
        var after = IndexedTestIndex.Projector(reorderedDef).Project("alice", state);

        // Act: Between must reconcile entries across the ordering mismatch.
        var plan = GrainIndexUpdatePlan.Between(before, after);

        // No entries changed, only their position in the list, so the plan is empty.
        Assert.That(plan.IsEmpty, Is.True);
    }

    private static string PropertyOf(string key)
    {
        GrainIndexKeyEncoder.TryParseKey(key, out var property, out _, out _);
        return property;
    }
}
