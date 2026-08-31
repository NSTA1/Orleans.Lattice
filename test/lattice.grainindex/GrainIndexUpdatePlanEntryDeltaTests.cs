using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.GrainIndex.Tests.Enrollment;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexUpdatePlan.EntryDelta"/>: the exact net change a
/// plan makes to an index's entry count, which is what keeps the entry-count
/// instrument from drifting on an in-place rewrite.
/// </summary>
[TestFixture]
public sealed class GrainIndexUpdatePlanEntryDeltaTests
{
    private const string GrainKey = "alice";

    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    [Test]
    public void A_grain_entering_the_index_adds_one_entry_per_projected_property()
    {
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        Assert.That(plan.EntryDelta, Is.EqualTo(plan.Projection.Entries.Count));
    }

    [Test]
    public void A_value_that_moved_changes_no_entry_count()
    {
        var plan = GrainIndexUpdatePlan.Between(Projection(17), Projection(18));

        Assert.Multiple(() =>
        {
            Assert.That(plan.Upserts, Is.Not.Empty);
            Assert.That(plan.Deletes, Is.Not.Empty);
            Assert.That(plan.EntryDelta, Is.Zero);
        });
    }

    [Test]
    public void An_unchanged_projection_changes_no_entry_count()
    {
        var plan = GrainIndexUpdatePlan.Between(Projection(30), Projection(30));

        Assert.Multiple(() =>
        {
            Assert.That(plan.IsEmpty, Is.True);
            Assert.That(plan.EntryDelta, Is.Zero);
        });
    }

    [Test]
    public void Withdrawing_a_grain_removes_every_entry_it_contributed()
    {
        var previous = Projection(30);
        var plan = GrainIndexUpdatePlan.Removing(previous);

        Assert.That(plan.EntryDelta, Is.EqualTo(-previous.Entries.Count));
    }

    [Test]
    public void Withdrawing_a_grain_that_contributed_nothing_changes_no_entry_count()
    {
        var plan = GrainIndexUpdatePlan.Removing(GrainIndexProjection.Empty(GrainKey));

        Assert.That(plan.EntryDelta, Is.Zero);
    }

    [Test]
    public void A_hand_built_plan_falls_back_to_the_upsert_minus_tombstone_count()
    {
        var plan = new GrainIndexUpdatePlan(
            GrainIndexProjection.Empty(GrainKey),
            [new KeyValuePair<string, byte[]>("k1", [1]), new KeyValuePair<string, byte[]>("k2", [2])],
            ["k0"]);

        Assert.That(plan.EntryDelta, Is.EqualTo(1));
    }

    [Test]
    public void An_explicit_delta_overrides_the_fallback()
    {
        var plan = new GrainIndexUpdatePlan(
            GrainIndexProjection.Empty(GrainKey),
            [new KeyValuePair<string, byte[]>("k1", [1])],
            [],
            entryDelta: 0);

        Assert.That(plan.EntryDelta, Is.Zero);
    }

    [Test]
    public void The_delta_survives_a_serialization_round_trip_so_a_drained_plan_still_counts()
    {
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection(30));

        var round = _serializer.Deserialize<GrainIndexUpdatePlan>(_serializer.SerializeToArray(plan));

        Assert.That(round.EntryDelta, Is.EqualTo(plan.EntryDelta));
    }

    private static GrainIndexProjection Projection(int age) =>
        EnrollmentTestIndex.Project(GrainKey, new IndexedTestState { Age = age, Country = "GB" });
}
