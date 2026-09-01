using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The projection records are persisted by the enrolment hook and travel
/// between silos, so their Orleans wire format is part of this package's
/// contract. These tests round-trip each one through the real serializer to
/// prove the attributes are wired up and nothing is lost in transit.
/// </summary>
[TestFixture]
public class GrainIndexProjectionSerializationTests
{
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

    private T RoundTrip<T>(T value) => _serializer.Deserialize<T>(_serializer.SerializeToArray(value));

    private static GrainIndexProjection Projection() =>
        IndexedTestIndex.Projector().Project("alice", new IndexedTestState
        {
            Age = 17,
            Country = "GB",
            LastSeen = new DateTimeOffset(2026, 8, 31, 9, 45, 57, TimeSpan.Zero),
            Status = TestStatus.Active,
        });

    [Test]
    public void An_entry_round_trips_its_key_and_payload()
    {
        var entry = new GrainIndexEntry(GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice"), [1, 2, 3]);

        var copy = RoundTrip(entry);

        Assert.That(copy.Key, Is.EqualTo(entry.Key));
        Assert.That(copy.Value, Is.EqualTo(entry.Value));
        Assert.That(copy, Is.EqualTo(entry));
    }

    [Test]
    public void An_entry_key_survives_the_control_characters_the_layout_relies_on()
    {
        var entry = new GrainIndexEntry(GrainIndexKeyEncoder.EncodeKey("Country", "a\u0000b", "al\u0001ice"), [7]);

        Assert.That(RoundTrip(entry).Key, Is.EqualTo(entry.Key));
    }

    [Test]
    public void A_projection_round_trips_every_entry_in_order()
    {
        var projection = Projection();

        var copy = RoundTrip(projection);

        Assert.That(copy.GrainKey, Is.EqualTo(projection.GrainKey));
        Assert.That(copy.Entries, Is.EqualTo(projection.Entries));
    }

    [Test]
    public void An_empty_projection_round_trips()
    {
        var copy = RoundTrip(GrainIndexProjection.Empty("alice"));

        Assert.That(copy.GrainKey, Is.EqualTo("alice"));
        Assert.That(copy.Entries, Is.Empty);
    }

    [Test]
    public void An_update_plan_round_trips_its_upserts_tombstones_and_projection()
    {
        var projector = IndexedTestIndex.Projector();
        var before = projector.Project("alice", new IndexedTestState { Age = 17, Country = "GB" });
        var plan = projector.Plan(before, "alice", new IndexedTestState { Age = 18, Country = "GB" });

        var copy = RoundTrip(plan);

        Assert.That(copy.Upserts.Select(u => u.Key), Is.EqualTo(plan.Upserts.Select(u => u.Key)));
        Assert.That(copy.Upserts.Select(u => u.Value), Is.EqualTo(plan.Upserts.Select(u => u.Value)));
        Assert.That(copy.Deletes, Is.EqualTo(plan.Deletes));
        Assert.That(copy.Projection.Entries, Is.EqualTo(plan.Projection.Entries));
        Assert.That(copy.IsEmpty, Is.False);
    }

    [Test]
    public void An_empty_update_plan_round_trips_as_empty()
    {
        var plan = GrainIndexUpdatePlan.Between(Projection(), Projection());

        Assert.That(RoundTrip(plan).IsEmpty, Is.True);
    }

    [Test]
    public async Task A_deserialised_plan_still_applies_to_the_tree()
    {
        // The plan caches the upsert list it hands to the atomic write; a plan
        // that arrived over the wire has to rebuild it rather than fail.
        var tree = Substitute.For<ILattice>();
        var maintainer = new GrainIndexMaintainer<ITestStringKeyedGrain, IndexedTestState>(
            IndexedTestIndex.Definition(),
            tree);
        var plan = RoundTrip(GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty("alice"), Projection()));

        await maintainer.ApplyAsync(plan, "op-1");

        await tree.Received(1).SetManyAtomicAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(u => u.Count == 4),
            "op-1",
            Arg.Any<CancellationToken>());
    }
}
