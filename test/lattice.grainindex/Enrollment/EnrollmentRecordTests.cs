using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.GrainIndex.Enrollment;
using Orleans.Serialization;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="GrainIndexEnrollmentRecord"/> and
/// <see cref="GrainIndexPendingProjection"/>: the two records the enrolment path
/// persists, and their survival of the Orleans wire format they are stored in.
/// </summary>
[TestFixture]
public sealed class EnrollmentRecordTests
{
    private const string GrainKey = "alice";

    private ServiceProvider _provider = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        _provider = services.BuildServiceProvider();
    }

    [TearDown]
    public void TearDown() => _provider.Dispose();

    private static GrainIndexProjection Projection(int age = 30) =>
        EnrollmentTestIndex.Project(GrainKey, new IndexedTestState { Age = age, Country = "GB" });

    private T RoundTrip<T>(T value) where T : notnull
    {
        var serializer = _provider.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    [Test]
    public void An_enrolment_record_carries_the_projection_the_index_holds()
    {
        var projection = Projection();

        Assert.That(new GrainIndexEnrollmentRecord(projection).Projection, Is.SameAs(projection));
    }

    [Test]
    public void An_enrolment_record_rejects_a_null_projection()
    {
        Assert.That(() => new GrainIndexEnrollmentRecord(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void An_enrolment_record_survives_the_wire_format_it_is_stored_in()
    {
        var projection = Projection();

        var read = RoundTrip(new GrainIndexEnrollmentRecord(projection));

        Assert.Multiple(() =>
        {
            Assert.That(read.Projection.GrainKey, Is.EqualTo(GrainKey));
            Assert.That(read.Projection.Entries, Is.EqualTo(projection.Entries),
                "The stored projection is the next diff's baseline, so losing it in transit would "
                + "make every activation re-write entries the index already holds.");
        });
    }

    [Test]
    public void An_outbox_entry_carries_everything_a_retry_needs()
    {
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection());
        var pending = new GrainIndexPendingProjection("users", GrainKey, "op", plan);

        Assert.Multiple(() =>
        {
            Assert.That(pending.IndexName, Is.EqualTo("users"));
            Assert.That(pending.GrainKey, Is.EqualTo(GrainKey));
            Assert.That(pending.OperationId, Is.EqualTo("op"));
            Assert.That(pending.Plan, Is.SameAs(plan));
        });
    }

    [Test]
    public void An_outbox_entry_rejects_a_null_argument()
    {
        var plan = GrainIndexUpdatePlan.Between(GrainIndexProjection.Empty(GrainKey), Projection());

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexPendingProjection(null!, GrainKey, "op", plan),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexPendingProjection("users", null!, "op", plan),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexPendingProjection("users", GrainKey, null!, plan),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexPendingProjection("users", GrainKey, "op", null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void An_outbox_entry_survives_the_wire_format_with_its_whole_batch_intact()
    {
        var plan = GrainIndexUpdatePlan.Between(Projection(17), Projection(18));
        var pending = new GrainIndexPendingProjection("users", GrainKey, "op", plan);

        var read = RoundTrip(pending);

        Assert.Multiple(() =>
        {
            Assert.That(read.IndexName, Is.EqualTo("users"));
            Assert.That(read.GrainKey, Is.EqualTo(GrainKey));
            Assert.That(read.OperationId, Is.EqualTo("op"),
                "The retry reuses this key, so a batch that committed before the writer learned of "
                + "it re-attaches instead of running twice.");
            Assert.That(read.Plan.Upserts.Select(u => u.Key), Is.EqualTo(plan.Upserts.Select(u => u.Key)));
            Assert.That(read.Plan.Deletes, Is.EqualTo(plan.Deletes),
                "Losing the tombstones would leave the grain answering a scan for its old value.");
        });
    }

    [Test]
    public void A_round_tripped_batch_can_still_be_applied()
    {
        var plan = GrainIndexUpdatePlan.Between(Projection(17), Projection(18));
        var read = RoundTrip(new GrainIndexPendingProjection("users", GrainKey, "op", plan));

        Assert.That(read.Plan.IsEmpty, Is.False,
            "A batch that deserialised as empty would be silently dropped by the drain.");
    }
}
