using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="GrainIndexEnrollmentSlot"/> and
/// <see cref="GrainIndexOutboxDrainResult"/>: the two small carriers the
/// enrolment path passes around.
/// </summary>
[TestFixture]
public sealed class EnrollmentValueTypeTests
{
    [Test]
    public void A_slot_carries_what_activation_learned_about_the_grain()
    {
        var projection = GrainIndexProjection.Empty("alice");
        var slot = new GrainIndexEnrollmentSlot("alice", projection, enrolled: true);

        Assert.Multiple(() =>
        {
            Assert.That(slot.GrainKey, Is.EqualTo("alice"));
            Assert.That(slot.Confirmed, Is.SameAs(projection));
            Assert.That(slot.Enrolled, Is.True);
            Assert.That(slot.Pending, Is.Null,
                "A freshly activated grain owes nothing until it writes.");
        });
    }

    [Test]
    public void A_slots_fields_are_mutable_so_an_activation_tracks_itself_without_reallocating()
    {
        var slots = new GrainIndexEnrollmentSlot[1];
        slots[0] = new GrainIndexEnrollmentSlot("alice", GrainIndexProjection.Empty("alice"), false);

        slots[0].Enrolled = true;

        Assert.That(slots[0].Enrolled, Is.True,
            "The slot is reached through the array indexer rather than copied into a local, which "
            + "is what keeps a write's bookkeeping allocation-free.");
    }

    [Test]
    public void A_drain_result_reports_an_empty_pass_as_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new GrainIndexOutboxDrainResult(0, 0, 0, 0).IsEmpty, Is.True);
            Assert.That(new GrainIndexOutboxDrainResult(1, 1, 0, 0).IsEmpty, Is.False);
        });
    }

    [Test]
    public void A_drain_result_carries_each_outcome_separately()
    {
        var result = new GrainIndexOutboxDrainResult(10, 7, 2, 1);

        Assert.Multiple(() =>
        {
            Assert.That(result.Scanned, Is.EqualTo(10));
            Assert.That(result.Applied, Is.EqualTo(7));
            Assert.That(result.Failed, Is.EqualTo(2));
            Assert.That(result.Skipped, Is.EqualTo(1),
                "Skipped and failed mean different things - one is another silo's work, the other "
                + "is work still owed here - so a single counter would hide a real problem.");
        });
    }

    [Test]
    public void Two_drain_results_describing_the_same_pass_compare_equal()
    {
        Assert.That(
            new GrainIndexOutboxDrainResult(1, 1, 0, 0),
            Is.EqualTo(new GrainIndexOutboxDrainResult(1, 1, 0, 0)));
    }
}
