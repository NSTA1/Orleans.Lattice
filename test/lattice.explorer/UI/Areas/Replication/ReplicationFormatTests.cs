using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

[TestFixture]
public sealed class ReplicationFormatTests
{
    [Test]
    public void Source_unknown_enrolment_preserves_the_wire_value() =>
        Assert.That(ReplicationFormat.Source((ReplicationEnrollmentSource)999), Is.EqualTo("999"));

    [TestCase(ReplicationLinkStallReason.ReseedRequired, "Re-seed required")]
    [TestCase(ReplicationLinkStallReason.DeadLetterQueueFull, "Dead-letter queue full")]
    [TestCase((ReplicationLinkStallReason)999, "Unknown")]
    [TestCase(null, null)]
    public void StallReason_distinguishes_known_unknown_and_absent_causes(ReplicationLinkStallReason? reason, string? expected) =>
        Assert.That(ReplicationFormat.StallReason(reason), Is.EqualTo(expected));

    [TestCase(LatticeMergeMode.OrSet, "OR-set")]
    [TestCase((LatticeMergeMode)999, "999")]
    public void MergeMode_nullable_value_uses_the_same_label_as_a_nonnullable_value(LatticeMergeMode mode, string expected) =>
        Assert.That(ReplicationFormat.MergeMode((LatticeMergeMode?)mode), Is.EqualTo(expected));

    [TestCase(0L, 1024L, "0 entries, 1.0 KB behind")]
    [TestCase(1L, 0L, "1 entry, 0 B behind")]
    [TestCase(-1L, 0L, "Caught up")]
    [TestCase(0L, -1L, "Caught up")]
    public void Backlog_either_positive_measure_is_enough_to_be_behind(long entries, long bytes, string expected) =>
        Assert.That(ReplicationFormat.Backlog(entries, bytes), Is.EqualTo(expected));

    [TestCase(60L, "1 min ago")]
    [TestCase(3600L, "1 h 0 min ago")]
    [TestCase(86400L, "1 d ago")]
    public void Contact_at_a_unit_boundary_switches_to_the_larger_unit(long seconds, string expected) =>
        Assert.That(ReplicationFormat.Contact(TimeSpan.FromSeconds(seconds)), Is.EqualTo(expected));

    [Test]
    public void Bytes_largest_count_stays_in_the_last_supported_unit() =>
        Assert.That(ReplicationFormat.Bytes(long.MaxValue), Is.EqualTo("8192 PB"));
}
