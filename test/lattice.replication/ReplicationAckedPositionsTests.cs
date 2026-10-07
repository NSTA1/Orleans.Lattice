using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4684: the acknowledged read positions a shipper vouches beside its
/// watermark arrive as peer input in a call header, so the receiver parses them
/// strictly and bounded; and a sibling passes its boundary only on the captured
/// log at or past every captured tail.
/// </summary>
[TestFixture]
public class ReplicationAckedPositionsTests
{
    private static readonly ReplicationAckedPositions Positions = new()
    {
        PhysicalTreeId = "tree-a|physical.0",
        Positions = [4, 0, 9],
    };

    [Test]
    public void The_text_form_round_trips_any_log_id()
    {
        Assert.That(ReplicationAckedPositions.TryParse(Positions.ToText(), out var parsed), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(parsed!.PhysicalTreeId, Is.EqualTo(Positions.PhysicalTreeId));
            Assert.That(parsed.Positions, Is.EqualTo(Positions.Positions));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("2|dHJlZQ==|1,2")]
    [TestCase("1||1,2")]
    [TestCase("1|dHJlZQ==|")]
    [TestCase("1|not base64|1")]
    [TestCase("1|dHJlZQ==|1,-2")]
    [TestCase("1|dHJlZQ==|1,x")]
    [TestCase("1|dHJlZQ==|1|2")]
    [TestCase("1|dHJlZQ==|1,,2")]
    public void Anything_but_the_canonical_form_is_refused(string? text)
    {
        Assert.That(ReplicationAckedPositions.TryParse(text, out var parsed), Is.False);
        Assert.That(parsed, Is.Null);
    }

    [Test]
    public void Over_long_text_and_too_many_partitions_are_refused()
    {
        var tooMany = "1|dHJlZQ==|" + string.Join(',', Enumerable.Repeat("1", ReplicationAckedPositions.MaxPartitions + 1));
        var tooLong = "1|dHJlZQ==|" + new string('1', ReplicationAckedPositions.MaxTextLength);
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationAckedPositions.TryParse(tooMany, out _), Is.False);
            Assert.That(ReplicationAckedPositions.TryParse(tooLong, out _), Is.False);
        });
    }

    [Test]
    public void Positions_cover_a_boundary_only_on_its_log_at_or_past_every_tail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Positions.CoversTails(Positions.PhysicalTreeId, [4, 0, 9]), Is.True);
            Assert.That(Positions.CoversTails(Positions.PhysicalTreeId, [3, 0, 2]), Is.True);
            Assert.That(Positions.CoversTails(Positions.PhysicalTreeId, [5, 0, 9]), Is.False, "one partition short");
            Assert.That(Positions.CoversTails("another-log", [0, 0, 0]), Is.False, "another physical log");
            Assert.That(Positions.CoversTails(Positions.PhysicalTreeId, [0, 0, 0, 1]), Is.False, "a partition it does not read");
        });
    }

    [Test]
    public void An_empty_boundary_is_one_whose_log_held_nothing()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new CrossTreeSiblingBoundary { PhysicalTreeId = "s", Tails = [0, 0], ExportEpoch = 1 }.IsEmpty, Is.True);
            Assert.That(new CrossTreeSiblingBoundary { PhysicalTreeId = "s", Tails = [0, 1], ExportEpoch = 1 }.IsEmpty, Is.False);
        });
    }
}
