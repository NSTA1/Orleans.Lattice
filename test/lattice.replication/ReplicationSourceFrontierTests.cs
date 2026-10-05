namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public sealed class ReplicationSourceFrontierTests
{
    private static readonly Guid Lineage = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) => new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void The_text_form_round_trips()
    {
        var frontier = new ReplicationSourceFrontier
        {
            ReceiverLineage = Lineage,
            TreeLowWatermark = Hlc(639_000_000_000_000_000, 7),
            OriginLowWatermark = Hlc(639_000_000_000_000_000, 3),
        };

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationSourceFrontier.TryParse(frontier.ToText(), out var parsed), Is.True);
            Assert.That(parsed, Is.EqualTo(frontier));
            Assert.That(frontier.ToText().Length, Is.LessThanOrEqualTo(ReplicationSourceFrontier.MaxTextLength));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("2.0f8fad5bd9cb469fa16570867728950e.10.0.5.0.1")]
    [TestCase("1.00000000000000000000000000000000.10.0.5.0.1")]
    [TestCase("1.not-a-guid.10.0.5.0.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.-10.0.5.0.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.-1.5.0.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.0.11.0.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.0.10.1.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.0.5.0.-1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.0.5.0")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.0.5.0.1.9")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e. 10.0.5.0.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.0.5.0.1 ")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.99999999999999999999.0.5.0.1")]
    [TestCase("1.0f8fad5bd9cb469fa16570867728950e.10.2147483648.5.0.1")]
    [TestCase("1.{0f8fad5bd9cb469fa16570867728950e}.10.0.5.0.1")]
    [TestCase("1..10.0.5.0.1")]
    public void Malformed_or_inconsistent_text_vouches_for_nothing(string? text)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationSourceFrontier.TryParse(text, out var parsed), Is.False);
            Assert.That(parsed, Is.EqualTo(default(ReplicationSourceFrontier)));
        });
    }

    [Test]
    public void Over_long_text_is_refused_before_it_is_split()
    {
        var text = "1.0f8fad5bd9cb469fa16570867728950e.10.0.5.0" + new string('0', ReplicationSourceFrontier.MaxTextLength);

        Assert.That(ReplicationSourceFrontier.TryParse(text, out _), Is.False);
    }
}
