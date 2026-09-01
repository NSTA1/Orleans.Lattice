namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Self-check for <see cref="DefaultOmittingStateRoundTrip"/>. The sentinel
/// regression tests that depend on this harness are only meaningful while the
/// harness actually omits type-default members; a harness that quietly stopped
/// omitting would make every test built on it pass vacuously. These legs fail
/// loudly if the probe dies.
/// </summary>
[TestFixture]
public sealed class DefaultOmittingStateRoundTripTests
{
    /// <summary>
    /// A local stand-in for the hazard shape, kept here rather than pointed at a
    /// production POCO so the self-check cannot be silently disarmed by a fix to
    /// whichever POCO it happened to reference.
    /// </summary>
    private sealed class SentinelProbe
    {
        [Id(0)] public int Sentinel { get; set; } = -1;

        [Id(1)] public int Plain { get; set; }

        [Id(2)] public string? Text { get; set; }

        [Id(3)] public int? NullableSentinel { get; set; }
    }

    [Test]
    public void Simulate_drops_a_member_written_as_the_type_default_and_the_initializer_resurrects_its_value()
    {
        var written = new SentinelProbe { Sentinel = 0 };

        var reconstructed = DefaultOmittingStateRoundTrip.Simulate(written);

        Assert.That(reconstructed.Sentinel, Is.EqualTo(-1),
            "The harness must reproduce the production hazard: a correctly written 0 is omitted "
            + "and the non-default initializer resurrects -1 on load. If this leg passes with 0 "
            + "the harness has stopped omitting and every test built on it is vacuous.");
    }

    [Test]
    public void Simulate_preserves_a_member_written_as_a_non_default_value()
    {
        var written = new SentinelProbe { Sentinel = 7, Plain = 3, Text = "kept" };

        var reconstructed = DefaultOmittingStateRoundTrip.Simulate(written);

        Assert.Multiple(() =>
        {
            Assert.That(reconstructed.Sentinel, Is.EqualTo(7));
            Assert.That(reconstructed.Plain, Is.EqualTo(3));
            Assert.That(reconstructed.Text, Is.EqualTo("kept"));
        });
    }

    /// <summary>
    /// The remedy this harness exists to verify: <c>default(int?)</c> is
    /// <see langword="null"/>, so a nullable member carrying <c>0</c> is not a
    /// default, is not omitted, and survives the round trip intact.
    /// </summary>
    [Test]
    public void Simulate_preserves_a_zero_written_to_a_nullable_member()
    {
        var written = new SentinelProbe { NullableSentinel = 0 };

        var reconstructed = DefaultOmittingStateRoundTrip.Simulate(written);

        Assert.That(reconstructed.NullableSentinel, Is.Zero,
            "A nullable member carrying 0 is not default(int?) and must survive, which is why "
            + "nullability - not initializer removal - is the repair when 0 is a legitimate value.");
    }

    [Test]
    public void Simulate_drops_an_unset_nullable_member()
    {
        var written = new SentinelProbe { NullableSentinel = null };

        var reconstructed = DefaultOmittingStateRoundTrip.Simulate(written);

        Assert.That(reconstructed.NullableSentinel, Is.Null);
    }
}
