using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Cover for <see cref="GaugeScrape"/>, the reader that resolves one observable
/// gauge series to one value.
/// <para>
/// These are the positive controls for the loud-failure half of issue #3004.
/// The producer fix for that issue is asserted by a test which passes when a
/// contradictory pair is <i>not</i> emitted - an absence - and an absence is
/// evidence only if the detector that would have reported the presence is
/// independently known to work. So the throwing path is exercised here
/// directly, against values handed to it, rather than only on the day a
/// producer regresses.
/// </para>
/// </summary>
[TestFixture]
public sealed class GaugeScrapeTests
{
    [Test]
    public void Resolve_single_returns_null_when_the_series_reported_nothing()
        => Assert.That(GaugeScrape.ResolveSingle("i", "tree", "t", Array.Empty<long>()), Is.Null,
            "no measurement must stay distinguishable from a measured zero");

    [Test]
    public void Resolve_single_returns_a_measured_zero_rather_than_null()
        => Assert.That(GaugeScrape.ResolveSingle("i", "tree", "t", new long[] { 0 }), Is.Zero,
            "a measured zero is a reading, not an absence");

    [Test]
    public void Resolve_single_returns_the_only_measurement()
        => Assert.That(GaugeScrape.ResolveSingle("i", "tree", "t", new long[] { 7 }), Is.EqualTo(7));

    /// <summary>
    /// The control proper: two measurements for one series must fail loudly,
    /// and the message must name both values. Naming them is what turns an
    /// uninterpretable <c>Expected: 1, But was: 0</c> into a report of the
    /// actual fault, which is a contradiction between producers rather than a
    /// wrong value.
    /// </summary>
    [Test]
    public void Resolve_single_throws_naming_every_value_when_one_series_reports_two()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => GaugeScrape.ResolveSingle(
                "storage.usage_deep_published", "tree", "t-42", new long[] { 1, 0 }));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("storage.usage_deep_published"),
                "the message must name the instrument");
            Assert.That(ex.Message, Does.Contain("t-42"),
                "the message must name the series");
            Assert.That(ex.Message, Does.Contain("[1, 0]"),
                "the message must name both contradicting values, not merely report a count");
        });
    }

    /// <summary>
    /// The same control, driven end to end through a real
    /// <see cref="MeterListener"/> scrape rather than through a handed-in list,
    /// so the throwing path is known to be reachable from
    /// <see cref="GaugeScrape.ReadSingle"/> and not only from its resolver.
    /// </summary>
    [Test]
    public void Read_single_throws_when_a_gauge_emits_two_measurements_for_one_series()
    {
        using var meter = new Meter($"gauge-scrape-{Guid.NewGuid():N}");
        const string Tree = "t-dup";
        meter.CreateObservableGauge(
            "probe.contradictory",
            () => new[]
            {
                new Measurement<long>(1, new KeyValuePair<string, object?>("tree", Tree)),
                new Measurement<long>(0, new KeyValuePair<string, object?>("tree", Tree)),
            });

        var ex = Assert.Throws<InvalidOperationException>(
            () => GaugeScrape.ReadSingle(meter, "probe.contradictory", "tree", Tree));

        Assert.That(ex!.Message, Does.Contain("[1, 0]"));
    }

    /// <summary>
    /// The matching negative control: an ordinary single-valued gauge scrapes
    /// cleanly through the same path. Without this, a reader that threw on
    /// every scrape would satisfy the test above.
    /// </summary>
    [Test]
    public void Read_single_reads_an_ordinary_gauge_and_ignores_other_series()
    {
        using var meter = new Meter($"gauge-scrape-{Guid.NewGuid():N}");
        meter.CreateObservableGauge(
            "probe.single",
            () => new[]
            {
                new Measurement<long>(5, new KeyValuePair<string, object?>("tree", "wanted")),
                new Measurement<long>(9, new KeyValuePair<string, object?>("tree", "other")),
            });

        Assert.Multiple(() =>
        {
            Assert.That(GaugeScrape.ReadSingle(meter, "probe.single", "tree", "wanted"), Is.EqualTo(5));
            Assert.That(GaugeScrape.ReadSingle(meter, "probe.single", "tree", "absent"), Is.Null,
                "a series the gauge never reported must read as absent, not as a zero");
        });
    }
}
