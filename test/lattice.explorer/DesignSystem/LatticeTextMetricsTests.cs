using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the text width estimator the adaptive strips measure their
/// capacity with.
/// </summary>
/// <remarks>
/// The estimator is an approximation of a proportional face, so these assert
/// the properties a layout depends on - monotonicity, proportionality to the
/// type size, and a sane treatment of characters outside the table - rather
/// than exact pixel values a font would have to reproduce.
/// </remarks>
[TestFixture]
public sealed class LatticeTextMetricsTests
{
    [Test]
    public void MeasureEm_ofEmptyText_isZero()
    {
        Assert.That(LatticeTextMetrics.MeasureEm(ReadOnlySpan<char>.Empty), Is.Zero);
    }

    [Test]
    public void MeasureEm_growsWithTheNumberOfCharacters()
    {
        Assert.That(
            LatticeTextMetrics.MeasureEm("Retention and residency"),
            Is.GreaterThan(LatticeTextMetrics.MeasureEm("Data")));
    }

    [Test]
    public void MeasureEm_distinguishesWideFromNarrowCharacters()
    {
        Assert.That(
            LatticeTextMetrics.MeasureEm("WWWW"),
            Is.GreaterThan(LatticeTextMetrics.MeasureEm("llll")),
            "a proportional face is the whole reason a fixed capacity cannot be right");
    }

    [Test]
    public void MeasureEm_ofAFullWidthScript_chargesAWholeEmPerCharacter()
    {
        Assert.That(
            LatticeTextMetrics.MeasureEm("\u30c7\u30fc\u30bf"),
            Is.EqualTo(3 * LatticeTextMetrics.WideAdvanceEm).Within(0.0001));
    }

    [Test]
    public void MeasureEm_ofAnUnknownNarrowCharacter_usesTheFallbackAdvance()
    {
        Assert.That(
            LatticeTextMetrics.MeasureEm("\u00e9"),
            Is.EqualTo(LatticeTextMetrics.FallbackAdvanceEm).Within(0.0001));
    }

    [Test]
    public void Measure_scalesLinearlyWithTheTypeSize()
    {
        var atTen = LatticeTextMetrics.Measure("Metrics", 10);
        var atTwenty = LatticeTextMetrics.Measure("Metrics", 20);

        Assert.That(atTwenty, Is.EqualTo(2 * atTen).Within(0.0001));
    }

    [Test]
    public void Measure_ofNullText_isZero()
    {
        Assert.That(LatticeTextMetrics.Measure((string?)null, 16), Is.Zero);
    }

    [Test]
    public void Measure_ofASpan_matchesTheStringOverload()
    {
        Assert.That(
            LatticeTextMetrics.Measure("Topology".AsSpan(), 14.4),
            Is.EqualTo(LatticeTextMetrics.Measure("Topology", 14.4)));
    }

    [TestCase(0)]
    [TestCase(-4)]
    public void Measure_withANonPositiveTypeSize_isZero(double fontSizePx)
    {
        Assert.That(LatticeTextMetrics.Measure("Metrics", fontSizePx), Is.Zero);
    }

    [Test]
    public void Measure_isStableAcrossCalls()
    {
        // The layout is decided from this on every render, so an estimator that
        // wandered would make a strip flicker between two shapes.
        var first = LatticeTextMetrics.Measure("Dead letters", 14.4);
        var second = LatticeTextMetrics.Measure("Dead letters", 14.4);

        Assert.That(second, Is.EqualTo(first));
    }

    [Test]
    public void MeasureEm_ofASpaceIsNarrowerThanALetter()
    {
        Assert.That(
            LatticeTextMetrics.MeasureEm(" "),
            Is.LessThan(LatticeTextMetrics.MeasureEm("m")));
    }
}
