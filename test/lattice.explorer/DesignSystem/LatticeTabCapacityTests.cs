using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the measured inline capacity that replaced the fixed
/// per-breakpoint constants.
/// </summary>
/// <remarks>
/// The defect these close is that a constant cannot know whether a strip holds
/// <c>Data</c> or <c>Retention and residency</c>: at four-per-band it collapsed
/// six short labels that fit, and let a long one overflow its slot. Every case
/// here is a pure function of its arguments, so none is timing-, ordering- or
/// environment-dependent.
/// </remarks>
[TestFixture]
public sealed class LatticeTabCapacityTests
{
    private static readonly LatticeTabItem[] SixShortTabs =
    [
        new("metrics", "Metrics"),
        new("topology", "Topology"),
        new("data", "Data"),
        new("dead-letter", "Dead letters"),
        new("history", "History"),
        new("tag-index", "Tag index"),
    ];

    private static readonly LatticeTabItem[] SixLongTabs =
    [
        new("retention", "Retention and residency"),
        new("compliance", "Compliance and schema policy"),
        new("membership", "Membership and delegation"),
        new("dead-letter", "Dead letters and replay"),
        new("history", "History and revision timeline"),
        new("tag-index", "Tag indexes and projections"),
    ];

    [Test]
    public void Measure_ofAnEmptyStrip_isZero()
    {
        Assert.That(LatticeTabCapacity.Measure(Array.Empty<LatticeTabItem>(), 1000), Is.Zero);
    }

    [Test]
    public void Measure_withNullTabs_throws()
    {
        Assert.That(
            () => LatticeTabCapacity.Measure(null!, 1000),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Measure_withNullTabs_andExplicitMetrics_throws()
    {
        Assert.That(
            () => LatticeTabCapacity.Measure(null!, 1000, LatticeStripMetrics.Default),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Measure_whenEverythingFits_isTheWholeStrip()
    {
        Assert.That(LatticeTabCapacity.Measure(SixShortTabs, 2000), Is.EqualTo(SixShortTabs.Length));
    }

    [Test]
    public void Measure_whenEverythingFits_doesNotReserveTheOverflowControl()
    {
        // Exactly the total width of the strip and nothing more: a strip that
        // fits renders no overflow control, so it must not be charged for one.
        var metrics = LatticeStripMetrics.Default;
        var exact = 0.0;
        for (var i = 0; i < SixShortTabs.Length; i++)
        {
            exact += metrics.MeasureItemWidth(SixShortTabs[i].Label);
            if (i > 0)
            {
                exact += metrics.ItemGapPx;
            }
        }

        Assert.That(LatticeTabCapacity.Measure(SixShortTabs, exact), Is.EqualTo(SixShortTabs.Length));
    }

    [Test]
    public void Measure_fitsFewerLongLabelsThanShortOnesInTheSameWidth()
    {
        Assert.That(
            LatticeTabCapacity.Measure(SixLongTabs, 600),
            Is.LessThan(LatticeTabCapacity.Measure(SixShortTabs, 600)));
    }

    [Test]
    public void Measure_growsWithTheWidthAvailable()
    {
        var narrow = LatticeTabCapacity.Measure(SixShortTabs, 320);
        var wide = LatticeTabCapacity.Measure(SixShortTabs, 560);

        Assert.That(wide, Is.GreaterThan(narrow));
    }

    [Test]
    public void Measure_neverExceedsTheStripsLength()
    {
        Assert.That(LatticeTabCapacity.Measure(SixShortTabs, 100000), Is.EqualTo(SixShortTabs.Length));
    }

    [TestCase(0)]
    [TestCase(-500)]
    [TestCase(1)]
    public void Measure_inAWidthThatCannotHoldEvenOneLabel_stillKeepsOneInline(double availableWidthPx)
    {
        Assert.That(LatticeTabCapacity.Measure(SixShortTabs, availableWidthPx), Is.EqualTo(1));
    }

    [Test]
    public void Measure_subtractsTheGutterFromTheWidthAvailable()
    {
        var withoutGutter = new LatticeStripMetrics(14.4, 12, 4, 64, GutterPx: 0);
        var withGutter = withoutGutter with { GutterPx = 200 };

        Assert.That(
            LatticeTabCapacity.Measure(SixShortTabs, 600, withGutter),
            Is.LessThan(LatticeTabCapacity.Measure(SixShortTabs, 600, withoutGutter)));
    }

    [Test]
    public void Measure_reservesRoomForTheOverflowControlWhenTheStripOverflows()
    {
        var cheapControl = new LatticeStripMetrics(14.4, 12, 4, OverflowControlPx: 0, GutterPx: 0);
        var costlyControl = cheapControl with { OverflowControlPx = 200 };

        Assert.That(
            LatticeTabCapacity.Measure(SixShortTabs, 300, costlyControl),
            Is.LessThan(LatticeTabCapacity.Measure(SixShortTabs, 300, cheapControl)));
    }

    [Test]
    public void Measure_withASegmentedGeometry_fitsMoreThanATabGeometry()
    {
        Assert.That(
            LatticeTabCapacity.Measure(SixShortTabs, 400, LatticeStripMetrics.Segment),
            Is.GreaterThanOrEqualTo(
                LatticeTabCapacity.Measure(SixShortTabs, 400, LatticeStripMetrics.Default)));
    }

    [Test]
    public void Measure_isDeterministic()
    {
        Assert.That(
            LatticeTabCapacity.Measure(SixShortTabs, 480),
            Is.EqualTo(LatticeTabCapacity.Measure(SixShortTabs, 480)));
    }

    [Test]
    public void Measure_composesWithTheOverflowLayoutSoTheActiveItemStaysInline()
    {
        var capacity = LatticeTabCapacity.Measure(SixLongTabs, 320);
        var layout = LatticeOverflowLayout.Resolve(SixLongTabs.Length, activeIndex: 5, capacity);

        Assert.Multiple(() =>
        {
            Assert.That(layout.HasOverflow, Is.True);
            Assert.That(layout.IsInline(5), Is.True, "the promotion rule survives the measurement");
        });
    }
}
