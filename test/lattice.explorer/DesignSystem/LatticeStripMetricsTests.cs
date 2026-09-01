using Orleans.Lattice.Explorer.DesignSystem.Layout;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the strip geometry the capacity measurement reads.
/// </summary>
[TestFixture]
public sealed class LatticeStripMetricsTests
{
    [Test]
    public void Default_describesTheShippedTabStrip()
    {
        var metrics = LatticeStripMetrics.Default;

        Assert.Multiple(() =>
        {
            Assert.That(metrics.FontSizePx, Is.EqualTo(LatticeStripMetrics.TabFontSizePx));
            Assert.That(metrics.ItemPaddingInlinePx, Is.EqualTo(LatticeStripMetrics.TabPaddingInlinePx));
            Assert.That(metrics.ItemGapPx, Is.EqualTo(LatticeStripMetrics.TabGapPx));
            Assert.That(metrics.OverflowControlPx, Is.EqualTo(LatticeStripMetrics.OverflowControlWidthPx));
            Assert.That(metrics.GutterPx, Is.Zero);
        });
    }

    [Test]
    public void Segment_isTighterThanATabStrip()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeStripMetrics.Segment.FontSizePx,
                Is.LessThan(LatticeStripMetrics.Default.FontSizePx));
            Assert.That(
                LatticeStripMetrics.Segment.ItemPaddingInlinePx,
                Is.LessThan(LatticeStripMetrics.Default.ItemPaddingInlinePx));
            Assert.That(LatticeStripMetrics.Segment.ItemGapPx, Is.Zero,
                "segmented options abut inside one track");
            Assert.That(
                LatticeStripMetrics.Segment.GutterPx,
                Is.EqualTo(LatticeStripMetrics.SegmentTrackPaddingPx));
        });
    }

    [Test]
    public void MeasureItemWidth_addsPaddingOnBothSidesOfTheLabel()
    {
        var metrics = new LatticeStripMetrics(
            FontSizePx: 16,
            ItemPaddingInlinePx: 10,
            ItemGapPx: 0,
            OverflowControlPx: 0,
            GutterPx: 0);

        Assert.That(
            metrics.MeasureItemWidth("Data"),
            Is.EqualTo(LatticeTextMetrics.Measure("Data", 16) + 20).Within(0.0001));
    }

    [Test]
    public void MeasureItemWidth_ofANullLabel_isTheItemsPaddingAlone()
    {
        var metrics = new LatticeStripMetrics(16, 10, 0, 0, 0);

        Assert.That(metrics.MeasureItemWidth(null), Is.EqualTo(20));
    }

    [Test]
    public void A_metrics_value_carriesTheGeometryItWasGiven()
    {
        var metrics = new LatticeStripMetrics(
            FontSizePx: 13,
            ItemPaddingInlinePx: 7,
            ItemGapPx: 3,
            OverflowControlPx: 40,
            GutterPx: 12);

        Assert.Multiple(() =>
        {
            Assert.That(metrics.FontSizePx, Is.EqualTo(13));
            Assert.That(metrics.ItemPaddingInlinePx, Is.EqualTo(7));
            Assert.That(metrics.ItemGapPx, Is.EqualTo(3));
            Assert.That(metrics.OverflowControlPx, Is.EqualTo(40));
            Assert.That(metrics.GutterPx, Is.EqualTo(12));
        });
    }

    [Test]
    public void Two_metrics_values_withTheSameGeometryAreEqual()
    {
        Assert.That(
            new LatticeStripMetrics(13, 7, 3, 40, 12),
            Is.EqualTo(new LatticeStripMetrics(13, 7, 3, 40, 12)));
    }
}
