using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The gauge's contract: the two distinctions a naive <c>long</c>/<c>0</c> model
/// would flatten survive the projection, and a bar is drawn only where one is
/// meaningful.
/// <para>
/// These are the tests that stop the surface rendering a bar that lies - an
/// unlimited tenant shown as full, or an unmeasured rate limit shown as unused.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantQuotaGaugeTests
{
    private static TenantQuotaGauge Gauge(long? usage, long? limit, long? burst = null) =>
        new(
            ExplorerTenantQuotaDimensionKind.Bytes,
            new ExplorerTenantQuotaDimension { Usage = usage, Limit = limit, BurstLimit = burst });

    [Test]
    public void Presentation_bounded_and_measured_is_a_bar() =>
        Assert.That(Gauge(usage: 250, limit: 1_000).Presentation, Is.EqualTo(TenantQuotaPresentation.Bar));

    [Test]
    public void Presentation_null_limit_is_unbounded_not_a_ceiling_of_zero()
    {
        var gauge = Gauge(usage: 3, limit: null);

        Assert.Multiple(() =>
        {
            Assert.That(gauge.Presentation, Is.EqualTo(TenantQuotaPresentation.UnboundedWithUsage));
            Assert.That(gauge.IsBounded, Is.False);
            Assert.That(gauge.HasBar, Is.False, "an unbounded dimension has nothing to be a proportion of");
            Assert.That(gauge.IsOverLimit, Is.False, "no ceiling means no breach");
        });
    }

    [Test]
    public void Presentation_null_usage_is_unmeasured_not_a_measured_zero()
    {
        var gauge = Gauge(usage: null, limit: 900);

        Assert.Multiple(() =>
        {
            Assert.That(gauge.Presentation, Is.EqualTo(TenantQuotaPresentation.UnmeasuredWithLimit));
            Assert.That(gauge.IsMeasured, Is.False);
            Assert.That(gauge.HasBar, Is.False, "an unmeasured dimension must not render an empty bar");
            Assert.That(gauge.IsOverLimit, Is.False);
        });
    }

    [Test]
    public void Presentation_neither_bounded_nor_measured_is_unknown()
    {
        var gauge = Gauge(usage: null, limit: null);

        Assert.Multiple(() =>
        {
            Assert.That(gauge.Presentation, Is.EqualTo(TenantQuotaPresentation.Unknown));
            Assert.That(gauge.HasBar, Is.False);
        });
    }

    [Test]
    public void A_ceiling_of_zero_is_a_real_cap_and_not_unbounded()
    {
        var gauge = Gauge(usage: 64, limit: 0);

        Assert.Multiple(() =>
        {
            Assert.That(gauge.IsBounded, Is.True, "zero is a cap permitting nothing, not the absence of one");
            Assert.That(gauge.Presentation, Is.EqualTo(TenantQuotaPresentation.Bar));
            Assert.That(gauge.BarPercent, Is.EqualTo(100), "every byte against a cap of nothing is overage");
            Assert.That(gauge.IsOverLimit, Is.True);
        });
    }

    [Test]
    public void A_ceiling_of_zero_with_no_usage_is_not_over_limit()
    {
        var gauge = Gauge(usage: 0, limit: 0);

        Assert.Multiple(() =>
        {
            Assert.That(gauge.BarPercent, Is.EqualTo(0));
            Assert.That(gauge.IsOverLimit, Is.False);
        });
    }

    [Test]
    public void An_unbounded_dimension_does_not_render_a_zero_percent_bar()
    {
        // The regression this whole type exists for: the seam's Utilization is
        // deliberately null when unbounded, so a renderer that coalesced it to
        // zero would draw an empty track for an unlimited quota - which reads as
        // "you have used none of your allowance" when there is no allowance.
        var unbounded = Gauge(usage: 5_000, limit: null);
        var empty = Gauge(usage: 0, limit: 1_000);

        Assert.Multiple(() =>
        {
            Assert.That(unbounded.BarPercent, Is.EqualTo(empty.BarPercent),
                "both report zero, which is exactly why BarPercent alone must never drive the render");
            Assert.That(unbounded.HasBar, Is.False);
            Assert.That(empty.HasBar, Is.True, "a measured zero against a real ceiling does render a bar");
        });
    }

    [Test]
    public void Bar_percent_is_clamped_to_one_hundred_when_over_the_ceiling() =>
        Assert.That(Gauge(usage: 5_000, limit: 1_000).BarPercent, Is.EqualTo(100));

    [Test]
    public void Bar_percent_rounds_the_measured_proportion() =>
        Assert.That(Gauge(usage: 255, limit: 1_000).BarPercent, Is.EqualTo(26));

    [Test]
    public void Bar_percent_is_zero_when_no_bar_may_be_drawn() =>
        Assert.That(Gauge(usage: null, limit: null).BarPercent, Is.EqualTo(0));

    [Test]
    public void Burst_headroom_is_the_gap_to_the_burst_ceiling() =>
        Assert.That(Gauge(usage: 250, limit: 1_000, burst: 1_100).BurstHeadroom, Is.EqualTo(850));

    [Test]
    public void Burst_headroom_is_null_when_the_burst_ceiling_is_unbounded() =>
        Assert.That(Gauge(usage: 250, limit: 1_000, burst: null).BurstHeadroom, Is.Null);

    [Test]
    public void Burst_headroom_is_null_when_the_dimension_is_unmeasured() =>
        Assert.That(Gauge(usage: null, limit: 900, burst: 990).BurstHeadroom, Is.Null);

    [Test]
    public void Burst_headroom_floors_at_zero_rather_than_reporting_a_debt() =>
        Assert.That(Gauge(usage: 2_000, limit: 1_000, burst: 1_100).BurstHeadroom, Is.EqualTo(0));

    [Test]
    public void Has_overage_only_where_an_overage_figure_is_meaningful()
    {
        var overrun = new TenantQuotaGauge(
            ExplorerTenantQuotaDimensionKind.MemoryBytes,
            new ExplorerTenantQuotaDimension
            {
                Usage = 64,
                Limit = 0,
                Overage = 64,
                MeteredOverage = 128,
            });

        var unmeasured = new TenantQuotaGauge(
            ExplorerTenantQuotaDimensionKind.OpsPerSecond,
            new ExplorerTenantQuotaDimension { Usage = null, Limit = 900, Overage = 5 });

        Assert.Multiple(() =>
        {
            Assert.That(overrun.HasOverage, Is.True);
            Assert.That(overrun.Overage, Is.EqualTo(64));
            Assert.That(overrun.MeteredOverage, Is.EqualTo(128));
            Assert.That(unmeasured.HasOverage, Is.False,
                "an overage figure carries no meaning on a dimension nothing is measuring");
        });
    }

    [Test]
    public void The_reading_is_carried_through_rather_than_copied_field_by_field()
    {
        var reading = new ExplorerTenantQuotaDimension { Usage = 7, Limit = 9, BurstLimit = 11 };
        var gauge = new TenantQuotaGauge(ExplorerTenantQuotaDimensionKind.Keys, reading);

        Assert.Multiple(() =>
        {
            Assert.That(gauge.Reading, Is.EqualTo(reading));
            Assert.That(gauge.Kind, Is.EqualTo(ExplorerTenantQuotaDimensionKind.Keys));
            Assert.That(gauge.Usage, Is.EqualTo(7));
            Assert.That(gauge.Limit, Is.EqualTo(9));
            Assert.That(gauge.BurstLimit, Is.EqualTo(11));
        });
    }

    [Test]
    public void Every_dimension_of_the_sample_reading_keeps_its_own_case()
    {
        var usage = MyTenantSample.Usage();

        Assert.Multiple(() =>
        {
            Assert.That(
                new TenantQuotaGauge(ExplorerTenantQuotaDimensionKind.Bytes, usage.Bytes).Presentation,
                Is.EqualTo(TenantQuotaPresentation.Bar));
            Assert.That(
                new TenantQuotaGauge(ExplorerTenantQuotaDimensionKind.Keys, usage.Keys).Presentation,
                Is.EqualTo(TenantQuotaPresentation.Bar));
            Assert.That(
                new TenantQuotaGauge(ExplorerTenantQuotaDimensionKind.MemoryBytes, usage.MemoryBytes)
                    .Presentation,
                Is.EqualTo(TenantQuotaPresentation.Bar));
            Assert.That(
                new TenantQuotaGauge(ExplorerTenantQuotaDimensionKind.TreeCount, usage.TreeCount)
                    .Presentation,
                Is.EqualTo(TenantQuotaPresentation.UnboundedWithUsage));
            Assert.That(
                new TenantQuotaGauge(ExplorerTenantQuotaDimensionKind.OpsPerSecond, usage.OpsPerSecond)
                    .Presentation,
                Is.EqualTo(TenantQuotaPresentation.UnmeasuredWithLimit));
        });
    }
}
