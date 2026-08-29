using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The quota display row: the four reading states, and the guarantee that
/// neither absence a quota reading can carry is ever flattened into a zero.
/// <para>
/// This is the fixture that stops the surface drawing a bar that lies. An
/// unbounded dimension must not render as a full bar, and an unmeasured one must
/// not render as an empty one.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantQuotaRowTests
{
    private static TenantQuotaRow Row(
        long? usage,
        long? limit,
        long? burst = null,
        long overage = 0,
        long meteredOverage = 0,
        ExplorerTenantQuotaDimensionKind kind = ExplorerTenantQuotaDimensionKind.Keys) =>
        TenantQuotaRow.From(kind, new ExplorerTenantQuotaDimension
        {
            Usage = usage,
            Limit = limit,
            BurstLimit = burst,
            Overage = overage,
            MeteredOverage = meteredOverage,
        });

    [Test]
    public void A_bounded_measured_dimension_is_bounded_and_draws_a_bar()
    {
        var row = Row(usage: 250, limit: 1_000, burst: 1_100);

        Assert.Multiple(() =>
        {
            Assert.That(row.State, Is.EqualTo(TenantQuotaReadingState.Bounded));
            Assert.That(row.UsageText, Is.EqualTo("250"));
            Assert.That(row.LimitText, Is.EqualTo("1,000"));
            Assert.That(row.BurstLimitText, Is.EqualTo("1,100"));
            Assert.That(row.ShowsBar, Is.True);
            Assert.That(row.Utilization, Is.EqualTo(0.25d));
            Assert.That(row.BarPercent, Is.EqualTo(25));
        });
    }

    [Test]
    public void An_unbounded_dimension_says_unlimited_and_draws_no_bar()
    {
        var row = Row(usage: 3, limit: null);

        Assert.Multiple(() =>
        {
            Assert.That(row.State, Is.EqualTo(TenantQuotaReadingState.Unlimited));
            Assert.That(row.LimitText, Is.EqualTo(TenantQuotaFormat.UnlimitedText));
            Assert.That(row.UsageText, Is.EqualTo("3"));

            // The whole point: an unlimited tenant must not render as a full bar,
            // and there is no honest fraction to draw.
            Assert.That(row.ShowsBar, Is.False);
            Assert.That(row.Utilization, Is.Null);
            Assert.That(row.BarPercent, Is.Zero);
        });
    }

    [Test]
    public void An_unmeasured_dimension_says_not_measured_and_draws_no_bar()
    {
        var row = Row(usage: null, limit: 900, burst: 990);

        Assert.Multiple(() =>
        {
            Assert.That(row.State, Is.EqualTo(TenantQuotaReadingState.NotMeasured));
            Assert.That(row.UsageText, Is.EqualTo(TenantQuotaFormat.NotMeasuredText));
            Assert.That(row.LimitText, Is.EqualTo("900"));

            // An empty bar would read as "you are using none of your rate limit"
            // when the truth is that nothing is being measured.
            Assert.That(row.ShowsBar, Is.False);
            Assert.That(row.Utilization, Is.Null);
        });
    }

    [Test]
    public void A_dimension_with_neither_a_ceiling_nor_a_sample_is_unknown()
    {
        var row = Row(usage: null, limit: null);

        Assert.Multiple(() =>
        {
            // The double absence must not fall through into either single-absence
            // state, or the surface would assert something nobody measured.
            Assert.That(row.State, Is.EqualTo(TenantQuotaReadingState.Unknown));
            Assert.That(row.UsageText, Is.EqualTo(TenantQuotaFormat.NotMeasuredText));
            Assert.That(row.LimitText, Is.EqualTo(TenantQuotaFormat.UnlimitedText));
            Assert.That(row.ShowsBar, Is.False);
        });
    }

    [Test]
    public void The_four_reading_states_are_all_distinct()
    {
        var states = new[]
        {
            Row(usage: 1, limit: 1).State,
            Row(usage: 1, limit: null).State,
            Row(usage: null, limit: 1).State,
            Row(usage: null, limit: null).State,
        };

        Assert.That(states, Is.Unique);
    }

    [Test]
    public void A_ceiling_of_zero_is_a_real_cap_and_not_unbounded()
    {
        var row = Row(usage: 64, limit: 0, burst: 0, overage: 64, meteredOverage: 128);

        Assert.Multiple(() =>
        {
            Assert.That(row.State, Is.EqualTo(TenantQuotaReadingState.Bounded));
            Assert.That(row.LimitText, Is.EqualTo("0"));
            Assert.That(row.LimitText, Is.Not.EqualTo(TenantQuotaFormat.UnlimitedText));

            // A cap of nothing with any usage at all is fully consumed, and the
            // overage carries the real excess.
            Assert.That(row.Utilization, Is.EqualTo(1d));
            Assert.That(row.IsOverLimit, Is.True);
            Assert.That(row.OverageText, Is.EqualTo("64"));
            Assert.That(row.MeteredOverageText, Is.EqualTo("128"));
        });
    }

    [Test]
    public void A_ceiling_of_zero_with_no_usage_is_not_over_limit()
    {
        var row = Row(usage: 0, limit: 0);

        Assert.Multiple(() =>
        {
            Assert.That(row.Utilization, Is.EqualTo(0d));
            Assert.That(row.IsOverLimit, Is.False);
            Assert.That(row.BarPercent, Is.Zero);
        });
    }

    [Test]
    public void A_measured_zero_is_not_the_same_row_as_an_unmeasured_dimension()
    {
        var measuredZero = Row(usage: 0, limit: 500);
        var unmeasured = Row(usage: null, limit: 500);

        Assert.Multiple(() =>
        {
            Assert.That(measuredZero.UsageText, Is.EqualTo("0"));
            Assert.That(unmeasured.UsageText, Is.EqualTo(TenantQuotaFormat.NotMeasuredText));
            Assert.That(measuredZero.State, Is.Not.EqualTo(unmeasured.State));
            Assert.That(measuredZero.ShowsBar, Is.True);
            Assert.That(unmeasured.ShowsBar, Is.False);
        });
    }

    [Test]
    public void A_breach_clamps_the_bar_to_the_track_while_reporting_the_real_fraction()
    {
        var row = Row(usage: 3_000, limit: 1_000, overage: 2_000);

        Assert.Multiple(() =>
        {
            Assert.That(row.IsOverLimit, Is.True);
            Assert.That(row.Utilization, Is.EqualTo(3d));
            Assert.That(row.BarPercent, Is.EqualTo(100));
            Assert.That(row.OverageText, Is.EqualTo("2,000"));
        });
    }

    [Test]
    public void Overage_carries_no_meaning_on_a_dimension_that_is_not_bounded_and_measured()
    {
        var unbounded = Row(usage: 5, limit: null, overage: 99, meteredOverage: 99);
        var unmeasured = Row(usage: null, limit: 5, overage: 99, meteredOverage: 99);

        Assert.Multiple(() =>
        {
            Assert.That(unbounded.OverageText, Is.Empty);
            Assert.That(unbounded.MeteredOverageText, Is.Empty);
            Assert.That(unmeasured.OverageText, Is.Empty);
            Assert.That(unmeasured.MeteredOverageText, Is.Empty);
        });
    }

    [Test]
    public void A_byte_dimension_formats_its_figures_in_binary_units()
    {
        var row = Row(
            usage: 2048,
            limit: 4096,
            burst: 8192,
            kind: ExplorerTenantQuotaDimensionKind.Bytes);

        Assert.Multiple(() =>
        {
            Assert.That(row.Label, Is.EqualTo("Stored bytes"));
            Assert.That(row.UsageText, Is.EqualTo("2 KB"));
            Assert.That(row.LimitText, Is.EqualTo("4 KB"));
            Assert.That(row.BurstLimitText, Is.EqualTo("8 KB"));
        });
    }

    [Test]
    public void An_unbounded_burst_ceiling_says_unlimited_rather_than_zero()
    {
        var row = Row(usage: 3, limit: null, burst: null);

        Assert.That(row.BurstLimitText, Is.EqualTo(TenantQuotaFormat.UnlimitedText));
    }

    [Test]
    public void Every_dimension_of_the_sample_reading_projects_without_flattening_anything()
    {
        var usage = SampleTenants.Usage();

        var rows = ExplorerTenantQuotaUsage.Dimensions
            .Select(kind => TenantQuotaRow.From(kind, usage[kind]))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(rows[0].State, Is.EqualTo(TenantQuotaReadingState.Bounded), "bytes");
            Assert.That(rows[1].State, Is.EqualTo(TenantQuotaReadingState.Bounded), "keys, measured at zero");
            Assert.That(rows[2].State, Is.EqualTo(TenantQuotaReadingState.Bounded), "memory, capped at zero");
            Assert.That(rows[3].State, Is.EqualTo(TenantQuotaReadingState.Unlimited), "trees");
            Assert.That(rows[4].State, Is.EqualTo(TenantQuotaReadingState.NotMeasured), "operation rate");
        });
    }

    [Test]
    public void From_undefined_dimension_throws()
    {
        Assert.That(
            () => TenantQuotaRow.From((ExplorerTenantQuotaDimensionKind)42, default),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
