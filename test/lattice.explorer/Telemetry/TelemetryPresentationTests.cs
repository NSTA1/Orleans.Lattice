using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The legend, the tree filter's option list, the label vocabulary, and the
/// reading format - the small pure pieces a chart is captioned from.
/// </summary>
[TestFixture]
public sealed class TelemetryPresentationTests
{
    // ---- legend -------------------------------------------------------------

    [Test]
    public void A_series_is_named_by_its_tree_first_then_its_tenant()
    {
        var series = ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1);

        var label = TelemetrySeriesLegend.LabelFor(series, 0);

        Assert.That(label, Is.EqualTo("t/acme/orders / acme"));
    }

    [Test]
    public void The_platform_sentinel_reads_as_platform_rather_than_as_a_reserved_id()
    {
        var series = ExplorerTelemetrySample.Series("sys-audit", TelemetryLabelNames.PlatformTenant, 1);

        Assert.That(TelemetrySeriesLegend.LabelFor(series, 0), Does.Contain("platform"));
    }

    [Test]
    public void The_tenancy_off_default_reads_as_default()
    {
        var series = ExplorerTelemetrySample.Series("orders", TelemetryLabelNames.TenancyOffTenant, 1);

        Assert.That(TelemetrySeriesLegend.LabelFor(series, 0), Does.Contain("default"));
    }

    [Test]
    public void A_series_carrying_no_labels_is_named_by_its_position() =>
        Assert.That(
            TelemetrySeriesLegend.LabelFor(new ExplorerTelemetrySeries { Labels = [], Points = [] }, 2),
            Is.EqualTo("Series 3"));

    [Test]
    public void A_series_carrying_an_unrecognised_label_is_still_named_by_it()
    {
        // A catalogue entry that starts emitting a new dimension must be legible
        // without a client change.
        var series = new ExplorerTelemetrySeries
        {
            Labels = [new ExplorerTelemetryLabel("region", "westeurope")],
            Points = ExplorerTelemetrySample.Points(1),
        };

        Assert.That(TelemetrySeriesLegend.LabelFor(series, 0), Is.EqualTo("westeurope"));
    }

    [Test]
    public void A_label_is_never_repeated_when_it_is_also_a_preferred_one()
    {
        var series = ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1);

        Assert.That(
            TelemetrySeriesLegend.LabelFor(series, 0).Split(" / "),
            Has.Length.EqualTo(2));
    }

    [Test]
    public void A_null_series_is_rejected() =>
        Assert.That(() => TelemetrySeriesLegend.LabelFor(null!, 0), Throws.ArgumentNullException);

    // ---- tree options -------------------------------------------------------

    [Test]
    public void The_tree_options_are_the_distinct_trees_the_answer_carried_in_ordinal_order()
    {
        var result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("t/acme/orders", null, 1),
            ExplorerTelemetrySample.Series("t/acme/audit", null, 1),
            ExplorerTelemetrySample.Series("t/acme/orders", null, 2));

        Assert.That(TelemetryTreeOptions.For(result), Is.EqualTo(new[] { "t/acme/audit", "t/acme/orders" }));
    }

    [Test]
    public void A_result_with_no_tree_labels_offers_nothing_to_narrow_to() =>
        Assert.That(
            TelemetryTreeOptions.For(ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series(null, ExplorerTelemetrySample.TenantId, 1))),
            Is.Empty);

    [Test]
    public void A_null_result_offers_nothing() =>
        Assert.That(TelemetryTreeOptions.For(null), Is.Empty);

    [Test]
    public void An_unset_tree_filter_is_always_offered() =>
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryTreeOptions.IsOffered([], null), Is.True);
            Assert.That(TelemetryTreeOptions.IsOffered([], TelemetryTreeOptions.AllTreesValue), Is.True);
        });

    [Test]
    public void A_tree_the_answer_did_not_carry_is_not_offered() =>
        Assert.That(TelemetryTreeOptions.IsOffered(["a", "b"], "c"), Is.False);

    [Test]
    public void Asking_a_null_option_list_is_rejected() =>
        Assert.That(() => TelemetryTreeOptions.IsOffered(null!, "a"), Throws.ArgumentNullException);

    // ---- label vocabulary ---------------------------------------------------

    [Test]
    public void The_tenant_label_is_bound_to_the_constant_the_cluster_actually_emits() =>
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryLabelNames.Tenant, Is.EqualTo(LatticeTenantLabel.TagTenant));
            Assert.That(TelemetryLabelNames.PlatformTenant, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
            Assert.That(TelemetryLabelNames.TenancyOffTenant, Is.EqualTo(LatticeTenantLabel.DefaultTenant));
        });

    [Test]
    public void An_absent_tenant_label_reads_as_unattributed() =>
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryLabelNames.DisplayTenant(null), Is.EqualTo("unattributed"));
            Assert.That(TelemetryLabelNames.DisplayTenant(string.Empty), Is.EqualTo("unattributed"));
            Assert.That(TelemetryLabelNames.DisplayTenant("acme"), Is.EqualTo("acme"));
        });

    // ---- value format -------------------------------------------------------

    [Test]
    public void An_absent_reading_reads_as_no_reading_rather_than_as_zero() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                TelemetryValueFormat.Value(null, ExplorerTelemetrySemantic.PerOperation),
                Is.EqualTo(TelemetryValueFormat.NoReadingText));
            Assert.That(
                TelemetryValueFormat.Value(double.NaN, ExplorerTelemetrySemantic.PerOperation),
                Is.EqualTo(TelemetryValueFormat.NoReadingText));
            Assert.That(
                TelemetryValueFormat.Value(double.PositiveInfinity, ExplorerTelemetrySemantic.Level),
                Is.EqualTo(TelemetryValueFormat.NoReadingText));
        });

    [Test]
    public void A_duration_keeps_enough_precision_that_a_fast_operation_is_not_rounded_to_zero() =>
        Assert.That(
            TelemetryValueFormat.Value(0.004, ExplorerTelemetrySemantic.Duration),
            Does.Not.EqualTo("0"));

    [Test]
    public void A_small_count_keeps_its_fraction_and_a_large_one_is_whole() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                TelemetryValueFormat.Value(0.5, ExplorerTelemetrySemantic.PerOperation),
                Does.Contain("5"));
            Assert.That(
                TelemetryValueFormat.Value(1234, ExplorerTelemetrySemantic.PerOperation),
                Does.Not.Contain("."));
        });

    [Test]
    public void The_unit_appended_is_the_one_the_server_published() =>
        Assert.That(
            TelemetryValueFormat.WithUnit(12, ExplorerTelemetrySemantic.PerOperation, "ops/s"),
            Does.EndWith("ops/s"));

    [Test]
    public void An_entry_declaring_no_unit_gets_no_unit_appended() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                TelemetryValueFormat.WithUnit(12, ExplorerTelemetrySemantic.PerOperation, null),
                Does.Not.Contain(" "));
            Assert.That(
                TelemetryValueFormat.WithUnit(12, ExplorerTelemetrySemantic.PerOperation, "  "),
                Does.Not.Contain("  "));
        });

    [Test]
    public void An_absent_reading_is_never_given_a_unit() =>
        Assert.That(
            TelemetryValueFormat.WithUnit(null, ExplorerTelemetrySemantic.PerOperation, "ops/s"),
            Is.EqualTo(TelemetryValueFormat.NoReadingText));
}
