using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Pins <see cref="PrometheusExporterNaming"/> to the rules of
/// <c>OpenTelemetry.Exporter.Prometheus</c> 1.15.3 (<c>PrometheusMetric</c>), and to
/// the series names measured on a live <c>.AddPrometheusExporter()</c> scrape.
/// </summary>
[TestFixture]
public sealed class PrometheusExporterNamingTests
{
    [TestCase("ms", "milliseconds")]
    [TestCase("s", "seconds")]
    [TestCase("By", "bytes")]
    [TestCase("KiBy", "kibibytes")]
    [TestCase("%", "percent")]
    [TestCase("1", "")]
    [TestCase("{entry}", "")]
    [TestCase("", "")]
    [TestCase(null, "")]
    [TestCase("{op}/s", "per_second")]
    [TestCase("By/s", "bytes_per_second")]
    [TestCase("widgets", "widgets")]
    [TestCase("s/", "s")]
    public void UnitWord_maps_the_declared_unit_as_the_exporter_does(string? unit, string expected)
    {
        Assert.That(PrometheusExporterNaming.UnitWord(unit), Is.EqualTo(expected));
    }

    // Measured on a live 1.15.3-beta.1 scrape (gotchas/prometheus-unit-suffix-doubling-2920
    // and gotchas/prometheus-exporter-bytes-suffix-not-doubled).
    [TestCase("probe.control.payload", "By", PrometheusFamilyType.Counter, "probe_control_payload_bytes_total")]
    [TestCase("orleans.lattice.storage.wal.stored_bytes", "By", PrometheusFamilyType.Counter, "orleans_lattice_storage_wal_stored_bytes_total")]
    [TestCase("orleans.lattice.leaf.residency.budget_bytes", "By", PrometheusFamilyType.Gauge, "orleans_lattice_leaf_residency_budget_bytes")]
    [TestCase("probe.control.gauge_size", "By", PrometheusFamilyType.Gauge, "probe_control_gauge_size_bytes")]
    [TestCase("probe.control.plain", "{thing}", PrometheusFamilyType.Counter, "probe_control_plain_total")]
    [TestCase("probe.apply.dependency_wait_ms", "ms", PrometheusFamilyType.Histogram, "probe_apply_dependency_wait_ms_milliseconds")]
    [TestCase("probe.apply.parked_milliseconds", "ms", PrometheusFamilyType.Histogram, "probe_apply_parked_milliseconds")]
    [TestCase("probe.leaf.tombstone", "1", PrometheusFamilyType.Histogram, "probe_leaf_tombstone")]
    [TestCase("orleans.lattice.grainindex.backfill.percent_complete", "%", PrometheusFamilyType.Gauge, "orleans_lattice_grainindex_backfill_percent_complete_percent")]
    [TestCase("already.total", "", PrometheusFamilyType.Counter, "already_total")]
    [TestCase("9lives.count", "", PrometheusFamilyType.Gauge, "_lives_count")]
    [TestCase("a..b-c", "", PrometheusFamilyType.Gauge, "a_b_c")]
    public void FamilyName_follows_the_exporter_rule(string name, string unit, PrometheusFamilyType type, string expected)
    {
        Assert.That(PrometheusExporterNaming.FamilyName(name, unit, type), Is.EqualTo(expected));
    }

    [Test]
    public void FamilyName_rejects_a_null_name()
    {
        Assert.Throws<ArgumentNullException>(() => PrometheusExporterNaming.FamilyName(null!, "ms", PrometheusFamilyType.Gauge));
    }

    [Test]
    public void SeriesNames_of_a_histogram_are_bucket_count_and_sum_and_never_the_bare_family()
    {
        Assert.That(
            PrometheusExporterNaming.SeriesNames("orleans.lattice.atomic_write.duration", "ms", PrometheusFamilyType.Histogram),
            Is.EqualTo(new[]
            {
                "orleans_lattice_atomic_write_duration_milliseconds_bucket",
                "orleans_lattice_atomic_write_duration_milliseconds_count",
                "orleans_lattice_atomic_write_duration_milliseconds_sum",
            }));
    }

    [TestCase(PrometheusFamilyType.Counter, "x_seconds_total")]
    [TestCase(PrometheusFamilyType.Gauge, "x_seconds")]
    public void SeriesNames_of_a_counter_or_gauge_is_the_family_alone(PrometheusFamilyType type, string expected)
    {
        Assert.That(PrometheusExporterNaming.SeriesNames("x", "s", type), Is.EqualTo(new[] { expected }));
    }

    [TestCase(DeclaredInstrumentKind.Counter, PrometheusFamilyType.Counter)]
    [TestCase(DeclaredInstrumentKind.ObservableCounter, PrometheusFamilyType.Counter)]
    [TestCase(DeclaredInstrumentKind.Histogram, PrometheusFamilyType.Histogram)]
    [TestCase(DeclaredInstrumentKind.UpDownCounter, PrometheusFamilyType.Gauge)]
    [TestCase(DeclaredInstrumentKind.ObservableGauge, PrometheusFamilyType.Gauge)]
    [TestCase(DeclaredInstrumentKind.ObservableUpDownCounter, PrometheusFamilyType.Gauge)]
    public void FamilyTypeOf_a_declared_kind_matches_the_exporter_type_mapping(DeclaredInstrumentKind kind, PrometheusFamilyType expected)
    {
        Assert.That(PrometheusExporterNaming.FamilyTypeOf(kind), Is.EqualTo(expected));
    }

    [Test]
    public void FamilyTypeOf_an_undefined_kind_throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => PrometheusExporterNaming.FamilyTypeOf((DeclaredInstrumentKind)99));
    }

    [Test]
    public void FamilyTypeOf_a_live_instrument_matches_its_declared_kind()
    {
        using var meter = new Meter("prometheus.exporter.naming.tests");

        Assert.Multiple(() =>
        {
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateCounter<long>("c")), Is.EqualTo(PrometheusFamilyType.Counter));
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateObservableCounter("oc", static () => 1L)), Is.EqualTo(PrometheusFamilyType.Counter));
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateHistogram<double>("h")), Is.EqualTo(PrometheusFamilyType.Histogram));
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateUpDownCounter<long>("u")), Is.EqualTo(PrometheusFamilyType.Gauge));
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateObservableGauge("g", static () => 1.0)), Is.EqualTo(PrometheusFamilyType.Gauge));
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateObservableUpDownCounter("ou", static () => 1L)), Is.EqualTo(PrometheusFamilyType.Gauge));
            Assert.That(PrometheusExporterNaming.FamilyTypeOf(meter.CreateGauge<int>("sg")), Is.EqualTo(PrometheusFamilyType.Gauge));
        });
    }

    [Test]
    public void FamilyTypeOf_a_null_instrument_throws()
    {
        Assert.Throws<ArgumentNullException>(() => PrometheusExporterNaming.FamilyTypeOf((Instrument)null!));
    }

    /// <summary>
    /// The registry's unit reader places a positional unit by factory: the second
    /// argument of a synchronous instrument and the third of an observable one,
    /// whose second argument is its callback. Reading the second argument of an
    /// observable reported <c>%</c> on this gauge as no unit at all (issue #3260).
    /// </summary>
    [Test]
    public void DeclaredInstruments_reads_the_positional_unit_of_an_observable_gauge()
    {
        Assert.That(
            DeclaredInstruments.UnitByDottedName.GetValueOrDefault("orleans.lattice.grainindex.backfill.percent_complete"),
            Is.EqualTo("%"));
    }
}
