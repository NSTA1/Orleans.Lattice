namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="PromQlMetricExtractor"/>: it recognises identifiers in
/// metric-name position, skips function and aggregation calls, label matchers,
/// keywords, strings, and duration literals, and reports each referenced metric
/// once. The documented conservatism gaps (a <c>__name__</c>-only selector) are
/// asserted too.
/// </summary>
[TestFixture]
public sealed class PromQlMetricExtractorTests
{
    [Test]
    public void A_bare_metric_name_is_extracted()
        => Assert.That(PromQlMetricExtractor.Extract("up"), Is.EqualTo(new[] { "up" }));

    [Test]
    public void A_metric_with_a_label_matcher_is_extracted_without_the_label_names()
    {
        var names = PromQlMetricExtractor.Extract("http_requests_total{job=\"api\",code=\"200\"}");
        Assert.That(names, Is.EqualTo(new[] { "http_requests_total" }));
    }

    [Test]
    public void A_function_call_wrapping_a_metric_extracts_only_the_metric()
    {
        var names = PromQlMetricExtractor.Extract("rate(lattice_wal_append_total[5m])");
        Assert.That(names, Is.EqualTo(new[] { "lattice_wal_append_total" }));
    }

    [Test]
    public void An_aggregation_with_a_modifier_extracts_only_the_metric()
    {
        var names = PromQlMetricExtractor.Extract("sum by (instance) (rate(node_cpu_seconds_total[1m]))");
        Assert.That(names, Is.EqualTo(new[] { "node_cpu_seconds_total" }));
    }

    [Test]
    public void Multiple_distinct_metrics_are_each_extracted_once()
    {
        var names = PromQlMetricExtractor.Extract("up + up + process_start_time_seconds");
        Assert.That(names, Is.EqualTo(new[] { "up", "process_start_time_seconds" }));
    }

    [Test]
    public void A_duration_literal_is_not_mistaken_for_a_metric()
    {
        var names = PromQlMetricExtractor.Extract("increase(errors_total[10m] offset 1h)");
        Assert.That(names, Is.EqualTo(new[] { "errors_total" }));
    }

    [Test]
    public void An_empty_query_extracts_nothing()
        => Assert.That(PromQlMetricExtractor.Extract(string.Empty), Is.Empty);

    [Test]
    public void A_name_only_present_inside_a_string_is_not_extracted()
    {
        // A __name__-only selector references its metric through a label value
        // string, which the conservative extractor deliberately does not surface.
        var names = PromQlMetricExtractor.Extract("{__name__=\"up\"}");
        Assert.That(names, Is.Empty);
    }

    [Test]
    public void A_null_query_is_rejected()
        => Assert.Throws<ArgumentNullException>(() => PromQlMetricExtractor.Extract(query: null!));
}
