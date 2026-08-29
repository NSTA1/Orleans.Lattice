namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="PromQlMetricExtractor"/>: it recognises identifiers in
/// metric-name position, skips function and aggregation calls, label matchers,
/// keywords, strings, and duration literals, and reports each referenced metric
/// once. It also surfaces the reserved <c>__name__</c> label matcher: an exact
/// matcher contributes its value as a name, while a regex or negative matcher sets
/// the unresolvable flag the deny-all gate rejects on.
/// </summary>
[TestFixture]
public sealed class PromQlMetricExtractorTests
{
    private static IReadOnlyList<string> Names(string query)
        => PromQlMetricExtractor.ExtractReferences(query).Names;

    [Test]
    public void A_bare_metric_name_is_extracted()
        => Assert.That(Names("up"), Is.EqualTo(new[] { "up" }));

    [Test]
    public void A_metric_with_a_label_matcher_is_extracted_without_the_label_names()
    {
        var names = Names("http_requests_total{job=\"api\",code=\"200\"}");
        Assert.That(names, Is.EqualTo(new[] { "http_requests_total" }));
    }

    [Test]
    public void A_function_call_wrapping_a_metric_extracts_only_the_metric()
    {
        var names = Names("rate(lattice_wal_append_total[5m])");
        Assert.That(names, Is.EqualTo(new[] { "lattice_wal_append_total" }));
    }

    [Test]
    public void An_aggregation_with_a_modifier_extracts_only_the_metric()
    {
        var names = Names("sum by (instance) (rate(node_cpu_seconds_total[1m]))");
        Assert.That(names, Is.EqualTo(new[] { "node_cpu_seconds_total" }));
    }

    [Test]
    public void Multiple_distinct_metrics_are_each_extracted_once()
    {
        var names = Names("up + up + process_start_time_seconds");
        Assert.That(names, Is.EqualTo(new[] { "up", "process_start_time_seconds" }));
    }

    [Test]
    public void A_duration_literal_is_not_mistaken_for_a_metric()
    {
        var names = Names("increase(errors_total[10m] offset 1h)");
        Assert.That(names, Is.EqualTo(new[] { "errors_total" }));
    }

    [Test]
    public void An_empty_query_extracts_nothing()
    {
        var references = PromQlMetricExtractor.ExtractReferences(string.Empty);
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void An_exact_name_matcher_contributes_its_value_as_a_name()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"up\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void An_exact_name_matcher_with_surrounding_whitespace_contributes_its_value()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{ __name__ = \"up\" , job=\"api\" }");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_regex_name_matcher_is_unresolvable()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=~\"secret_.*\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void A_negative_exact_name_matcher_is_unresolvable()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__!=\"up\"}");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_negative_regex_name_matcher_is_unresolvable()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__!~\"up\"}");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_label_only_selector_names_no_metric()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{job=\"api\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void A_bare_label_selector_is_flagged_as_unconstrained()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{job=\"api\"}");
        Assert.That(references.HasUnconstrainedSelector, Is.True);
    }

    [Test]
    public void An_admitted_metric_ORed_with_a_bare_selector_is_flagged_as_unconstrained()
    {
        // Regression: the extracted name set is non-empty (["up"]) yet the
        // right-hand bare selector is unanchored, so the unconstrained flag must
        // be set so the deny-all gate can fail closed.
        var references = PromQlMetricExtractor.ExtractReferences("up or {job=\"api\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
            Assert.That(references.HasUnconstrainedSelector, Is.True);
        });
    }

    [Test]
    public void A_metric_anchored_label_selector_is_not_flagged_as_unconstrained()
    {
        var references = PromQlMetricExtractor.ExtractReferences("http_requests_total{job=\"api\",code=\"200\"}");
        Assert.That(references.HasUnconstrainedSelector, Is.False);
    }

    [Test]
    public void An_exact_name_matcher_selector_is_not_flagged_as_unconstrained()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"up\",job=\"api\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void An_operator_separated_bare_selector_is_flagged_even_after_a_metric()
    {
        // The '+' breaks the adjacency between the metric name and the selector,
        // so the selector is unanchored and must be flagged.
        var references = PromQlMetricExtractor.ExtractReferences("up + {job=\"api\"}");
        Assert.That(references.HasUnconstrainedSelector, Is.True);
    }

    [Test]
    public void An_unterminated_name_matcher_value_is_unresolvable()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"up}");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_name_position_metric_with_a_name_matcher_extracts_both_names()
    {
        var references = PromQlMetricExtractor.ExtractReferences("up{__name__=\"down\"}");
        Assert.That(references.Names, Is.EqualTo(new[] { "up", "down" }));
    }

    [Test]
    public void A_null_query_is_rejected()
        => Assert.Throws<ArgumentNullException>(() => PromQlMetricExtractor.ExtractReferences(query: null!));
}
