namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

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
    public void An_exact_name_matcher_unescapes_an_escaped_quote()
    {
        // PromQL {__name__="a\"b"} designates the metric named a"b. The extractor
        // must unescape the value so it matches an allow-listed UTF-8 metric name;
        // the pre-fix code returned the raw span a\"b (backslash retained), which
        // never matched the allow-list and wrongly denied a legitimate query.
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"a\\\"b\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "a\"b" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void An_exact_name_matcher_unescapes_an_escaped_backslash()
    {
        // {__name__="a\\b"} designates the metric a\b (one backslash).
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"a\\\\b\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "a\\b" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void An_exact_single_quoted_name_matcher_unescapes_an_escaped_quote()
    {
        // {__name__='a\'b'} designates the metric a'b.
        var references = PromQlMetricExtractor.ExtractReferences("{__name__='a\\'b'}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "a'b" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void An_exact_name_matcher_with_an_unmodeled_escape_is_unresolvable()
    {
        // A \n (like \x, \u, \U, and octal) escape is not modelled by the
        // conservative extractor. Rather than resolve to a name that could diverge
        // from Prometheus's own unescaping, it leaves the value unresolved so the
        // deny-all gate fails closed.
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"a\\nb\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void An_exact_name_matcher_in_a_raw_backtick_string_keeps_backslashes_literal()
    {
        // Backtick strings are raw in PromQL: `a\b` is the literal name a\b, with no
        // escape processing, so the backslash is preserved verbatim.
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=`a\\b`}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "a\\b" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
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
    public void A_comment_cannot_hide_a_metric_name_on_a_later_line()
    {
        // Security regression: a '#' comment runs to end-of-line, so the quote it
        // contains opens no string literal. Before the fix the scanner treated it
        // as a string opener and swallowed the whole next line up to the second
        // quote, so 'secret_metric' never reached the deny-all gate while
        // Prometheus - which strips comments before parsing - still evaluated it.
        var references = PromQlMetricExtractor.ExtractReferences("up or #\"\nsecret_metric #\"");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up", "secret_metric" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void A_comment_cannot_hide_a_bare_selector_on_a_later_line()
    {
        // The same swallowing trick applied to an unanchored label selector: the
        // unconstrained flag must still be raised so the gate fails closed.
        var references = PromQlMetricExtractor.ExtractReferences("up or #\"\n{job=\"api\"} #\"");
        Assert.That(references.HasUnconstrainedSelector, Is.True);
    }

    [Test]
    public void A_comment_cannot_hide_a_name_matcher_on_a_later_line()
    {
        var references = PromQlMetricExtractor.ExtractReferences("up or #'\n{__name__=~\"secret.*\"} #'");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_trailing_comment_contributes_no_metric_name()
    {
        // Prometheus discards the comment, so its text must not be extracted as a
        // referenced name either - extracting it would wrongly deny the query.
        var references = PromQlMetricExtractor.ExtractReferences("up # secret_metric");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_comment_does_not_break_metric_selector_adjacency()
    {
        // A comment is discarded like whitespace, so the selector stays anchored to
        // the metric name (which was itself allow-list checked when extracted).
        var references = PromQlMetricExtractor.ExtractReferences("up #c\n{job=\"api\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void A_hash_inside_a_string_literal_is_not_a_comment()
    {
        var references = PromQlMetricExtractor.ExtractReferences("{__name__=\"a#b\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "a#b" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void A_comment_terminated_by_a_carriage_return_ends_at_the_line_break()
    {
        var references = PromQlMetricExtractor.ExtractReferences("up or #\"\r\nsecret_metric");
        Assert.That(references.Names, Is.EqualTo(new[] { "up", "secret_metric" }));
    }

    [Test]
    public void A_null_query_is_rejected()
        => Assert.Throws<ArgumentNullException>(() => PromQlMetricExtractor.ExtractReferences(query: null!));
}
