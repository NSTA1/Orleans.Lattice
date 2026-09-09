using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextPrometheusExposition"/>. The name mapping
/// is pinned deliberately: an exposed series name is what a scrape configuration
/// and every dashboard panel bind to, so a change here is a breaking change for a
/// consumer that this project cannot see.
/// </summary>
[TestFixture]
public sealed class RepoContextPrometheusExpositionTests
{
    [Test]
    public void Content_type_declares_the_text_exposition_version()
        => Assert.That(RepoContextPrometheusExposition.ContentType,
            Is.EqualTo("text/plain; version=0.0.4; charset=utf-8"));

    [TestCase("repocontext.calls", RepoContextMetricKind.Counter, "repocontext_calls_total")]
    [TestCase("repocontext.ann.sweep", RepoContextMetricKind.Counter, "repocontext_ann_sweep_total")]
    [TestCase("repocontext.retrieval.ann.search", RepoContextMetricKind.Counter, "repocontext_retrieval_ann_search_total")]
    [TestCase("repocontext.retrieval.ready_seconds", RepoContextMetricKind.Summary, "repocontext_retrieval_ready_seconds")]
    [TestCase("orleans.lattice.leaf.activation.failures", RepoContextMetricKind.Counter, "orleans_lattice_leaf_activation_failures_total")]
    [TestCase("queue.depth", RepoContextMetricKind.Gauge, "queue_depth")]
    public void Metric_names_map_to_the_expected_exposed_name(
        string instrumentName, RepoContextMetricKind kind, string expected)
        => Assert.That(RepoContextPrometheusExposition.MetricName(instrumentName, kind), Is.EqualTo(expected));

    [Test]
    public void A_counter_already_carrying_the_total_suffix_is_not_suffixed_twice()
        => Assert.That(
            RepoContextPrometheusExposition.MetricName("widgets_total", RepoContextMetricKind.Counter),
            Is.EqualTo("widgets_total"));

    [Test]
    public void Metric_name_rejects_null()
        => Assert.Throws<ArgumentNullException>(
            () => RepoContextPrometheusExposition.MetricName(null!, RepoContextMetricKind.Counter));

    [TestCase("a.b-c/d", "a_b_c_d")]
    [TestCase("9lives", "_9lives")]
    [TestCase("", "_")]
    [TestCase("ns:metric", "ns:metric")]
    public void Metric_names_are_sanitized_to_the_prometheus_grammar(string raw, string expected)
        => Assert.That(RepoContextPrometheusExposition.SanitizeMetricName(raw), Is.EqualTo(expected));

    [TestCase("tree.id", "tree_id")]
    [TestCase("9x", "_9x")]
    [TestCase("", "_")]
    [TestCase("ns:label", "ns_label")]
    public void Label_names_are_sanitized_and_may_not_carry_a_colon(string raw, string expected)
        => Assert.That(RepoContextPrometheusExposition.SanitizeLabelName(raw), Is.EqualTo(expected));

    [Test]
    public void Sanitizers_reject_null()
        => Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => RepoContextPrometheusExposition.SanitizeMetricName(null!));
            Assert.Throws<ArgumentNullException>(() => RepoContextPrometheusExposition.SanitizeLabelName(null!));
        });

    [TestCase("plain", "plain")]
    [TestCase("with \"quotes\"", "with \\\"quotes\\\"")]
    [TestCase("back\\slash", "back\\\\slash")]
    [TestCase("two\nlines", "two\\nlines")]
    [TestCase(null, "")]
    public void Label_values_escape_the_three_reserved_characters(string? raw, string expected)
        => Assert.That(RepoContextPrometheusExposition.EscapeLabelValue(raw), Is.EqualTo(expected));

    [Test]
    public void Help_text_escapes_backslash_and_newline_but_not_the_quote()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextPrometheusExposition.EscapeHelp("a\\b\nc"), Is.EqualTo("a\\\\b\\nc"));
            Assert.That(RepoContextPrometheusExposition.EscapeHelp("say \"hi\""), Is.EqualTo("say \"hi\""));
            Assert.That(RepoContextPrometheusExposition.EscapeHelp(null), Is.Empty);
        });

    [Test]
    public void Values_format_invariantly_with_the_prometheus_spellings_for_non_finite()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextPrometheusExposition.FormatValue(4d), Is.EqualTo("4"));
            Assert.That(RepoContextPrometheusExposition.FormatValue(1.5d), Is.EqualTo("1.5"));
            Assert.That(RepoContextPrometheusExposition.FormatValue(double.NaN), Is.EqualTo("NaN"));
            Assert.That(RepoContextPrometheusExposition.FormatValue(double.PositiveInfinity), Is.EqualTo("+Inf"));
            Assert.That(RepoContextPrometheusExposition.FormatValue(double.NegativeInfinity), Is.EqualTo("-Inf"));
        });

    [Test]
    public void Type_keywords_cover_every_metric_kind()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextPrometheusExposition.TypeKeyword(RepoContextMetricKind.Counter), Is.EqualTo("counter"));
            Assert.That(RepoContextPrometheusExposition.TypeKeyword(RepoContextMetricKind.Gauge), Is.EqualTo("gauge"));
            Assert.That(RepoContextPrometheusExposition.TypeKeyword(RepoContextMetricKind.Summary), Is.EqualTo("summary"));
            Assert.That(Enum.GetValues<RepoContextMetricKind>(), Has.Length.EqualTo(3),
                "a new metric kind needs a type keyword and a rendering rule");
        });
}
