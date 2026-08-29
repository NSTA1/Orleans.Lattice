namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the label-value escaper: the single seam through which a caller-supplied
/// value (the optional tree filter) becomes part of a rendered query, and therefore
/// the only place a query-injection attempt could land.
/// </summary>
[TestFixture]
public sealed class PromQlLabelValueTests
{
    [Test]
    public void Escape_returns_the_same_reference_when_nothing_needs_escaping()
    {
        const string value = "t/acme/orders";

        Assert.That(PromQlLabelValue.Escape(value), Is.SameAs(value),
            "The ordinary path must allocate nothing.");
    }

    [TestCase("""a"b""", """a\"b""")]
    [TestCase(@"a\b", @"a\\b")]
    [TestCase("""a"b\c""", """a\"b\\c""")]
    [TestCase("", "")]
    public void Escape_escapes_the_two_characters_a_promql_literal_reserves(string input, string expected)
    {
        Assert.That(PromQlLabelValue.Escape(input), Is.EqualTo(expected));
    }

    [Test]
    public void Escape_neutralises_an_attempt_to_close_the_matcher_and_append_syntax()
    {
        // The classic injection shape: close the quote, close the brace, and start a
        // new selector. After escaping, every one of those characters is inert text
        // inside the label value.
        const string hostile = """acme"} or up{job="admin""";

        var escaped = PromQlLabelValue.Escape(hostile);

        Assert.Multiple(() =>
        {
            Assert.That(escaped, Is.EqualTo("""acme\"} or up{job=\"admin"""));
            Assert.That(escaped, Does.Not.Match(@"(?<!\\)"""),
                "No unescaped quote may survive, because one would terminate the matcher early.");
        });
    }

    [Test]
    public void Escaped_value_renders_as_a_single_intact_matcher()
    {
        var template = TelemetryQueryTemplate.Parse("metric{$scope$}");
        var escaped = PromQlLabelValue.Escape("""x" or up{a="b""");

        var rendered = template.Render(TelemetryScopeSelector.ForTenant("acme", escaped), "5m");

        Assert.That(rendered, Is.EqualTo(
            """metric{tenant="acme",tree="x\" or up{a=\"b",}"""),
            "The hostile text stays inside one quoted label value, so it selects nothing rather "
            + "than becoming query syntax.");
    }

    [TestCase("t/acme/orders", true)]
    [TestCase("plain", true)]
    [TestCase("""with"quote""", true)]
    [TestCase("", true)]
    [TestCase("with\nnewline", false)]
    [TestCase("with\ttab", false)]
    [TestCase("with\0nul", false)]
    public void Is_renderable_rejects_only_control_characters(string value, bool expected)
    {
        Assert.That(PromQlLabelValue.IsRenderable(value), Is.EqualTo(expected));
    }
}
