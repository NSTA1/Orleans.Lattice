namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Malformed and degenerate PromQL inputs for <see cref="PromQlMetricExtractor"/>.
/// Every arm exercised here is a fail-closed one: a construct the conservative
/// extractor cannot reduce to a fixed set of metric names must raise
/// <see cref="PromQlMetricReferences.HasUnresolvableNameMatcher"/> (or
/// <see cref="PromQlMetricReferences.HasUnconstrainedSelector"/>) so the deny-all
/// gate rejects the query, rather than returning an empty name list that the gate
/// would read as "this query names nothing denied".
/// </summary>
/// <remarks>
/// The distinction is the whole point of the fixture. An empty
/// <see cref="PromQlMetricReferences.Names"/> list is what a genuinely
/// metric-free expression produces, so a malformed selector that also produced an
/// empty list with both flags clear would be admitted by the gate while the
/// backend still evaluated it. Each test therefore asserts the flag, not merely
/// that extraction did not throw.
/// </remarks>
public sealed partial class PromQlMetricExtractorTests
{
    private static PromQlMetricReferences Extract(string query)
        => PromQlMetricExtractor.ExtractReferences(query);

    [Test]
    public void The_inf_literal_is_not_reported_as_a_metric_name()
    {
        // 'inf' is a PromQL numeric literal, never a selector. Reporting it would
        // put a name on the gate's allow-list check that no series can carry.
        var references = Extract("up > inf");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void The_nan_literal_is_not_reported_as_a_metric_name()
    {
        var references = Extract("up != nan");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void A_name_matcher_truncated_at_the_end_of_the_query_is_unresolvable()
    {
        // '{__name__' with nothing after the label token: there is no operator to
        // read, so the matcher constrains nothing and must fail closed.
        var references = Extract("{__name__");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void A_name_matcher_whose_value_is_unquoted_is_unresolvable()
    {
        // '=' not followed by a quoted value. The bare token is not a value the
        // extractor can compare against the allow-list, so it fails closed rather
        // than treating 'up' as the designated name.
        var references = Extract("{__name__=up}");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void A_name_matcher_with_an_unrecognised_operator_is_unresolvable()
    {
        // '<' is not a label-matching operator. Anything the extractor does not
        // model is unresolvable, not absent.
        var references = Extract("{__name__<\"up\"}");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void A_bang_not_followed_by_a_matcher_operator_is_unresolvable()
    {
        // '!' only opens a matcher when '=' or '~' follows it. A lone '!' falls
        // through to the same fail-closed arm as any unrecognised operator.
        var references = Extract("{__name__!}");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void A_name_matcher_whose_operator_is_at_the_very_end_is_unresolvable()
    {
        // '!' is the last character, so the i + 1 < length guard fails before the
        // operator pair can be read.
        var references = Extract("{__name__!");

        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void An_unterminated_string_literal_swallows_the_rest_of_the_query()
    {
        // The scanner cannot close the literal, so it consumes to the end. The
        // metric named before it is still reported; nothing after it is, which is
        // the conservative direction (a hidden name is never silently admitted).
        var references = Extract("up or \"unterminated");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }

    [Test]
    public void An_unterminated_raw_backtick_name_matcher_value_is_unresolvable()
    {
        // A backtick literal is raw, so the only terminator is a closing backtick.
        // Without one the value never resolves and the matcher fails closed.
        var references = Extract("{__name__=`up}");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void A_name_matcher_value_ending_in_a_trailing_backslash_is_unresolvable()
    {
        // '{__name__="up\' - the backslash has no character to escape, so the
        // literal is unterminated and the value unresolved.
        var references = Extract("{__name__=\"up\\");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
        });
    }

    [Test]
    public void An_unbalanced_grouping_label_list_consumes_the_rest_of_the_query()
    {
        // 'sum by (job' never closes its label list. The scan must terminate at the
        // end of the input rather than run past it, and the label name inside the
        // list is not a metric.
        var references = Extract("sum by (job");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void A_nested_grouping_label_list_is_skipped_to_its_matching_paren()
    {
        // The inner ')' decrements the depth without closing the list, so the skip
        // must continue to the outer one. Stopping at the first ')' would leave the
        // trailing ')' to re-open operand position and mis-scan the aggregand.
        var references = Extract("sum by ((job)) (up)");

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }
}
