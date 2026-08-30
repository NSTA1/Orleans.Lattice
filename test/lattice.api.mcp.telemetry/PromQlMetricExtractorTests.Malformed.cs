namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Malformed and truncated-input tests for <see cref="PromQlMetricExtractor"/>.
/// The extractor feeds the deny-all metric-access gate, so every expression it
/// cannot reduce to a fixed set of metric names must fail closed - by setting
/// <c>HasUnresolvableNameMatcher</c> or <c>HasUnconstrainedSelector</c> - rather
/// than silently resolving to an empty, admissible name set. These cases cover the
/// truncated scanner paths a hostile or simply broken expression reaches.
/// </summary>
public sealed partial class PromQlMetricExtractorTests
{
    private static PromQlMetricReferences Refs(string query)
        => PromQlMetricExtractor.ExtractReferences(query);

    // ---- Truncated __name__ matchers: every arm must fail closed ----

    [Test]
    public void A_name_matcher_truncated_before_its_operator_is_unresolvable()
    {
        // '{__name__' with nothing after it: the matcher operator never arrives.
        var references = Refs("{__name__");
        Assert.Multiple(() =>
        {
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
            Assert.That(references.Names, Is.Empty);
        });
    }

    [Test]
    public void A_name_matcher_truncated_after_trailing_whitespace_is_unresolvable()
    {
        var references = Refs("{__name__   ");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_name_matcher_equating_to_an_unquoted_value_is_unresolvable()
    {
        // '=' must be followed by a quoted literal; a bare identifier is not a
        // value this extractor can resolve, so it fails closed.
        var references = Refs("{__name__=up}");
        Assert.Multiple(() =>
        {
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
            Assert.That(references.Names, Does.Not.Contain("up"));
        });
    }

    [Test]
    public void A_name_matcher_truncated_immediately_after_its_equals_is_unresolvable()
    {
        var references = Refs("{__name__=");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_name_matcher_followed_by_an_unrecognised_operator_is_unresolvable()
    {
        // Neither '=', '=~', '!=' nor '!~': not a matcher form the extractor models.
        var references = Refs("{__name__ up}");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_bare_bang_after_a_name_label_is_unresolvable()
    {
        // A lone '!' is not the start of '!=' or '!~', so it falls through to the
        // no-recognised-operator arm rather than being read as a negative matcher.
        var references = Refs("{__name__!");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    [Test]
    public void A_name_matcher_ending_in_a_trailing_backslash_is_unresolvable()
    {
        // The escape has no character to escape, so the literal is unterminated.
        var references = Refs("{__name__=\"up\\");
        Assert.Multiple(() =>
        {
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
            Assert.That(references.Names, Is.Empty);
        });
    }

    [Test]
    public void An_unterminated_raw_backtick_name_matcher_is_unresolvable()
    {
        // A raw (backtick) literal does no escape processing, so its only
        // terminator is the closing backtick - which never arrives here.
        var references = Refs("{__name__=`up");
        Assert.Multiple(() =>
        {
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
            Assert.That(references.Names, Is.Empty);
        });
    }

    [Test]
    public void An_unterminated_single_quoted_name_matcher_is_unresolvable()
    {
        var references = Refs("{__name__='up");
        Assert.That(references.HasUnresolvableNameMatcher, Is.True);
    }

    // ---- Unterminated selectors and strings ----

    [Test]
    public void An_unterminated_top_level_selector_is_flagged_as_unconstrained()
    {
        // The closing brace never arrives, so the selector was never proven to be
        // anchored to a metric name; the gate must fail closed on it.
        var references = Refs("{job=\"api\"");
        Assert.That(references.HasUnconstrainedSelector, Is.True);
    }

    [Test]
    public void An_unterminated_selector_anchored_to_a_metric_is_not_unconstrained()
    {
        // The metric name preceding '{' anchors the selector, and that name was
        // itself allow-list checked, so truncation alone does not make it unsafe.
        var references = Refs("up{job=\"api\"");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void An_unterminated_selector_pinned_by_an_exact_name_matcher_is_not_unconstrained()
    {
        var references = Refs("{__name__=\"up\",job=\"api\"");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void An_unterminated_top_level_string_swallows_the_rest_of_the_expression()
    {
        // The scanner runs to the end of the input looking for the closing quote,
        // so nothing after the opening quote is read as a metric name.
        var references = Refs("up + \"secret_metric");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_stray_closing_brace_does_not_flag_an_unconstrained_selector()
    {
        // Depth is already zero, so the close is ignored rather than underflowing
        // into a spurious unconstrained-selector verdict.
        var references = Refs("up}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    // ---- Quote flavours and nesting ----

    [Test]
    public void A_single_quoted_top_level_string_is_skipped()
    {
        var references = Refs("up == 'secret_metric'");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_raw_backtick_top_level_string_is_skipped()
    {
        var references = Refs("up == `secret_metric`");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_backslash_in_a_raw_backtick_string_does_not_escape_its_terminator()
    {
        // PromQL backtick literals are raw: the backslash is data, so the very next
        // backtick still closes the string and 'up' after it is a real metric name.
        var references = Refs("`a\\` + up");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void An_escaped_quote_inside_a_top_level_string_does_not_terminate_it()
    {
        var references = Refs("up == \"a\\\"b\" + down");
        Assert.That(references.Names, Is.EqualTo(new[] { "up", "down" }));
    }

    [Test]
    public void A_nested_selector_brace_does_not_reset_the_outer_anchor()
    {
        // Only the outermost '{' records the anchor, so an inner brace cannot
        // launder an unanchored outer selector into an anchored one.
        var references = Refs("{job=\"{api}\"}");
        Assert.That(references.HasUnconstrainedSelector, Is.True);
    }

    // ---- Grouping modifiers, calls, and literals ----

    [Test]
    public void A_grouping_keyword_with_no_label_list_leaves_the_next_identifier_in_metric_position()
    {
        // 'sum by job' is not valid PromQL - a grouping modifier requires a
        // parenthesised label list. With no '(' to skip, the scanner leaves the
        // following identifier in metric-name position, so the deny-all gate has to
        // admit 'job' before the expression passes. That is the fail-closed
        // direction: a malformed expression demands more of the allow-list, never
        // less.
        var references = Refs("sum by job");
        Assert.That(references.Names, Is.EqualTo(new[] { "job" }));
    }

    [Test]
    public void A_grouping_label_list_with_an_unbalanced_paren_swallows_the_remainder()
    {
        // The label list is never closed, so the scanner consumes to the end and no
        // identifier inside it is mistaken for a metric name.
        var references = Refs("sum by (instance, secret_metric");
        Assert.That(references.Names, Is.Empty);
    }

    [Test]
    public void A_reserved_word_is_not_extracted_as_a_metric_name()
    {
        var references = Refs("up offset 5m");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_trailing_duration_literal_is_not_mistaken_for_a_metric()
    {
        // The digit arm consumes to the end of the input here, exercising its
        // end-of-string exit rather than its usual delimiter exit.
        var references = Refs("up offset 1h30m");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_query_that_is_only_whitespace_names_nothing()
    {
        var references = Refs("   \t\r\n  ");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void A_name_matcher_repeated_for_the_same_metric_contributes_one_name()
    {
        var references = Refs("{__name__=\"up\"} or {__name__=\"up\"}");
        Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
    }

    [Test]
    public void A_nested_brace_does_not_re_evaluate_the_outer_selector_anchor()
    {
        // Only the outermost '{' records whether a metric name preceded it, and only
        // its matching '}' decides the unconstrained verdict, so an inner brace pair
        // cannot launder an unanchored outer selector into an anchored one.
        var references = Refs("{job={nested}}");
        Assert.That(references.HasUnconstrainedSelector, Is.True);
    }

    [Test]
    public void A_nested_brace_inside_a_metric_anchored_selector_stays_anchored()
    {
        var references = Refs("up{job={nested}}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void A_grouping_keyword_at_the_end_of_the_expression_names_no_metric()
    {
        // There is no label list to skip and nothing follows, so the scanner reaches
        // the end of the input from the grouping-modifier arm.
        var references = Refs("sum by ");
        Assert.That(references.Names, Is.Empty);
    }

    [Test]
    public void A_name_matcher_with_several_escapes_unescapes_every_one()
    {
        // Two escapes in one literal exercise the append path after the builder has
        // already been created by the first.
        var references = Refs("{__name__=\"a\\\\b\\\\c\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "a\\b\\c" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
        });
    }
}
