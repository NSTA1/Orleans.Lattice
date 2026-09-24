namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Keyword-named metric tests for <see cref="PromQlMetricExtractor"/>. Prometheus's
/// grammar accepts the aggregation operators, the set operators, and the
/// <c>by</c> / <c>without</c> / <c>offset</c> / <c>start</c> / <c>end</c> keywords
/// as a bare metric name wherever an operand is expected (its
/// <c>metric_identifier</c> production), so <c>up or min</c> evaluates the metric
/// named <c>min</c>. The extractor feeds the deny-all gate, so every such selector
/// must be reported as a referenced name - and the same keywords used as operators
/// or modifiers must still not be.
/// </summary>
public sealed partial class PromQlMetricExtractorTests
{
    [Test]
    public void A_bare_aggregation_keyword_in_operand_position_is_a_metric_name()
        => Assert.That(Names("up or min"), Is.EqualTo(new[] { "up", "min" }));

    [Test]
    public void A_keyword_named_range_selector_inside_a_function_call_is_a_metric_name()
        => Assert.That(Names("up + rate(count[5m])"), Is.EqualTo(new[] { "up", "count" }));

    [Test]
    public void A_keyword_named_metric_with_a_label_selector_is_extracted_and_anchored()
    {
        var references = PromQlMetricExtractor.ExtractReferences("sum{job=\"api\"}");
        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "sum" }));
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void The_start_and_end_preprocessor_keywords_are_metric_names_without_parentheses()
        => Assert.That(Names("up + start - end"), Is.EqualTo(new[] { "up", "start", "end" }));

    [Test]
    public void A_set_operator_keyword_in_operand_position_is_a_metric_name()
        => Assert.That(Names("up and unless"), Is.EqualTo(new[] { "up", "unless" }));

    [Test]
    public void Offset_and_grouping_keywords_in_operand_position_are_metric_names()
        => Assert.That(Names("up or offset or by or without"), Is.EqualTo(new[] { "up", "offset", "by", "without" }));

    [Test]
    public void A_keyword_named_metric_after_a_bool_modifier_is_a_metric_name()
        => Assert.That(Names("up > bool max"), Is.EqualTo(new[] { "up", "max" }));

    [Test]
    public void Set_operators_between_operands_are_not_metric_names()
        => Assert.That(Names("a and b or c unless d"), Is.EqualTo(new[] { "a", "b", "c", "d" }));

    [Test]
    public void An_offset_modifier_after_an_operand_is_not_a_metric_name()
        => Assert.That(
            Names("a offset 5m or b[5m] offset -1h or c{job=\"x\"} offset 1h or sum(d offset 2h)"),
            Is.EqualTo(new[] { "a", "b", "c", "d" }));

    [Test]
    public void Aggregations_with_leading_or_trailing_grouping_are_not_metric_names()
        => Assert.That(
            Names("sum by (job) (a) or min without(job)(b) or topk(5, c) by (job) or count_values(\"v\", d)"),
            Is.EqualTo(new[] { "a", "b", "c", "d" }));

    [Test]
    public void A_set_operator_after_a_trailing_grouping_clause_is_not_a_metric_name()
        => Assert.That(Names("sum(a) by (job) or b"), Is.EqualTo(new[] { "a", "b" }));

    [Test]
    public void Vector_matching_modifiers_leave_the_right_hand_operand_in_metric_position()
        => Assert.That(
            Names("a * on(job) group_left(team) min or b / ignoring(x) group_right c"),
            Is.EqualTo(new[] { "a", "min", "b", "c" }));

    [Test]
    public void The_at_modifier_start_and_end_functions_are_not_metric_names()
        => Assert.That(Names("a @ start() or b @ end()"), Is.EqualTo(new[] { "a", "b" }));
}
