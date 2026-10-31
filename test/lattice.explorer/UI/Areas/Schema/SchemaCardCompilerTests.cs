using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaCardCompilerTests
{
    [TestCase("9007199254740993", "9007199254740992")]
    [TestCase("9223372036854775807", "9223372036854775806")]
    [TestCase("-9223372036854775807", "-9223372036854775808")]
    public void TryCompile_reversed_large_integer_bounds_reports_the_range_error(string minimum, string maximum)
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.NumberRange, "order.total");
        card.Minimum = minimum;
        card.Maximum = maximum;

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.EqualTo("The smallest number is larger than the largest."));
            Assert.That(rule, Is.EqualTo(default(LatticeSchemaRule)));
        });
    }

    [TestCase("9007199254740992", "9007199254740993")]
    [TestCase("9223372036854775806", "9223372036854775807")]
    [TestCase("-9223372036854775808", "-9223372036854775807")]
    public void TryCompile_ordered_large_integer_bounds_preserves_the_exact_constants(string minimum, string maximum)
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.NumberRange, "order.total");
        card.Minimum = minimum;
        card.Maximum = maximum;

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        Assert.Multiple(() =>
        {
            Assert.That(rule.Predicate!.Value.Children![0].Children![1].Constant,
                Is.EqualTo(LatticeConstant.Integer(long.Parse(minimum, System.Globalization.CultureInfo.InvariantCulture))));
            Assert.That(rule.Predicate.Value.Children![1].Children![1].Constant,
                Is.EqualTo(LatticeConstant.Integer(long.Parse(maximum, System.Globalization.CultureInfo.InvariantCulture))));
        });
    }

    [Test]
    public void TryCompile_invalid_pattern_returns_its_diagnostic_without_a_rule()
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.Pattern, "sku");
        card.Pattern = "(";

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.Not.Null.And.Not.Empty);
            Assert.That(rule, Is.EqualTo(default(LatticeSchemaRule)));
            Assert.That(card.Pattern, Is.EqualTo("("));
        });
    }

    [TestCase("NumberRange", "Enter the largest allowed number, such as 10000.")]
    [TestCase("TextLength", "Enter the most characters as a whole number.")]
    [TestCase("ListLength", "Enter the most items as a whole number.")]
    public void TryCompile_invalid_upper_bound_explains_the_failed_field(string kind, string expected)
    {
        var card = SchemaRuleCard.Of(Enum.Parse<SchemaCardKind>(kind), "order.total");
        card.Minimum = "1";
        card.Maximum = "invalid";

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.EqualTo(expected));
            Assert.That(rule, Is.EqualTo(default(LatticeSchemaRule)));
            Assert.That(card.Maximum, Is.EqualTo("invalid"));
        });
    }

    [TestCase("Custom", "A custom rule cannot be combined with other cards.")]
    [TestCase("Encoding", "A whole-value check cannot be combined with \"any of\" or \"every item\".")]
    [TestCase("MaxSize", "A whole-value check cannot be combined with \"any of\" or \"every item\".")]
    [TestCase("Pattern", "A format or pattern checks one member on its own, so it cannot be combined with \"any of\" or \"every item\".")]
    [TestCase("Format", "A format or pattern checks one member on its own, so it cannot be combined with \"any of\" or \"every item\".")]
    public void TryCompilePredicate_standalone_rule_is_refused_in_a_group(string kind, string expected)
    {
        var card = SchemaRuleCard.Of(Enum.Parse<SchemaCardKind>(kind));

        Assert.That(SchemaCardCompiler.TryCompilePredicate(card, out var predicate, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.EqualTo(expected));
            Assert.That(predicate, Is.EqualTo(default(LatticePredicateNode)));
        });
    }

    [TestCase(0)]
    [TestCase(1)]
    public void TryCompile_invalid_alternative_names_its_one_based_position(int invalidIndex)
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.AnyOf);
        card.Alternatives =
        [
            SchemaRuleCard.Of(SchemaCardKind.Required, "first"),
            SchemaRuleCard.Of(SchemaCardKind.Required, "second"),
        ];
        card.Alternatives[invalidIndex] = SchemaRuleCard.Of(SchemaCardKind.Custom);

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.EqualTo($"Alternative {invalidIndex + 1}: A custom rule cannot be combined with other cards."));
            Assert.That(rule, Is.EqualTo(default(LatticeSchemaRule)));
        });
    }

    [Test]
    public void TryCompile_nested_invalid_alternative_retains_every_item_context()
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.EveryItem, "lines");
        card.Item = SchemaRuleCard.Of(SchemaCardKind.AnyOf);
        card.Item.Alternatives =
        [
            SchemaRuleCard.Of(SchemaCardKind.Required, "sku"),
            SchemaRuleCard.Of(SchemaCardKind.NumberRange, "quantity"),
        ];
        card.Item.Alternatives[1].Maximum = "NaN";

        Assert.That(SchemaCardCompiler.TryCompile(card, out _, out var error), Is.False);
        Assert.That(error, Is.EqualTo("Every item: Alternative 2: Enter the largest allowed number, such as 10000."));
    }

    [Test]
    public void TryCompile_unknown_card_kind_returns_an_explicit_error()
    {
        var card = SchemaRuleCard.Of((SchemaCardKind)int.MaxValue);

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(error, Is.EqualTo("This card cannot be written as a predicate."));
            Assert.That(rule, Is.EqualTo(default(LatticeSchemaRule)));
        });
    }
}
