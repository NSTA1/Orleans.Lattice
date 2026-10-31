using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// A conditional set step (#5476): the remediation editor's set step limited to the values
/// whose member meets a condition, so a remediation rewrites only the values that are wrong
/// and every other value keeps its member.
/// </summary>
[TestFixture]
public sealed class SchemaTransformConditionTests
{
    /// <summary>The transform the U2 journey pins: a negative <c>total</c> becomes 0, any other total is kept.</summary>
    private static readonly LatticeValueTransform NegativeTotalToZero = LatticeValueTransform.Passthrough(
        LatticeValueTransform.SetMember(
            "total",
            LatticeValueTransform.Conditional(
                LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, LatticePredicateNode.Member("total"), LatticePredicateNode.Const(LatticeConstant.Integer(0))),
                LatticeValueTransform.Const(LatticeConstant.Integer(0)),
                LatticeValueTransform.Member("total"))));

    [Test]
    public void A_set_where_the_member_is_less_than_zero_compiles_to_a_conditional_that_keeps_every_other_value()
    {
        var draft = new SchemaTransformDraft
        {
            Kind = SchemaTransformStepKind.Set,
            Path = "total",
            ValueKind = SchemaConstantKind.Number,
            Value = "0",
            Conditional = true,
            WhereOperator = LatticeComparisonOperator.LessThan,
            WhereKind = SchemaConstantKind.Number,
            WhereValue = " 0 ",
        };

        Assert.That(draft.TryAdd(out var error), Is.True, error);
        Assert.That(draft.TryBuild(out var transform, out _), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(transform, Is.EqualTo(NegativeTotalToZero));
            Assert.That(draft.Steps.Single().Describe(), Is.EqualTo("Set total to 0 where total is less than 0"));
            Assert.That(draft.Conditional, Is.False, "the builder clears the condition after each step");
            Assert.That(draft.WhereValue, Is.Empty);
        });
    }

    [Test]
    public void An_unconditional_set_compiles_as_it_always_did()
    {
        var draft = new SchemaTransformDraft { Kind = SchemaTransformStepKind.Set, Path = "total", ValueKind = SchemaConstantKind.Number, Value = "0" };

        Assert.That(draft.TryAdd(out _), Is.True);
        Assert.That(draft.TryBuild(out var transform, out _), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(transform, Is.EqualTo(LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember("total", LatticeValueTransform.Const(LatticeConstant.Integer(0))))));
            Assert.That(draft.Steps.Single().Where, Is.Null);
        });
    }

    [Test]
    public void A_text_condition_reads_back_quoted_and_compares_as_text()
    {
        var draft = new SchemaTransformDraft
        {
            Path = "status",
            Value = "shipped",
            Conditional = true,
            WhereOperator = LatticeComparisonOperator.Equal,
            WhereKind = SchemaConstantKind.Text,
            WhereValue = "sent",
        };

        Assert.That(draft.TryAdd(out _), Is.True);
        Assert.That(draft.TryBuild(out var transform, out _), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(draft.Steps.Single().Describe(), Is.EqualTo("Set status to \"shipped\" where status is equal to \"sent\""));
            Assert.That(transform.Children![0].Children![0].Condition, Is.EqualTo(
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("sent")))));
        });
    }

    [Test]
    public void A_null_condition_reads_as_null_and_needs_no_value()
    {
        var draft = new SchemaTransformDraft
        {
            Path = "region",
            Value = "eu",
            Conditional = true,
            WhereOperator = LatticeComparisonOperator.Equal,
            WhereKind = SchemaConstantKind.Null,
        };

        Assert.That(draft.TryAdd(out var error), Is.True, error);
        Assert.That(draft.Steps.Single().Describe(), Is.EqualTo("Set region to \"eu\" where region is equal to null"));
    }

    [TestCase(nameof(LatticeComparisonOperator.LessThan), nameof(SchemaConstantKind.Null), "", "Compare a member with null only as equal to or not equal to.")]
    [TestCase(nameof(LatticeComparisonOperator.LessThan), nameof(SchemaConstantKind.Number), "minus one", "The condition's value: Enter a number, such as 42 or 2.5.")]
    [TestCase(nameof(LatticeComparisonOperator.Equal), nameof(SchemaConstantKind.Boolean), "maybe", "The condition's value: Enter true or false.")]
    public void A_condition_that_is_not_complete_is_explained_and_adds_nothing(string comparison, string kind, string value, string expected)
    {
        var draft = new SchemaTransformDraft
        {
            Path = "total",
            ValueKind = SchemaConstantKind.Number,
            Value = "0",
            Conditional = true,
            WhereOperator = Enum.Parse<LatticeComparisonOperator>(comparison),
            WhereKind = Enum.Parse<SchemaConstantKind>(kind),
            WhereValue = value,
        };

        Assert.Multiple(() =>
        {
            Assert.That(draft.TryAdd(out var error), Is.False);
            Assert.That(error, Is.EqualTo(expected));
            Assert.That(draft.Steps, Is.Empty);
        });
    }

    [Test]
    public void Clearing_the_draft_resets_the_condition()
    {
        var draft = new SchemaTransformDraft
        {
            Conditional = true,
            WhereOperator = LatticeComparisonOperator.GreaterThan,
            WhereKind = SchemaConstantKind.Text,
            WhereValue = "x",
        };

        draft.Clear();

        Assert.Multiple(() =>
        {
            Assert.That(draft.Conditional, Is.False);
            Assert.That(draft.WhereOperator, Is.EqualTo(LatticeComparisonOperator.LessThan));
            Assert.That(draft.WhereKind, Is.EqualTo(SchemaConstantKind.Number));
            Assert.That(draft.WhereValue, Is.Empty);
        });
    }

    [Test]
    public void Every_comparison_has_words_and_reads_back_in_a_condition()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaTransformCondition.Comparisons.Select(comparison => comparison.Operator),
                Is.EquivalentTo(Enum.GetValues<LatticeComparisonOperator>()), "every comparison the predicate model has is offered");
            Assert.That(SchemaTransformCondition.Comparisons.Select(comparison => SchemaTransformCondition.WordsOf(comparison.Operator)),
                Is.EqualTo(new[] { "less than", "at most", "equal to", "not equal to", "at least", "greater than" }));
            Assert.That(new SchemaTransformCondition(LatticeComparisonOperator.GreaterThanOrEqual, SchemaConstantKind.Number, "10").Describe(),
                Is.EqualTo("is at least 10"));
            Assert.That(SchemaTransformCondition.WordsOf((LatticeComparisonOperator)99), Is.EqualTo("99"));
        });
    }
}
