using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaCardDecompilerTests
{
    [TestCaseSource(nameof(UnrepresentableRules))]
    public void Decompile_unrepresentable_rule_is_preserved_without_rewriting(object boxed)
    {
        var rule = ((LatticeSchemaRule)boxed) with { Description = "  keep this rule  " };

        var card = SchemaCardDecompiler.Decompile(rule);

        Assert.Multiple(() =>
        {
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.Custom));
            Assert.That(card.Original, Is.EqualTo(rule));
            Assert.That(card.Description, Is.EqualTo(rule.Description));
            Assert.That(SchemaCardCompiler.TryCompile(card, out var saved, out var error), Is.True, error);
            Assert.That(saved, Is.EqualTo(rule), "Opening and saving an unrepresentable rule must be lossless.");
        });
    }

    private static IEnumerable<TestCaseData> UnrepresentableRules()
    {
        yield return Case("unknown encoding", new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = (LatticeSchemaEncodingKind)byte.MaxValue,
        });
        yield return Case("size without a bound", new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.MaxByteLength,
        });
        yield return Case("negative size", new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.MaxByteLength,
            MaxByteLength = -1,
        });
        yield return Predicate("invalid member path", Compare(LatticeComparisonOperator.GreaterThanOrEqual, "a..b", 1));
        yield return Predicate("length comparison without a type guard", Length(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1));
        yield return Predicate("unknown string method", LatticePredicateNode.StringCall(
            (LatticeStringMethod)byte.MaxValue, LatticePredicateNode.Member("a"), LatticePredicateNode.Const(LatticeConstant.Text("x"))));
        yield return Predicate("unrecognised structural type", LatticePredicateNode.TypeOf("a", LatticeValueKind.String));
        yield return Predicate("every item with an unsupported body", LatticePredicateNode.Every("items",
            LatticePredicateNode.Bool(LatticeBooleanOperator.Not, LatticePredicateNode.Self())));

        var text = LatticePredicateNode.TypeOf("a", LatticeValueKind.String);
        var length = LatticePredicateNode.LengthOf("a");
        yield return Predicate("length on another member", And(text, Length(LatticeComparisonOperator.GreaterThanOrEqual, "b", 1)));
        yield return Predicate("noninteger length", And(text, LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual, length, LatticePredicateNode.Const(LatticeConstant.Real(1.5)))));
        yield return Predicate("unsupported length operator", And(text, Length(LatticeComparisonOperator.Equal, "a", 1)));
        yield return Predicate("duplicate minimum length", And(text,
            Length(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1),
            Length(LatticeComparisonOperator.GreaterThanOrEqual, "a", 2)));
        yield return Predicate("duplicate maximum length", And(text,
            Length(LatticeComparisonOperator.LessThanOrEqual, "a", 2),
            Length(LatticeComparisonOperator.LessThanOrEqual, "a", 3)));
        yield return Predicate("reordered length bounds", And(text,
            Length(LatticeComparisonOperator.LessThanOrEqual, "a", 5),
            Length(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1)));
        yield return Predicate("range with a nonnumeric part", And(
            Compare(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1),
            LatticePredicateNode.TypeOf("a", LatticeValueKind.Object)));
        yield return Predicate("duplicate numeric minimum", And(
            Compare(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1),
            Compare(LatticeComparisonOperator.GreaterThanOrEqual, "a", 2)));
        yield return Predicate("duplicate integer constraint", And(
            LatticePredicateNode.TypeOf("a", LatticeValueKind.Integer),
            LatticePredicateNode.TypeOf("a", LatticeValueKind.Integer)));
        yield return Predicate("reordered numeric bounds", And(
            Compare(LatticeComparisonOperator.LessThanOrEqual, "a", 5),
            Compare(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1)));

        var nested = LatticePredicateNode.Bool(LatticeBooleanOperator.Or,
            LatticePredicateNode.TypeOf("a", LatticeValueKind.Object),
            LatticePredicateNode.TypeOf("b", LatticeValueKind.Array));
        yield return Predicate("nested any of", LatticePredicateNode.Bool(LatticeBooleanOperator.Or,
            nested, LatticePredicateNode.TypeOf("c", LatticeValueKind.Integer)));
    }

    [Test]
    public void Decompile_valid_numeric_range_retains_integer_and_real_bounds()
    {
        var rule = LatticeSchemaRule.Structured(And(
            LatticePredicateNode.TypeOf("a", LatticeValueKind.Integer),
            Compare(LatticeComparisonOperator.GreaterThanOrEqual, "a", 1),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual,
                LatticePredicateNode.Member("a"), LatticePredicateNode.Const(LatticeConstant.Real(4.5)))));

        var card = SchemaCardDecompiler.Decompile(rule);

        Assert.Multiple(() =>
        {
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.NumberRange));
            Assert.That(card.IntegerOnly, Is.True);
            Assert.That(card.Minimum, Is.EqualTo("1"));
            Assert.That(card.Maximum, Is.EqualTo("4.5"));
            Assert.That(SchemaCardCompiler.TryCompile(card, out var saved, out _), Is.True);
            Assert.That(saved, Is.EqualTo(rule));
        });
    }

    private static TestCaseData Case(string name, LatticeSchemaRule rule) => new TestCaseData(rule).SetName("Decompile_preserves_" + name.Replace(' ', '_'));

    private static TestCaseData Predicate(string name, LatticePredicateNode node) => Case(name, LatticeSchemaRule.Structured(node));

    private static LatticePredicateNode And(params LatticePredicateNode[] nodes) => LatticePredicateNode.Bool(LatticeBooleanOperator.And, nodes);

    private static LatticePredicateNode Compare(LatticeComparisonOperator operation, string path, long value) =>
        LatticePredicateNode.Compare(operation, LatticePredicateNode.Member(path), LatticePredicateNode.Const(LatticeConstant.Integer(value)));

    private static LatticePredicateNode Length(LatticeComparisonOperator operation, string path, long value) =>
        LatticePredicateNode.Compare(operation, LatticePredicateNode.LengthOf(path), LatticePredicateNode.Const(LatticeConstant.Integer(value)));
}
