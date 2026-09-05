using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Structural-validation tests for the value-transform evaluator: a transform whose
/// shape is malformed - a non-Passthrough root, a value expression in document
/// position, an operation with the wrong child count, an empty member path, an
/// unknown compute operator, or nesting past the depth budget - must throw a clear
/// exception rather than silently produce a wrong document.
/// </summary>
public sealed partial class LatticeValueTransformEvaluatorTests
{
    [Test]
    public void Evaluate_passthrough_preserves_json_null_root()
    {
        var result = LatticeValueTransformEvaluation.Evaluate(Utf8("null"), LatticeValueTransform.Passthrough());

        Assert.That(Encoding.UTF8.GetString(result), Is.EqualTo("null"));
    }

    [Test]
    public void Evaluate_passthrough_with_null_children_preserves_document()
    {
        var result = LatticeValueTransformEvaluation.Evaluate(
            Utf8("{\"a\":1}"),
            new LatticeValueTransform { Kind = LatticeValueTransformKind.Passthrough });

        using var document = JsonDocument.Parse(result);
        Assert.That(document.RootElement.GetProperty("a").GetInt32(), Is.EqualTo(1));
    }

    [Test]
    public void Evaluate_rejects_non_passthrough_root()
    {
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{\"a\":1}"), LatticeValueTransform.Member("a")),
            Throws.InvalidOperationException.With.Message.Contains("root"));
    }

    [Test]
    public void Evaluate_rejects_value_expression_in_document_pipeline()
    {
        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.Member("a"));

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{\"a\":1}"), transform),
            Throws.InvalidOperationException.With.Message.Contains("document operation"));
    }

    [Test]
    public void SetMember_without_exactly_one_child_throws()
    {
        var noChild = new LatticeValueTransform
        {
            Kind = LatticeValueTransformKind.SetMember,
            MemberPath = "x",
            Children = Array.Empty<LatticeValueTransform>(),
        };
        var twoChildren = new LatticeValueTransform
        {
            Kind = LatticeValueTransformKind.SetMember,
            MemberPath = "x",
            Children =
            [
                LatticeValueTransform.Const(LatticeConstant.Integer(1)),
                LatticeValueTransform.Const(LatticeConstant.Integer(2)),
            ],
        };

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{}"), LatticeValueTransform.Passthrough(noChild)),
            Throws.InvalidOperationException.With.Message.Contains("exactly one"));
        var nullChildren = noChild with { Children = null };
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{}"), LatticeValueTransform.Passthrough(nullChildren)),
            Throws.InvalidOperationException.With.Message.Contains("exactly one"));
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{}"), LatticeValueTransform.Passthrough(twoChildren)),
            Throws.InvalidOperationException.With.Message.Contains("exactly one"));
    }

    [Test]
    public void Conditional_without_two_branches_throws()
    {
        var condition = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("a"),
            LatticePredicateNode.Const(LatticeConstant.Integer(1)));
        var malformed = new LatticeValueTransform
        {
            Kind = LatticeValueTransformKind.Conditional,
            Condition = condition,
            Children = [LatticeValueTransform.Const(LatticeConstant.Text("yes"))],
        };

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(
                Utf8("{\"a\":1}"),
                LatticeValueTransform.Passthrough(LatticeValueTransform.SetMember("x", malformed))),
            Throws.InvalidOperationException.With.Message.Contains("exactly two"));
        var nullChildren = malformed with { Children = null };
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(
                Utf8("{\"a\":1}"),
                LatticeValueTransform.Passthrough(LatticeValueTransform.SetMember("x", nullChildren))),
            Throws.InvalidOperationException.With.Message.Contains("exactly two"));
    }

    [Test]
    public void SetMember_with_document_operation_as_value_expression_throws()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("x", LatticeValueTransform.DropMember("a")));

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{\"a\":1}"), transform),
            Throws.InvalidOperationException.With.Message.Contains("value expression"));
    }

    [Test]
    public void Compute_without_operands_throws()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("x", LatticeValueTransform.Compute(LatticeComputeOperator.Concat)));

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{}"), transform),
            Throws.InvalidOperationException.With.Message.Contains("at least one"));
        var nullChildren = new LatticeValueTransform
        {
            Kind = LatticeValueTransformKind.Compute,
            ComputeOperator = LatticeComputeOperator.Concat,
        };
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(
                Utf8("{}"),
                LatticeValueTransform.Passthrough(LatticeValueTransform.SetMember("x", nullChildren))),
            Throws.InvalidOperationException.With.Message.Contains("at least one"));
    }

    [Test]
    public void Compute_coalesce_returns_json_null_when_every_operand_is_missing_or_null()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("name", LatticeValueTransform.Compute(
                LatticeComputeOperator.Coalesce,
                LatticeValueTransform.Member("missing"),
                LatticeValueTransform.Const(LatticeConstant.Null()))));

        var root = Evaluate("{\"other\":1}", transform);

        Assert.That(root.GetProperty("name").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public void Compute_with_unknown_operator_throws()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(
                "x",
                LatticeValueTransform.Compute(
                    (LatticeComputeOperator)99,
                    LatticeValueTransform.Const(LatticeConstant.Integer(1)))));

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{}"), transform),
            Throws.InvalidOperationException.With.Message.Contains("Unknown compute operator"));
    }

    [Test]
    public void Constants_project_boolean_and_double_as_json_values()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("flag", LatticeValueTransform.Const(LatticeConstant.Bool(true))),
            LatticeValueTransform.SetMember("score", LatticeValueTransform.Const(LatticeConstant.Real(1.5))));

        var root = Evaluate("{}", transform);

        Assert.That(root.GetProperty("flag").GetBoolean(), Is.True);
        Assert.That(root.GetProperty("score").GetDouble(), Is.EqualTo(1.5d));
    }

    [Test]
    public void Constant_with_unknown_kind_throws()
    {
        // LatticeConstantKind is wire format and is persisted in the remediation
        // state, so a kind authored by a newer node is reachable here on a
        // mixed-version cluster. It must fail closed like the unknown compute
        // operator above, rather than projecting as JSON null.
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{\"tier\":\"gold\"}"), UnknownConstantTransform()),
            Throws.InvalidOperationException.With.Message.Contains("Unknown constant kind"));
    }

    [Test]
    public void Constant_with_unknown_kind_does_not_silently_null_the_existing_member()
    {
        // The concrete data-loss shape: a remediation pass rewrites every entry,
        // so a kind this node cannot map must abort the pass rather than replace
        // the member's existing value with a null indistinguishable from an
        // explicit LatticeConstantKind.Null constant.
        var input = Utf8("{\"tier\":\"gold\"}");

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(input, UnknownConstantTransform()),
            Throws.InvalidOperationException);

        using var document = JsonDocument.Parse(input);
        Assert.That(document.RootElement.GetProperty("tier").GetString(), Is.EqualTo("gold"),
            "The transform failed closed, so no value was overwritten.");
    }

    private static LatticeValueTransform UnknownConstantTransform() =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember(
                "tier",
                LatticeValueTransform.Const(new LatticeConstant { Kind = (LatticeConstantKind)99 })));

    [Test]
    public void Empty_member_paths_on_operations_and_expressions_throw()
    {
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(
                Utf8("{}"),
                LatticeValueTransform.Passthrough(LatticeValueTransform.DropMember(string.Empty))),
            Throws.InvalidOperationException.With.Message.Contains("non-empty member path"));
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(
                Utf8("{}"),
                LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("a", string.Empty))),
            Throws.InvalidOperationException.With.Message.Contains("non-empty member path"));
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(
                Utf8("{}"),
                LatticeValueTransform.Passthrough(
                    LatticeValueTransform.SetMember("x", LatticeValueTransform.Member(string.Empty)))),
            Throws.InvalidOperationException.With.Message.Contains("non-empty member path"));
    }

    [Test]
    public void Evaluate_rejects_excessive_transform_depth()
    {
        var expression = LatticeValueTransform.Const(LatticeConstant.Null());
        for (var i = 0; i <= LatticeValueTransformEvaluator.MaxDepth; i++)
        {
            expression = LatticeValueTransform.Compute(LatticeComputeOperator.Coalesce, expression);
        }

        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.SetMember("x", expression));

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{}"), transform),
            Throws.InvalidOperationException.With.Message.Contains("nesting depth"));
    }
}
