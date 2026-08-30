using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Covers <see cref="LatticeValueTransformEvaluation"/> / the internal evaluator:
/// each document operation and value expression rewrites the JSON document as
/// specified, member reads resolve against the input document, and a null, empty,
/// or malformed payload throws a clear exception rather than corrupting the value.
/// </summary>
[TestFixture]
public sealed partial class LatticeValueTransformEvaluatorTests
{
    private static byte[] Utf8(string json) => Encoding.UTF8.GetBytes(json);

    private static JsonElement Evaluate(string json, LatticeValueTransform transform)
    {
        var result = LatticeValueTransformEvaluation.Evaluate(Utf8(json), transform);
        return JsonDocument.Parse(result).RootElement.Clone();
    }

    [Test]
    public void Passthrough_identity_preserves_all_members()
    {
        var root = Evaluate("{\"a\":1,\"b\":\"x\"}", LatticeValueTransform.Passthrough());

        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
        Assert.That(root.GetProperty("b").GetString(), Is.EqualTo("x"));
    }

    [Test]
    public void SetMember_constant_adds_new_member()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("added", LatticeValueTransform.Const(LatticeConstant.Integer(9))));

        var root = Evaluate("{\"a\":1}", transform);

        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
        Assert.That(root.GetProperty("added").GetInt32(), Is.EqualTo(9));
    }

    [Test]
    public void SetMember_constant_overwrites_existing_member()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("a", LatticeValueTransform.Const(LatticeConstant.Text("new"))));

        var root = Evaluate("{\"a\":\"old\"}", transform);

        Assert.That(root.GetProperty("a").GetString(), Is.EqualTo("new"));
    }

    [Test]
    public void SetMember_member_copies_value_from_input_document()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("copy", LatticeValueTransform.Member("source")));

        var root = Evaluate("{\"source\":42}", transform);

        Assert.That(root.GetProperty("copy").GetInt32(), Is.EqualTo(42));
    }

    [Test]
    public void SetMember_reads_original_input_not_prior_overwrite()
    {
        // Reads resolve against the input document, so overwriting "a" first does
        // not change what the later read of "a" sees - order-independent reads.
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("a", LatticeValueTransform.Const(LatticeConstant.Integer(2))),
            LatticeValueTransform.SetMember("echo", LatticeValueTransform.Member("a")));

        var root = Evaluate("{\"a\":1}", transform);

        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(2));
        Assert.That(root.GetProperty("echo").GetInt32(), Is.EqualTo(1));
    }

    [Test]
    public void DropMember_removes_the_member()
    {
        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.DropMember("gone"));

        var root = Evaluate("{\"keep\":1,\"gone\":2}", transform);

        Assert.That(root.TryGetProperty("gone", out _), Is.False);
        Assert.That(root.GetProperty("keep").GetInt32(), Is.EqualTo(1));
    }

    [Test]
    public void RenameMember_moves_the_member_value()
    {
        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("old", "new"));

        var root = Evaluate("{\"old\":\"v\"}", transform);

        Assert.That(root.TryGetProperty("old", out _), Is.False);
        Assert.That(root.GetProperty("new").GetString(), Is.EqualTo("v"));
    }

    [Test]
    public void RenameMember_absent_source_is_a_no_op()
    {
        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.RenameMember("missing", "new"));

        var root = Evaluate("{\"a\":1}", transform);

        Assert.That(root.TryGetProperty("new", out _), Is.False);
        Assert.That(root.GetProperty("a").GetInt32(), Is.EqualTo(1));
    }

    [Test]
    public void Conditional_selects_then_branch_when_predicate_matches()
    {
        var condition = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(18)));

        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("tier", LatticeValueTransform.Conditional(
                condition,
                LatticeValueTransform.Const(LatticeConstant.Text("adult")),
                LatticeValueTransform.Const(LatticeConstant.Text("minor")))));

        var root = Evaluate("{\"age\":21}", transform);

        Assert.That(root.GetProperty("tier").GetString(), Is.EqualTo("adult"));
    }

    [Test]
    public void Conditional_selects_else_branch_when_predicate_does_not_match()
    {
        var condition = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("age"),
            LatticePredicateNode.Const(LatticeConstant.Integer(18)));

        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("tier", LatticeValueTransform.Conditional(
                condition,
                LatticeValueTransform.Const(LatticeConstant.Text("adult")),
                LatticeValueTransform.Const(LatticeConstant.Text("minor")))));

        var root = Evaluate("{\"age\":12}", transform);

        Assert.That(root.GetProperty("tier").GetString(), Is.EqualTo("minor"));
    }

    [Test]
    public void Compute_concat_joins_members_and_constants_as_strings()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("full", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Member("first"),
                LatticeValueTransform.Const(LatticeConstant.Text(" ")),
                LatticeValueTransform.Member("last"))));

        var root = Evaluate("{\"first\":\"Ada\",\"last\":\"Byte\"}", transform);

        Assert.That(root.GetProperty("full").GetString(), Is.EqualTo("Ada Byte"));
    }

    [Test]
    public void Compute_concat_renders_number_operand_without_quotes()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("label", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Const(LatticeConstant.Text("v")),
                LatticeValueTransform.Member("n"))));

        var root = Evaluate("{\"n\":7}", transform);

        Assert.That(root.GetProperty("label").GetString(), Is.EqualTo("v7"));
    }

    [Test]
    public void Compute_concat_renders_missing_operand_as_empty_string()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("label", LatticeValueTransform.Compute(
                LatticeComputeOperator.Concat,
                LatticeValueTransform.Const(LatticeConstant.Text("v")),
                LatticeValueTransform.Member("missing"))));

        var root = Evaluate("{}", transform);

        Assert.That(root.GetProperty("label").GetString(), Is.EqualTo("v"));
    }

    [Test]
    public void Compute_coalesce_yields_first_present_operand()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("name", LatticeValueTransform.Compute(
                LatticeComputeOperator.Coalesce,
                LatticeValueTransform.Member("nickname"),
                LatticeValueTransform.Member("given"),
                LatticeValueTransform.Const(LatticeConstant.Text("anon")))));

        var root = Evaluate("{\"given\":\"Grace\"}", transform);

        Assert.That(root.GetProperty("name").GetString(), Is.EqualTo("Grace"));
    }

    [Test]
    public void Compute_coalesce_falls_through_to_constant_when_all_members_missing()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("name", LatticeValueTransform.Compute(
                LatticeComputeOperator.Coalesce,
                LatticeValueTransform.Member("nickname"),
                LatticeValueTransform.Const(LatticeConstant.Text("anon")))));

        var root = Evaluate("{\"other\":1}", transform);

        Assert.That(root.GetProperty("name").GetString(), Is.EqualTo("anon"));
    }

    [Test]
    public void SetMember_missing_source_member_writes_json_null()
    {
        var transform = LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("x", LatticeValueTransform.Member("absent")));

        var root = Evaluate("{\"a\":1}", transform);

        Assert.That(root.GetProperty("x").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public void Evaluate_null_value_throws_invalid_operation()
    {
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(null, LatticeValueTransform.Passthrough()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void Evaluate_empty_value_throws_invalid_operation()
    {
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Array.Empty<byte>(), LatticeValueTransform.Passthrough()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void Evaluate_malformed_json_throws_invalid_operation()
    {
        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("{not json"), LatticeValueTransform.Passthrough()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void Evaluate_operations_on_non_object_root_throws_invalid_operation()
    {
        var transform = LatticeValueTransform.Passthrough(LatticeValueTransform.DropMember("a"));

        Assert.That(
            () => LatticeValueTransformEvaluation.Evaluate(Utf8("[1,2,3]"), transform),
            Throws.InvalidOperationException);
    }
}
