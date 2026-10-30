using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// The <see cref="ReadOnlyMemory{T}"/> overload of the internal evaluator, which a
/// transform remediation runs directly over a stored value's body past its
/// schema-version envelope header so the body is never copied out (issue #5243). It
/// must produce byte-identical output to the array overload, including through a
/// conditional, whose predicate needs the input as an array.
/// </summary>
public sealed partial class LatticeValueTransformEvaluatorTests
{
    private static LatticeValueTransform TierByAge() =>
        LatticeValueTransform.Passthrough(
            LatticeValueTransform.SetMember("tier", LatticeValueTransform.Conditional(
                LatticePredicateNode.Compare(
                    LatticeComparisonOperator.GreaterThanOrEqual,
                    LatticePredicateNode.Member("age"),
                    LatticePredicateNode.Const(LatticeConstant.Integer(18))),
                LatticeValueTransform.Const(LatticeConstant.Text("adult")),
                LatticeValueTransform.Const(LatticeConstant.Text("minor")))),
            LatticeValueTransform.SetMember("checked", LatticeValueTransform.Conditional(
                LatticePredicateNode.Compare(
                    LatticeComparisonOperator.GreaterThanOrEqual,
                    LatticePredicateNode.Member("age"),
                    LatticePredicateNode.Const(LatticeConstant.Integer(65))),
                LatticeValueTransform.Const(LatticeConstant.Text("senior")),
                LatticeValueTransform.Const(LatticeConstant.Text("no")))));

    [TestCase("{\"age\":21}")]
    [TestCase("{\"age\":70}")]
    [TestCase("{\"age\":3}")]
    public void Evaluate_over_a_view_into_an_enveloped_value_matches_the_array_overload(string json)
    {
        var body = Encoding.UTF8.GetBytes(json);
        var enveloped = LatticeSchemaEnvelope.Encode(1, 1, body);
        var transform = TierByAge();

        var fromView = LatticeValueTransformEvaluator.Evaluate(
            enveloped.AsMemory(LatticeSchemaEnvelope.HeaderLength), in transform);
        var fromArray = LatticeValueTransformEvaluator.Evaluate(body, in transform);

        Assert.That(fromView, Is.EqualTo(fromArray));
    }

    [Test]
    public void Evaluate_over_an_empty_view_throws_InvalidOperationException()
    {
        var enveloped = LatticeSchemaEnvelope.Encode(1, 1, ReadOnlySpan<byte>.Empty);

        Assert.That(
            () => LatticeValueTransformEvaluator.Evaluate(
                enveloped.AsMemory(LatticeSchemaEnvelope.HeaderLength), LatticeValueTransform.Passthrough()),
            Throws.InstanceOf<InvalidOperationException>());
    }
}
