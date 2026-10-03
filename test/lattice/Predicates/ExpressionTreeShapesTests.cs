using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Covers <see cref="ExpressionTreeShapes"/>, the structural expression-tree
/// queries shared by the expression translators.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ExpressionTreeShapesTests
{
    [Test]
    public void StripConversions_removes_every_nested_convert()
    {
        var operand = Expression.Constant(5);
        var wrapped = Expression.Convert(Expression.ConvertChecked(operand, typeof(long)), typeof(object));

        Assert.That(ExpressionTreeShapes.StripConversions(wrapped), Is.SameAs(operand));
    }

    [Test]
    public void StripConversions_leaves_other_unary_nodes_alone()
    {
        var negate = Expression.Negate(Expression.Constant(5));

        Assert.That(ExpressionTreeShapes.StripConversions(negate), Is.SameAs(negate));
    }

    [Test]
    public void ReferencesParameter_finds_the_parameter_through_the_recognised_shapes()
    {
        Expression<Func<string, bool>> uses = s => s.Length > 3 && s.StartsWith("a");
        Expression<Func<string, bool>> ignores = s => "x".Length > 3;

        Assert.Multiple(() =>
        {
            Assert.That(ExpressionTreeShapes.ReferencesParameter(uses.Body, uses.Parameters[0]), Is.True);
            Assert.That(ExpressionTreeShapes.ReferencesParameter(ignores.Body, ignores.Parameters[0]), Is.False);
            Assert.That(ExpressionTreeShapes.ReferencesParameter(null, uses.Parameters[0]), Is.False);
        });
    }

    [Test]
    public void ReferencesParameter_falls_back_to_a_full_walk_for_unrecognised_shapes()
    {
        Expression<Func<string, List<int>>> init = s => new List<int> { s.Length };
        Expression<Func<string, List<int>>> constant = s => new List<int> { 1 };

        Assert.Multiple(() =>
        {
            Assert.That(ExpressionTreeShapes.ReferencesParameter(init.Body, init.Parameters[0]), Is.True);
            Assert.That(ExpressionTreeShapes.ReferencesParameter(constant.Body, constant.Parameters[0]), Is.False);
        });
    }
}
