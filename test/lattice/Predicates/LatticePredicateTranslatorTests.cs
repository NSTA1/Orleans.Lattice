using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Unit tests for <see cref="LatticePredicateTranslator"/>: the client-side
/// lowering of an <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c> into the
/// allowlisted IR, including the unsupported-construct guard.
/// </summary>
[TestFixture]
public class LatticePredicateTranslatorTests
{
    [Test]
    public void Translate_null_predicate_throws()
    {
        Assert.That(
            () => LatticePredicateTranslator.Translate<PredicatePerson>(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Translate_member_comparison_lowers_to_compare_node()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 18);

        Assert.That(ir.Kind, Is.EqualTo(LatticePredicateNodeKind.Compare));
        Assert.That(ir.ComparisonOperator, Is.EqualTo(LatticeComparisonOperator.GreaterThanOrEqual));
        Assert.That(ir.Children, Is.Not.Null);
        Assert.That(ir.Children!.Length, Is.EqualTo(2));
        Assert.That(ir.Children[0].Kind, Is.EqualTo(LatticePredicateNodeKind.Member));
        Assert.That(ir.Children[0].MemberPath, Is.EqualTo("Age"));
        Assert.That(ir.Children[1].Kind, Is.EqualTo(LatticePredicateNodeKind.Constant));
        Assert.That(ir.Children[1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
        Assert.That(ir.Children[1].Constant.Int64Value, Is.EqualTo(18));
    }

    [Test]
    public void Translate_dotted_member_path_joins_segments()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Address!.City == "London");

        Assert.That(ir.Kind, Is.EqualTo(LatticePredicateNodeKind.Compare));
        Assert.That(ir.Children![0].MemberPath, Is.EqualTo("Address.City"));
        Assert.That(ir.Children[1].Constant.StringValue, Is.EqualTo("London"));
    }

    [Test]
    public void Translate_and_or_lowers_to_boolean_nodes()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= 18 && p.Active);

        Assert.That(ir.Kind, Is.EqualTo(LatticePredicateNodeKind.Boolean));
        Assert.That(ir.BooleanOperator, Is.EqualTo(LatticeBooleanOperator.And));
        Assert.That(ir.Children!.Length, Is.EqualTo(2));
    }

    [Test]
    public void Translate_not_lowers_to_boolean_not()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => !p.Active);

        Assert.That(ir.Kind, Is.EqualTo(LatticePredicateNodeKind.Boolean));
        Assert.That(ir.BooleanOperator, Is.EqualTo(LatticeBooleanOperator.Not));
        Assert.That(ir.Children!.Length, Is.EqualTo(1));
        Assert.That(ir.Children[0].Kind, Is.EqualTo(LatticePredicateNodeKind.Member));
    }

    [Test]
    public void Translate_bare_boolean_member_lowers_to_member_node()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Active);

        Assert.That(ir.Kind, Is.EqualTo(LatticePredicateNodeKind.Member));
        Assert.That(ir.MemberPath, Is.EqualTo("Active"));
    }

    [Test]
    public void Translate_string_methods_lower_to_string_method_nodes()
    {
        var starts = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Name.StartsWith("A"));
        var ends = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Name.EndsWith("z"));
        var contains = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Name.Contains("b"));
        var equals = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Name.Equals("Bob"));

        Assert.That(starts.Kind, Is.EqualTo(LatticePredicateNodeKind.StringMethod));
        Assert.That(starts.StringMethod, Is.EqualTo(LatticeStringMethod.StartsWith));
        Assert.That(ends.StringMethod, Is.EqualTo(LatticeStringMethod.EndsWith));
        Assert.That(contains.StringMethod, Is.EqualTo(LatticeStringMethod.Contains));
        Assert.That(equals.StringMethod, Is.EqualTo(LatticeStringMethod.Equals));
    }

    [Test]
    public void Translate_captured_local_is_evaluated_to_constant()
    {
        int threshold = 21;
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age > threshold);

        Assert.That(ir.Children![1].Kind, Is.EqualTo(LatticePredicateNodeKind.Constant));
        Assert.That(ir.Children[1].Constant.Int64Value, Is.EqualTo(21));
    }

    [Test]
    public void Translate_double_constant_captured_as_double()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Score < 0.5);

        Assert.That(ir.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Double));
        Assert.That(ir.Children[1].Constant.DoubleValue, Is.EqualTo(0.5));
    }

    [Test]
    public void Translate_null_comparison_captures_null_constant()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Nickname == null);

        Assert.That(ir.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Null));
    }

    [Test]
    public void Translate_unsupported_method_throws_NotSupported()
    {
        Expression<Func<PredicatePerson, bool>> predicate = p => p.Name.ToUpper() == "BOB";

        Assert.That(
            () => LatticePredicateTranslator.Translate(predicate),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_unsupported_arithmetic_throws_NotSupported()
    {
        Expression<Func<PredicatePerson, bool>> predicate = p => p.Age + 1 > 18;

        Assert.That(
            () => LatticePredicateTranslator.Translate(predicate),
            Throws.TypeOf<NotSupportedException>());
    }
}
