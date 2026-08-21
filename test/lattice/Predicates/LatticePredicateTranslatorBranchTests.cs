using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Branch-coverage tests for <see cref="LatticePredicateTranslator"/> targeting
/// the unsupported-expression rejection paths and the less-common
/// constant-capture type conversions that the round-trip oracle suite does not
/// reach. Rejections must surface as <see cref="NotSupportedException"/>;
/// captured literals must normalise to the documented wire kinds.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticePredicateTranslatorBranchTests
{
    private static LatticePredicateNode Translate(Expression<Func<PredicatePerson, bool>> predicate) =>
        LatticePredicateTranslator.Translate(predicate);

    [Test]
    public void Translate_unsupported_binary_operator_in_boolean_position_throws()
    {
        Assert.That(() => Translate(p => p.Active ^ true), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_constant_true_body_produces_boolean_constant()
    {
        var node = Translate(p => true);
        Assert.That(node.Kind, Is.EqualTo(LatticePredicateNodeKind.Constant));
        Assert.That(node.Constant.Kind, Is.EqualTo(LatticeConstantKind.Boolean));
        Assert.That(node.Constant.BooleanValue, Is.True);
    }

    [Test]
    public void Translate_conditional_expression_in_boolean_position_throws()
    {
        Assert.That(() => Translate(p => p.Age > 0 ? true : false), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_bool_method_on_non_string_type_throws()
    {
        Assert.That(() => Translate(p => Equals(p.Name, "x")), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_unknown_string_method_throws()
    {
        Assert.That(() => Translate(p => string.IsNullOrEmpty(p.Name)), Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_static_string_equals_maps_to_equals_method()
    {
        var node = Translate(p => string.Equals(p.Name, "Bob"));
        Assert.That(node.Kind, Is.EqualTo(LatticePredicateNodeKind.StringMethod));
        Assert.That(node.StringMethod, Is.EqualTo(LatticeStringMethod.Equals));
        Assert.That(node.Children![0].Kind, Is.EqualTo(LatticePredicateNodeKind.Member));
        Assert.That(node.Children![1].Constant.StringValue, Is.EqualTo("Bob"));
    }

    [Test]
    public void Translate_captured_bool_constant_normalises_to_boolean()
    {
        var flag = true;
        var node = Translate(p => p.Active == flag);
        Assert.That(node.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Boolean));
    }

    [Test]
    public void Translate_captured_char_constant_normalises_to_string()
    {
        var ch = 'A';
        var node = Translate(p => p.Age == ch);
        Assert.That(node.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.String));
        Assert.That(node.Children![1].Constant.StringValue, Is.EqualTo("A"));
    }

    [Test]
    public void Translate_captured_ulong_constant_normalises_to_integer()
    {
        var u = 5UL;
        var node = Translate(p => p.Score == u);
        Assert.That(node.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
        Assert.That(node.Children![1].Constant.Int64Value, Is.EqualTo(5));
    }

    [Test]
    public void Translate_captured_decimal_constant_normalises_to_double()
    {
        var d = 1.5m;
        var node = Translate(p => p.Age == d);
        Assert.That(node.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Double));
        Assert.That(node.Children![1].Constant.DoubleValue, Is.EqualTo(1.5));
    }

    [Test]
    public void Translate_captured_enum_constant_normalises_to_integer()
    {
        var day = DayOfWeek.Wednesday;
        var node = Translate(p => p.Age == (int)day);
        Assert.That(node.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
        Assert.That(node.Children![1].Constant.Int64Value, Is.EqualTo((long)DayOfWeek.Wednesday));
    }
}
