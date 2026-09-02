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

    // ===== StringComparison / culture overloads are rejected, not silently
    // pushed down as ordinal. The evaluator compares strings ordinally, so
    // accepting an overload that carries a StringComparison / CultureInfo would
    // silently evaluate a semantically different match than the compiled
    // lambda. Rejecting at translation time upholds the documented contract
    // that an unsupported construct throws rather than misleads. =====

    [Test]
    public void Translate_instance_equals_with_string_comparison_throws()
    {
        Assert.That(
            () => Translate(p => p.Name.Equals("bob", StringComparison.OrdinalIgnoreCase)),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_instance_startswith_with_string_comparison_throws()
    {
        Assert.That(
            () => Translate(p => p.Name.StartsWith("A", StringComparison.OrdinalIgnoreCase)),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_instance_endswith_with_string_comparison_throws()
    {
        Assert.That(
            () => Translate(p => p.Name.EndsWith("z", StringComparison.OrdinalIgnoreCase)),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_instance_contains_with_string_comparison_throws()
    {
        Assert.That(
            () => Translate(p => p.Name.Contains("b", StringComparison.OrdinalIgnoreCase)),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_static_equals_with_string_comparison_throws()
    {
        Assert.That(
            () => Translate(p => string.Equals(p.Name, "bob", StringComparison.OrdinalIgnoreCase)),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void Translate_instance_contains_char_overload_is_still_supported()
    {
        // The single-argument char overload carries no comparison modifier and
        // is evaluated ordinally, exactly matching the compiled lambda, so it
        // must keep translating rather than being caught by the rejection.
        var node = Translate(p => p.Name.Contains('b'));
        Assert.That(node.Kind, Is.EqualTo(LatticePredicateNodeKind.StringMethod));
        Assert.That(node.StringMethod, Is.EqualTo(LatticeStringMethod.Contains));
        Assert.That(node.Children![1].Constant.StringValue, Is.EqualTo("b"));
    }
}
