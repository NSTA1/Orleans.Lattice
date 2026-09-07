using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Pins the fold that reads a predicate's constant side without invoking the
/// expression compiler.
/// <para>
/// The reader replaces a general mechanism (build a lambda, compile it, invoke
/// the delegate) with a cheaper one on the shapes that dominate (a constant, or
/// a field or property chain rooted in one). What matters is therefore that the
/// cheap path and the general path <b>agree</b> - including on the awkward roots
/// that decide which of the two runs - and that anything outside the closed set
/// still reaches the compiler rather than being silently mis-read.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExpressionConstantReaderTests
{
    private const int SharedThreshold = 18;

    private static string StaticName => "static-name";

    [Test]
    public void TryRead_reads_a_bare_constant()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(Expression.Constant(42), out object? value), Is.True);
            Assert.That(value, Is.EqualTo(42));
        });
    }

    [Test]
    public void TryRead_reads_a_null_constant_as_null_rather_than_failing()
    {
        // A null value and an unreadable shape both leave 'value' null, so the
        // boolean is the only thing that separates them. A captured null bound
        // is legitimate and must read as a successful null.
        string? absent = null;

        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Name == absent), out object? value), Is.True);
            Assert.That(value, Is.Null);
        });
    }

    [Test]
    public void TryRead_reads_a_captured_local_without_compiling()
    {
        int bound = 30;

        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Age >= bound), out object? value), Is.True);
            Assert.That(value, Is.EqualTo(30));
        });
    }

    [Test]
    public void TryRead_reads_a_chain_two_members_deep()
    {
        var bound = new Bound { Limit = 41, Label = "deep" };

        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Name == bound.Label), out object? value), Is.True);
            Assert.That(value, Is.EqualTo("deep"));
        });
    }

    [Test]
    public void TryRead_reads_a_static_field()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Age >= SharedThreshold), out object? value), Is.True);
            Assert.That(value, Is.EqualTo(18));
        });
    }

    [Test]
    public void TryRead_reads_a_static_property_through_its_getter()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Name == StaticName), out object? value), Is.True);
            Assert.That(value, Is.EqualTo("static-name"));
        });
    }

    [Test]
    public void TryRead_unwraps_a_widening_conversion_around_a_captured_bound()
    {
        int bound = 7;

        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Score >= bound), out object? value), Is.True);
            Assert.That(value, Is.EqualTo(7));
        });
    }

    [Test]
    public void TryRead_declines_a_method_call()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Name == "gb".ToUpperInvariant()), out object? value), Is.False);
            Assert.That(value, Is.Null);
        });
    }

    [Test]
    public void TryRead_declines_arithmetic_over_a_captured_bound()
    {
        int bound = 10;

        Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Age >= bound + 5), out _), Is.False);
    }

    [Test]
    public void TryRead_declines_an_indexed_property()
    {
        // An indexer needs arguments the reader has no way to supply, so it must
        // decline rather than attempt the read.
        var values = new Dictionary<string, string> { ["k"] = "v" };
        var expression = Expression.Property(
            Expression.Constant(values),
            typeof(Dictionary<string, string>).GetProperty("Item")!,
            Expression.Constant("k"));

        Assert.That(ExpressionConstantReader.TryRead(expression, out _), Is.False);
    }

    [Test]
    public void TryRead_declines_an_instance_member_over_a_null_root()
    {
        // Reading off a null target would throw here, where the compiled
        // delegate throws the same fault at the same point. It is left to the
        // fallback so the reader never introduces an exception of its own.
        Bound? bound = null;

        Assert.That(ExpressionConstantReader.TryRead(ConstantSide(p => p.Name == bound!.Label), out _), Is.False);
    }

    [Test]
    public void Read_falls_back_to_compiling_a_shape_it_cannot_fold()
    {
        Assert.That(ExpressionConstantReader.Read(ConstantSide(p => p.Name == "gb".ToUpperInvariant())), Is.EqualTo("GB"));
    }

    [Test]
    public void Read_agrees_with_compiling_across_every_supported_root()
    {
        int local = 3;
        var bound = new Bound { Limit = 41, Label = "deep" };

        var cases = new Expression[]
        {
            ConstantSide(p => p.Age >= 21),
            ConstantSide(p => p.Age >= local),
            ConstantSide(p => p.Name == bound.Label),
            ConstantSide(p => p.Age >= bound.Limit),
            ConstantSide(p => p.Age >= SharedThreshold),
            ConstantSide(p => p.Name == StaticName),
            ConstantSide(p => p.Name == "gb".ToUpperInvariant()),
            ConstantSide(p => p.Age >= local + 5),
        };

        foreach (var expression in cases)
        {
            object? compiled = Expression.Lambda(expression).Compile().DynamicInvoke();
            Assert.That(ExpressionConstantReader.Read(expression), Is.EqualTo(compiled), expression.ToString());
        }
    }

    [Test]
    public void Translator_captures_a_captured_local_bound_as_its_literal()
    {
        // The reader's caller. The translated IR must carry the bound's value,
        // not a reference to the display class that held it.
        int bound = 30;

        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Age >= bound);

        Assert.Multiple(() =>
        {
            Assert.That(ir.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
            Assert.That(ir.Children[1].Constant.Int64Value, Is.EqualTo(30));
        });
    }

    [Test]
    public void Translator_captures_a_nested_chain_bound_as_its_literal()
    {
        var bound = new Bound { Limit = 41, Label = "deep" };

        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Name == bound.Label);

        Assert.Multiple(() =>
        {
            Assert.That(ir.Children![1].Constant.Kind, Is.EqualTo(LatticeConstantKind.String));
            Assert.That(ir.Children[1].Constant.StringValue, Is.EqualTo("deep"));
        });
    }

    [Test]
    public void Translator_captures_a_bound_it_must_still_compile()
    {
        var ir = LatticePredicateTranslator.Translate<PredicatePerson>(p => p.Name == "gb".ToUpperInvariant());

        Assert.That(ir.Children![1].Constant.StringValue, Is.EqualTo("GB"));
    }

    /// <summary>The constant side of a two-sided comparison.</summary>
    private static Expression ConstantSide(Expression<Func<PredicatePerson, bool>> predicate) =>
        ((BinaryExpression)predicate.Body).Right;

    private sealed class Bound
    {
        internal int Limit { get; init; }

        internal string Label { get; init; } = string.Empty;
    }
}
