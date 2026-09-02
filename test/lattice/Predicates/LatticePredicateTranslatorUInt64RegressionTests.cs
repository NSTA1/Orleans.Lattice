using System.Linq.Expressions;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Regression tests for the capture of a <see cref="ulong"/> constant whose
/// value exceeds <see cref="long.MaxValue"/>. The translator previously stored
/// such a literal as <c>Integer(unchecked((long)ul))</c>, silently corrupting
/// (for example) <see cref="ulong.MaxValue"/> into <c>-1</c> and producing a
/// pushed-down predicate that never matches the intended documents. Values that
/// exceed <see cref="long.MaxValue"/> must be captured as a <c>Double</c> - the
/// same representation the evaluator already reads a large <see cref="ulong"/>
/// JSON member as - so a translate-then-evaluate round trip agrees with the
/// compiled lambda; values within <see cref="long.MaxValue"/> must remain exact
/// integers.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticePredicateTranslatorUInt64RegressionTests
{
    private static LatticePredicateNode Translate(Expression<Func<PredicatePerson, bool>> predicate) =>
        LatticePredicateTranslator.Translate(predicate);

    [Test]
    public void Translate_ulong_max_value_captures_double_not_corrupted_integer()
    {
        var u = ulong.MaxValue;
        var node = Translate(p => p.Score == u);
        var constant = node.Children![1].Constant;

        Assert.Multiple(() =>
        {
            Assert.That(constant.Kind, Is.EqualTo(LatticeConstantKind.Double));
            Assert.That(constant.DoubleValue, Is.EqualTo((double)ulong.MaxValue));
            Assert.That(constant.Int64Value, Is.Not.EqualTo(-1L));
        });
    }

    [Test]
    public void Translate_ulong_just_above_long_max_captures_double()
    {
        var u = (ulong)long.MaxValue + 1UL;
        var node = Translate(p => p.Score == u);
        var constant = node.Children![1].Constant;

        Assert.Multiple(() =>
        {
            Assert.That(constant.Kind, Is.EqualTo(LatticeConstantKind.Double));
            Assert.That(constant.DoubleValue, Is.EqualTo((double)((ulong)long.MaxValue + 1UL)));
        });
    }

    [Test]
    public void Translate_ulong_at_long_max_boundary_stays_exact_integer()
    {
        var u = (ulong)long.MaxValue;
        var node = Translate(p => p.Score == u);
        var constant = node.Children![1].Constant;

        Assert.Multiple(() =>
        {
            Assert.That(constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
            Assert.That(constant.Int64Value, Is.EqualTo(long.MaxValue));
        });
    }

    [Test]
    public void Evaluate_ulong_max_value_matches_equal_member()
    {
        var u = ulong.MaxValue;
        var person = new PredicatePerson("x", 0, true, (double)ulong.MaxValue, null, null);
        var ir = Translate(p => p.Score == u);
        var bytes = JsonLatticeSerializer<PredicatePerson>.Default.Serialize(person);

        Assert.That(LatticePredicateEvaluator.Matches(bytes, ir), Is.True);
    }
}
