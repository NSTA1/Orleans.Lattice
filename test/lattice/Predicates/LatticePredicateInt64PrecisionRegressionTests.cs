using System.Text;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Regression tests for 64-bit integer precision in
/// <see cref="LatticePredicateEvaluator"/>. The evaluator once funnelled every
/// number - both a JSON integer member and an <c>Int64</c> constant - through
/// <see cref="double"/>, so two longs that differ only above 2^53 (the limit of
/// exact double representation) collapsed to the same value and compared equal.
/// That silently mis-matched 64-bit identifiers such as snowflake IDs. These
/// tests pin exact Int64 comparison on both the Utf8JsonReader fast path (a
/// single top-level member) and the JsonDocument slow path (a dotted member),
/// while confirming a mixed integer/real comparison still folds through double.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticePredicateInt64PrecisionRegressionTests
{
    // 2^53 and its immediate successor: distinct as Int64, identical as double.
    private const long PowTwo53 = 9007199254740992L;
    private const long PowTwo53Plus1 = 9007199254740993L;

    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    private static bool Match(byte[] value, LatticePredicateNode node) =>
        LatticePredicateEvaluator.Matches(value, node);

    [Test]
    public void Fast_path_adjacent_longs_above_2p53_are_not_equal()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Id"),
            LatticePredicateNode.Const(LatticeConstant.Integer(PowTwo53Plus1)));

        // The member is 2^53; the constant is 2^53 + 1. They are distinct
        // longs, so the predicate must not match, even though both round to the
        // same double.
        Assert.That(Match(Json($"{{\"Id\":{PowTwo53}}}"), node), Is.False);
    }

    [Test]
    public void Fast_path_equal_longs_above_2p53_are_equal()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Id"),
            LatticePredicateNode.Const(LatticeConstant.Integer(PowTwo53Plus1)));

        Assert.That(Match(Json($"{{\"Id\":{PowTwo53Plus1}}}"), node), Is.True);
    }

    [Test]
    public void Fast_path_ordering_distinguishes_adjacent_longs_above_2p53()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThan,
            LatticePredicateNode.Member("Id"),
            LatticePredicateNode.Const(LatticeConstant.Integer(PowTwo53)));

        // 2^53 + 1 is strictly greater than 2^53; as doubles they are equal, so
        // the pre-fix ordering wrongly returned false.
        Assert.That(Match(Json($"{{\"Id\":{PowTwo53Plus1}}}"), node), Is.True);
    }

    [Test]
    public void Fast_path_long_max_neighbour_is_not_equal()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Id"),
            LatticePredicateNode.Const(LatticeConstant.Integer(long.MaxValue - 1)));

        // long.MaxValue and long.MaxValue - 1 both round to 2^63 as doubles.
        Assert.That(Match(Json($"{{\"Id\":{long.MaxValue}}}"), node), Is.False);
    }

    [Test]
    public void Slow_path_dotted_member_adjacent_longs_above_2p53_are_not_equal()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Rec.Id"),
            LatticePredicateNode.Const(LatticeConstant.Integer(PowTwo53Plus1)));

        // A dotted path forces the JsonDocument slow path (FromJson).
        Assert.That(Match(Json($"{{\"Rec\":{{\"Id\":{PowTwo53}}}}}"), node), Is.False);
    }

    [Test]
    public void Mixed_integer_member_and_real_constant_still_compare_by_value()
    {
        var equal = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("N"),
            LatticePredicateNode.Const(LatticeConstant.Real(5.0)));
        var atLeast = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("N"),
            LatticePredicateNode.Const(LatticeConstant.Real(4.5)));

        // The member serialises as the integer 5; comparing it to a real
        // constant must still fold through double so 5 == 5.0 and 5 >= 4.5.
        Assert.That(Match(Json("{\"N\":5}"), equal), Is.True);
        Assert.That(Match(Json("{\"N\":5}"), atLeast), Is.True);
    }
}
