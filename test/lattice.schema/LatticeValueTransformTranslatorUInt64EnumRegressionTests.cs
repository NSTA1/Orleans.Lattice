namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Regression tests for lowering an enum constant whose underlying type is
/// <see cref="ulong"/>. The transform translator captured every enum via
/// <c>Integer(Convert.ToInt64(e))</c>, which throws <see cref="OverflowException"/>
/// for a member above <see cref="long.MaxValue"/>, so any transform assigning
/// such an enum failed to lower at all. A ulong-backed enum must be captured
/// exactly as the sibling plain-<see cref="ulong"/> constant is: a <c>Double</c>
/// above <see cref="long.MaxValue"/>, an exact integer within it.
/// </summary>
[TestFixture]
public sealed class LatticeValueTransformTranslatorUInt64EnumRegressionTests
{
    private enum WideEnum : ulong
    {
        Low = 1,
        AboveLongMax = (ulong)long.MaxValue + 1UL,
        Max = ulong.MaxValue,
    }

    private sealed record Source;

    private sealed record EnumTarget
    {
        public WideEnum Kind { get; init; }
    }

    private static LatticeConstant Capture(WideEnum value)
    {
        // On the unfixed translator this throws OverflowException during Translate.
        var ir = LatticeValueTransformTranslator.Translate<Source, EnumTarget>(
            _ => new EnumTarget { Kind = value });
        return ir.Children!.Single(o => o.MemberPath == "Kind").Children![0].Constant;
    }

    [Test]
    public void Translate_ulong_backed_enum_above_long_max_captures_double()
    {
        var constant = Capture(WideEnum.AboveLongMax);

        Assert.Multiple(() =>
        {
            Assert.That(constant.Kind, Is.EqualTo(LatticeConstantKind.Double));
            Assert.That(constant.DoubleValue, Is.EqualTo((double)((ulong)long.MaxValue + 1UL)));
        });
    }

    [Test]
    public void Translate_ulong_backed_enum_max_value_captures_double_not_corrupted_integer()
    {
        var constant = Capture(WideEnum.Max);

        Assert.Multiple(() =>
        {
            Assert.That(constant.Kind, Is.EqualTo(LatticeConstantKind.Double));
            Assert.That(constant.DoubleValue, Is.EqualTo((double)ulong.MaxValue));
            Assert.That(constant.Int64Value, Is.Not.EqualTo(-1L), "ulong.MaxValue must not wrap to -1");
        });
    }

    [Test]
    public void Translate_ulong_backed_enum_small_value_stays_exact_integer()
    {
        var constant = Capture(WideEnum.Low);

        Assert.Multiple(() =>
        {
            Assert.That(constant.Kind, Is.EqualTo(LatticeConstantKind.Int64));
            Assert.That(constant.Int64Value, Is.EqualTo(1L));
        });
    }
}
