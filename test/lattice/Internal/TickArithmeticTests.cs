namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="TickArithmetic"/>, the overflow-safe tick addition used to
/// schedule deadlines from operator-configurable intervals.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class TickArithmeticTests
{
    [Test]
    public void SaturatingAdd_clamps_a_positive_overflow_to_long_MaxValue()
    {
        Assert.That(TickArithmetic.SaturatingAdd(long.MaxValue - 10, 1_000L), Is.EqualTo(long.MaxValue));
    }

    [Test]
    public void SaturatingAdd_clamps_a_negative_overflow_to_long_MinValue()
    {
        Assert.That(TickArithmetic.SaturatingAdd(long.MinValue + 10, -1_000L), Is.EqualTo(long.MinValue));
    }

    [Test]
    public void SaturatingAdd_is_exact_when_the_sum_does_not_overflow()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TickArithmetic.SaturatingAdd(1_000L, 2_000L), Is.EqualTo(3_000L));
            Assert.That(TickArithmetic.SaturatingAdd(long.MaxValue, -1L), Is.EqualTo(long.MaxValue - 1));
        });
    }
}
