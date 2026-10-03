namespace Orleans.Lattice;

/// <summary>
/// Overflow-safe tick arithmetic for deadlines and expiries built from
/// operator-configurable intervals, so an extreme configured interval saturates
/// to the far end of the range instead of wrapping to a past instant.
/// </summary>
internal static class TickArithmetic
{
    /// <summary>
    /// <c>a + b</c>, clamped to <see cref="long.MaxValue"/> /
    /// <see cref="long.MinValue"/> instead of wrapping.
    /// </summary>
    /// <param name="a">The first addend, in ticks.</param>
    /// <param name="b">The second addend, in ticks.</param>
    internal static long SaturatingAdd(long a, long b)
    {
        var sum = unchecked(a + b);
        // Overflow iff the operands share a sign that the result does not.
        if (((a ^ sum) & (b ^ sum)) < 0)
        {
            return b < 0 ? long.MinValue : long.MaxValue;
        }
        return sum;
    }
}
