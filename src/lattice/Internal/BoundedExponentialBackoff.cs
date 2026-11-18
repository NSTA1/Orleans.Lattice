namespace Orleans.Lattice;

/// <summary>
/// The bounded-exponential back-off schedule shared by
/// <see cref="BoundedExponentialRetryPolicy"/> and the hand-rolled grain retry
/// loops that mirror it: <c>min(max, initial * 2^(attempt-1))</c>, computed
/// without overflow and clamped to the longest delay <see cref="Task.Delay(TimeSpan)"/>
/// accepts.
/// </summary>
internal static class BoundedExponentialBackoff
{
    /// <summary>
    /// The longest delay <see cref="Task.Delay(TimeSpan)"/> accepts
    /// (<c>uint.MaxValue - 1</c> milliseconds, about 49.7 days). A longer one
    /// throws <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    internal static readonly TimeSpan MaxTaskDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// Computes the back-off before the retry that follows the 1-based
    /// <paramref name="attempt"/>.
    /// </summary>
    /// <param name="attempt">The 1-based attempt that just failed.</param>
    /// <param name="initial">The delay before the first retry; non-negative.</param>
    /// <param name="max">The schedule's upper bound; at least <paramref name="initial"/>.</param>
    internal static TimeSpan Compute(int attempt, TimeSpan initial, TimeSpan max)
    {
        if (initial <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var bound = max > MaxTaskDelay ? MaxTaskDelay : max;
        var shift = attempt - 1;
        if (shift < 0)
        {
            shift = 0;
        }

        // Compare against max >> shift rather than multiplying, so a large
        // initial delay cannot wrap to a small or zero delay.
        if (shift >= 63 || initial.Ticks > (bound.Ticks >> shift))
        {
            return bound;
        }

        return TimeSpan.FromTicks(initial.Ticks << shift);
    }
}
