namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Pure scheduling helper for the anti-entropy digest-probe cadence.
/// Factored out so the jitter calculation can be unit-tested without
/// standing up a grain activation.
/// </summary>
internal static class DigestProbeScheduling
{
    /// <summary>
    /// Applies a symmetric multiplicative jitter to <paramref name="interval"/>.
    /// The result is scaled by a random factor in
    /// <c>[1 - jitter, 1 + jitter]</c> drawn from <paramref name="random"/>,
    /// clamped to a minimum of one tick. A <paramref name="jitter"/> of
    /// <c>0</c> returns <paramref name="interval"/> unchanged.
    /// </summary>
    /// <param name="interval">The base interval.</param>
    /// <param name="jitter">The jitter fraction in <c>[0.0, 1.0]</c>.</param>
    /// <param name="random">The random source.</param>
    /// <returns>The jittered interval.</returns>
    public static TimeSpan ApplyJitter(TimeSpan interval, double jitter, Random random)
    {
        ArgumentNullException.ThrowIfNull(random);
        if (jitter <= 0.0 || interval <= TimeSpan.Zero)
        {
            return interval;
        }

        var factor = 1.0 + (((random.NextDouble() * 2.0) - 1.0) * jitter);
        var ticks = (long)(interval.Ticks * factor);
        return TimeSpan.FromTicks(Math.Max(1L, ticks));
    }
}
