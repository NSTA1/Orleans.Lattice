namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The cadence gate the replication maintenance grains apply to each periodic
/// phase of their tick.
/// </summary>
internal static class ReplicationCadence
{
    /// <summary>
    /// Returns <see langword="true"/> when a phase last run at
    /// <paramref name="lastTicks"/> is due again at <paramref name="nowTicks"/>: a
    /// phase that has never run (<paramref name="lastTicks"/> is zero) fires on
    /// the first tick, and otherwise once <paramref name="interval"/> has elapsed.
    /// </summary>
    /// <param name="nowTicks">The current time, in ticks.</param>
    /// <param name="lastTicks">When the phase last ran, in ticks, or zero if never.</param>
    /// <param name="interval">The phase's configured interval.</param>
    internal static bool IsDue(long nowTicks, long lastTicks, TimeSpan interval)
    {
        if (lastTicks == 0)
        {
            return true; // Never run before - fire on first tick.
        }
        return nowTicks - lastTicks >= interval.Ticks;
    }
}
