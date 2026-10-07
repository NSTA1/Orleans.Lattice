namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pacing and the wall-clock budget of the stale-routing retry loops on the
/// <see cref="ILattice"/> point and multi-key read paths (issue #4545).
/// <para>
/// A read that hits <see cref="StaleShardRoutingException"/> or
/// <see cref="StaleTreeRoutingException"/> invalidates its cached routing and
/// tries again. Most such signals clear within one refresh, so the first
/// <see cref="ImmediateRetries"/> retries run at once. A signal that persists -
/// a split source whose registry flip has not landed yet, or a leaf whose
/// reshard read gate is holding a key - used to be retried in a tight loop of
/// registry and shard calls. That loop ran for a 60 second budget, twice the
/// default 30 second response timeout, so a read that could not make progress
/// spun its routing activation for the caller's entire wait and surfaced as an
/// anonymous <see cref="TimeoutException"/> instead of the typed stale-routing
/// fault. Beyond the immediate retries each retry now waits, doubling from
/// <see cref="InitialBackoff"/> up to <see cref="MaxBackoff"/>, and the budget is
/// bounded below the response timeout, so the caller receives the typed fault
/// before its own request times out.
/// </para>
/// <para>
/// The core owns no <c>Task</c>, no wall-clock and no Orleans types, and
/// allocates nothing.
/// </para>
/// </summary>
internal static class StaleRoutingReadRetry
{
    /// <summary>Retries that run without a delay before backoff starts.</summary>
    internal const int ImmediateRetries = 2;

    /// <summary>The delay before the first retry that waits.</summary>
    internal static readonly TimeSpan InitialBackoff = TimeSpan.FromMilliseconds(1);

    /// <summary>The ceiling on the delay between two retries.</summary>
    internal static readonly TimeSpan MaxBackoff = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// The delay before stale-routing retry number <paramref name="retry"/>
    /// (zero-based) of one read: <see cref="TimeSpan.Zero"/> for the first
    /// <see cref="ImmediateRetries"/>, then <see cref="InitialBackoff"/> doubling
    /// per retry, capped at <see cref="MaxBackoff"/>.
    /// </summary>
    /// <param name="retry">The zero-based retry number.</param>
    public static TimeSpan Backoff(int retry)
    {
        if (retry < ImmediateRetries)
        {
            return TimeSpan.Zero;
        }

        var exponent = Math.Min(retry - ImmediateRetries, 16);
        var ticks = InitialBackoff.Ticks << exponent;
        return ticks >= MaxBackoff.Ticks ? MaxBackoff : TimeSpan.FromTicks(ticks);
    }

    /// <summary>
    /// The wall-clock budget of one read's stale-routing retry loop: five sixths
    /// of <paramref name="responseTimeout"/>, so the typed fault reaches the
    /// caller before its request times out, and never more than
    /// <paramref name="ceiling"/>. A non-positive or infinite response timeout
    /// leaves the ceiling in force.
    /// </summary>
    /// <param name="responseTimeout">The silo's grain-call response timeout.</param>
    /// <param name="ceiling">The upper bound, the write paths' budget.</param>
    public static TimeSpan Budget(TimeSpan responseTimeout, TimeSpan ceiling)
    {
        if (responseTimeout <= TimeSpan.Zero || responseTimeout == Timeout.InfiniteTimeSpan || responseTimeout >= ceiling)
        {
            return ceiling;
        }

        return TimeSpan.FromTicks(responseTimeout.Ticks / 6 * 5);
    }
}
