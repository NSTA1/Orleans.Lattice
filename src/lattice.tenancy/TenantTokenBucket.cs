namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A lock-free, allocation-free silo-local token bucket for one tenant, built on
/// the generic cell rate algorithm (GCRA). The whole bucket is a single
/// <c>long</c> theoretical-arrival-time (<c>TAT</c>) register advanced by an
/// atomic compare-exchange, so a per-op <see cref="TryAcquire"/> takes no lock and
/// allocates nothing: on the reject path a single volatile read and comparison,
/// on the admit path one compare-exchange plus one atomic demand increment.
/// </summary>
/// <remarks>
/// <para>
/// GCRA models a token bucket by a single virtual clock: the emission interval
/// <c>T</c> (<see cref="EmissionIntervalTicks"/>) is the sustained spacing between
/// operations, and the burst tolerance <c>tau</c>
/// (<see cref="BurstToleranceTicks"/>) is how far the theoretical arrival time may
/// run ahead of real time, which yields an immediate burst of
/// <c>tau / T + 1</c> operations before throttling engages. Both are expressed in
/// the timestamp ticks of the shared <see cref="TimeProvider"/>, so the bucket is
/// oblivious to wall-clock units and is driven deterministically in tests by an
/// injected timestamp.
/// </para>
/// <para>
/// The bucket's rate and burst are fixed at construction; the budget coordinator
/// replaces the bucket (at lease cadence, never on the hot path) when a tenant's
/// apportioned share changes.
/// </para>
/// </remarks>
internal sealed class TenantTokenBucket
{
    private readonly long _emissionIntervalTicks;
    private readonly long _burstToleranceTicks;

    // The theoretical arrival time, in TimeProvider timestamp ticks. Advanced by
    // one emission interval per admitted operation via compare-exchange. Started
    // at 0 (far below any live positive timestamp) so a fresh bucket grants its
    // full burst on first use.
    private long _tat;

    // The number of operations admitted since the last demand read. Read and
    // reset by the budget coordinator each lease cycle to drive demand-proportional
    // apportionment; never read on the hot path.
    private long _grantedSinceReset;

    /// <summary>
    /// Initializes a bucket with the given GCRA parameters, both in
    /// <see cref="TimeProvider"/> timestamp ticks.
    /// </summary>
    /// <param name="emissionIntervalTicks">The sustained spacing between operations (<c>T</c>); must be positive.</param>
    /// <param name="burstToleranceTicks">The burst tolerance (<c>tau</c>); <c>0</c> for strict spacing with no burst.</param>
    internal TenantTokenBucket(long emissionIntervalTicks, long burstToleranceTicks)
    {
        _emissionIntervalTicks = emissionIntervalTicks < 1 ? 1 : emissionIntervalTicks;
        _burstToleranceTicks = burstToleranceTicks < 0 ? 0 : burstToleranceTicks;
    }

    /// <summary>The sustained emission interval <c>T</c> in timestamp ticks.</summary>
    internal long EmissionIntervalTicks => _emissionIntervalTicks;

    /// <summary>The burst tolerance <c>tau</c> in timestamp ticks.</summary>
    internal long BurstToleranceTicks => _burstToleranceTicks;

    /// <summary>
    /// Attempts to admit one operation at logical time <paramref name="nowTicks"/>
    /// (a <see cref="TimeProvider.GetTimestamp"/> reading). Lock-free and
    /// allocation-free: it advances the theoretical arrival time by one emission
    /// interval under a compare-exchange, retrying only on a lost race.
    /// </summary>
    /// <param name="nowTicks">The current timestamp, from the shared time provider.</param>
    /// <returns><c>true</c> when admitted; <c>false</c> when throttled.</returns>
    internal bool TryAcquire(long nowTicks)
    {
        while (true)
        {
            var tat = Volatile.Read(ref _tat);

            // The earliest real time at which this operation is allowed: the
            // theoretical arrival time pulled back by the burst tolerance.
            var allowAt = tat - _burstToleranceTicks;
            if (nowTicks < allowAt)
            {
                return false;
            }

            // Advance from max(tat, now) so an idle bucket does not accrue credit
            // beyond its burst, then add one emission interval for this operation.
            var baseline = tat > nowTicks ? tat : nowTicks;
            var newTat = baseline + _emissionIntervalTicks;

            if (Interlocked.CompareExchange(ref _tat, newTat, tat) == tat)
            {
                Interlocked.Increment(ref _grantedSinceReset);
                return true;
            }
        }
    }

    /// <summary>
    /// Atomically reads and resets the number of operations admitted since the
    /// last call. Invoked by the budget coordinator at lease cadence only.
    /// </summary>
    /// <returns>The admitted-operation count since the previous read.</returns>
    internal long ReadAndResetDemand() => Interlocked.Exchange(ref _grantedSinceReset, 0);

    /// <summary>
    /// <c>true</c> when this bucket already carries the given GCRA parameters, so
    /// the coordinator can leave it in place (preserving its arrival-time state)
    /// rather than replacing it when the apportioned share is unchanged.
    /// </summary>
    /// <param name="emissionIntervalTicks">The candidate emission interval.</param>
    /// <param name="burstToleranceTicks">The candidate burst tolerance.</param>
    /// <returns><c>true</c> when both parameters match.</returns>
    internal bool Matches(long emissionIntervalTicks, long burstToleranceTicks) =>
        _emissionIntervalTicks == (emissionIntervalTicks < 1 ? 1 : emissionIntervalTicks)
        && _burstToleranceTicks == (burstToleranceTicks < 0 ? 0 : burstToleranceTicks);

    /// <summary>
    /// The emission interval <c>T</c> in timestamp ticks for a sustained
    /// <paramref name="ratePerSecond"/>: the timestamp frequency divided by the
    /// rate, floored at one tick.
    /// </summary>
    /// <param name="ratePerSecond">The sustained operations-per-second rate (floored at 1).</param>
    /// <param name="timestampFrequency">The <see cref="TimeProvider.TimestampFrequency"/> of the shared clock.</param>
    /// <returns>The emission interval in timestamp ticks (at least 1).</returns>
    internal static long ComputeEmissionIntervalTicks(long ratePerSecond, long timestampFrequency)
    {
        if (ratePerSecond < 1)
        {
            ratePerSecond = 1;
        }

        var interval = timestampFrequency / ratePerSecond;
        return interval < 1 ? 1 : interval;
    }

    /// <summary>
    /// The burst tolerance <c>tau</c> in timestamp ticks for a
    /// <paramref name="ratePerSecond"/> rate with a
    /// <paramref name="burstPercent"/> allowance: the number of burst tokens
    /// (<c>rate * burstPercent / 100</c>, at least one when the percent is
    /// positive) times the emission interval. A non-positive percent yields no
    /// tolerance (strict spacing).
    /// </summary>
    /// <param name="ratePerSecond">The sustained operations-per-second rate (floored at 1).</param>
    /// <param name="burstPercent">The burst allowance as a percentage of the rate.</param>
    /// <param name="timestampFrequency">The <see cref="TimeProvider.TimestampFrequency"/> of the shared clock.</param>
    /// <returns>The burst tolerance in timestamp ticks (<c>0</c> when no burst is allowed).</returns>
    internal static long ComputeBurstToleranceTicks(long ratePerSecond, int burstPercent, long timestampFrequency)
    {
        if (burstPercent <= 0)
        {
            return 0;
        }

        if (ratePerSecond < 1)
        {
            ratePerSecond = 1;
        }

        var burstTokens = (long)((UInt128)ratePerSecond * (uint)burstPercent / 100);
        if (burstTokens < 1)
        {
            burstTokens = 1;
        }

        return burstTokens * ComputeEmissionIntervalTicks(ratePerSecond, timestampFrequency);
    }
}
