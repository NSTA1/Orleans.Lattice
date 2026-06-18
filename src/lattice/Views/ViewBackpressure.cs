namespace Orleans.Lattice.Views;

/// <summary>
/// Pure mapping from the source tree's <see cref="WalSaturationState"/> onto the
/// view maintainer's self-throttle decisions: a scaled-down per-pass drain batch
/// and a background-tick deferral. Kept free of grain / signal state so the
/// arithmetic can be unit-tested in isolation; the maintainer supplies the live
/// regime and the per-view <see cref="LatticeViewOptions"/> tuning.
/// </summary>
internal static class ViewBackpressure
{
    /// <summary>
    /// Returns the effective per-pass drain batch for <paramref name="state"/>.
    /// <see cref="WalSaturationState.Healthy"/> drains the full
    /// <paramref name="batchSize"/>; <see cref="WalSaturationState.Throttled"/>
    /// drains <c>ceil(batchSize * ratio)</c> (the ratio clamped to <c>[0, 1]</c>);
    /// <see cref="WalSaturationState.Saturated"/> drains
    /// <paramref name="saturatedBatchSize"/>. Every result is clamped to the closed
    /// interval <c>[1, batchSize]</c> so back-pressure can only shrink, never inflate,
    /// the batch and never starves a pass to zero.
    /// </summary>
    /// <param name="state">The observed source saturation regime.</param>
    /// <param name="batchSize">The configured full-rate batch size (assumed positive).</param>
    /// <param name="throttledRatio">Fraction of <paramref name="batchSize"/> to drain while throttled.</param>
    /// <param name="saturatedBatchSize">Absolute drip-feed batch to drain while saturated.</param>
    /// <returns>The clamped effective batch size.</returns>
    public static int ScaleBatch(WalSaturationState state, int batchSize, double throttledRatio, int saturatedBatchSize)
    {
        var ceiling = Math.Max(1, batchSize);
        return state switch
        {
            WalSaturationState.Throttled => Math.Clamp(
                (int)Math.Ceiling(ceiling * Math.Clamp(throttledRatio, 0d, 1d)),
                1,
                ceiling),
            WalSaturationState.Saturated => Math.Clamp(saturatedBatchSize, 1, ceiling),
            _ => ceiling,
        };
    }

    /// <summary>
    /// Returns the milliseconds the maintainer should skip background drain ticks
    /// for after a pass that observed <paramref name="state"/>:
    /// <paramref name="throttledPauseMs"/> while throttled,
    /// <paramref name="saturatedPauseMs"/> while saturated, and <c>0</c> (no
    /// deferral) while healthy. A configured pause less than or equal to zero is
    /// returned as <c>0</c> (deferral disabled, batch scaling still applies).
    /// </summary>
    /// <param name="state">The observed source saturation regime.</param>
    /// <param name="throttledPauseMs">Configured deferral while throttled.</param>
    /// <param name="saturatedPauseMs">Configured deferral while saturated.</param>
    /// <returns>The non-negative deferral in milliseconds.</returns>
    public static int PauseMs(WalSaturationState state, int throttledPauseMs, int saturatedPauseMs)
    {
        var configured = state switch
        {
            WalSaturationState.Throttled => throttledPauseMs,
            WalSaturationState.Saturated => saturatedPauseMs,
            _ => 0,
        };

        return configured > 0 ? configured : 0;
    }
}
