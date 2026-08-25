namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The pure hysteresis gate that decides whether a freshly rolled-up local usage
/// sample has moved enough, relative to the last published sample, to justify
/// republishing the cluster's usage slot. Publishing is comparatively expensive
/// (an optimistic-concurrency merge into the registry-backed usage tree), so the
/// cadence roll-up only republishes when some dimension's movement clears a
/// significance band that is the larger of an absolute floor and a relative
/// fraction of the last published value; a stream of negligible movements is
/// suppressed.
/// </summary>
internal static class UsagePublishHysteresis
{
    /// <summary>
    /// Returns <c>true</c> when <paramref name="candidate"/> differs from
    /// <paramref name="lastPublished"/> on any dimension by at least that
    /// dimension's significance band - the larger of the absolute floor
    /// (<paramref name="minAbsoluteDelta"/>) and the relative fraction
    /// (<paramref name="minRelativeDelta"/> of the last value) - so the cluster
    /// should republish. A movement below the band on every dimension is
    /// suppressed. A pure function of its inputs, so it is deterministic and free
    /// of timing.
    /// </summary>
    /// <param name="lastPublished">The last published sample (<see cref="LocalUsageSample.Empty"/> when none).</param>
    /// <param name="candidate">The freshly rolled-up candidate sample.</param>
    /// <param name="minAbsoluteDelta">The absolute per-dimension floor. Negative values are treated as zero.</param>
    /// <param name="minRelativeDelta">The relative per-dimension fraction of the last value. Negative values are treated as zero.</param>
    /// <returns><c>true</c> when the candidate should be republished.</returns>
    internal static bool ShouldPublish(
        LocalUsageSample lastPublished,
        LocalUsageSample candidate,
        long minAbsoluteDelta,
        double minRelativeDelta)
    {
        var absolute = minAbsoluteDelta < 0 ? 0 : minAbsoluteDelta;
        var relative = minRelativeDelta < 0 ? 0 : minRelativeDelta;

        return DimensionMoved(lastPublished.Bytes, candidate.Bytes, absolute, relative)
            || DimensionMoved(lastPublished.Keys, candidate.Keys, absolute, relative)
            || DimensionMoved(lastPublished.MemoryBytes, candidate.MemoryBytes, absolute, relative)
            || DimensionMoved(lastPublished.TreeCount, candidate.TreeCount, absolute, relative);
    }

    private static bool DimensionMoved(long last, long candidate, long absolute, double relative)
    {
        var delta = Math.Abs(candidate - last);
        if (delta == 0)
        {
            return false;
        }

        var relativeThreshold = relative * Math.Abs(last);
        var threshold = Math.Max(absolute, relativeThreshold);
        return delta >= threshold;
    }
}
