namespace Orleans.Lattice;

/// <summary>
/// The resume policy every resilient scan wrapper (tree scans, view scans and
/// system-tree scans) applies when an underlying stream faults and is reopened:
/// where the reopened stream starts, and how long to wait before reopening it.
/// Sharing it keeps every wrapper's resume behaviour identical.
/// </summary>
internal static class ResilientScanResume
{
    /// <summary>
    /// Computes the resume bounds for a resilient scan given the last successfully
    /// yielded key. Forward scans tighten the lower bound to the successor of
    /// <paramref name="lastKey"/> (<c>lastKey + "\u0000"</c>); reverse scans
    /// tighten the upper bound to <paramref name="lastKey"/> (exclusive). With no
    /// key yielded yet the original bounds are returned unchanged.
    /// </summary>
    /// <param name="originalStart">The scan's inclusive lower bound, or <see langword="null"/>.</param>
    /// <param name="originalEnd">The scan's exclusive upper bound, or <see langword="null"/>.</param>
    /// <param name="lastKey">The last key the scan yielded, or <see langword="null"/>.</param>
    /// <param name="reverse">Whether the scan runs in descending key order.</param>
    internal static (string? Start, string? End) Bounds(
        string? originalStart, string? originalEnd, string? lastKey, bool reverse)
    {
        if (lastKey is null)
        {
            return (originalStart, originalEnd);
        }

        return reverse
            ? (originalStart, lastKey)
            : (lastKey + "\u0000", originalEnd);
    }

    /// <summary>
    /// The inter-reconnect backoff: the first reconnect is immediate and
    /// subsequent attempts apply a small linear ramp capped at 100&#160;ms to
    /// avoid a tight loop against a persistently-faulting orchestrator.
    /// </summary>
    /// <param name="attempt">The 1-based reconnect attempt. Callers pass the
    /// scan's monotonic abort total, never its replenished consecutive count, so
    /// progress does not flatten the backoff back to its first rung.</param>
    internal static int ReconnectDelayMs(int attempt) =>
        attempt <= 1 ? 0 : Math.Min(100, 10 * attempt);

    /// <summary>
    /// Whether a resilient scan may reopen after an
    /// <c>EnumerationAbortedException</c> (issue 6080). Mirrors the stall split
    /// from issue 2539: <paramref name="consecutiveAborts"/> is futility
    /// accounting, reset whenever the scan yields a record, and is gated by the
    /// caller's <paramref name="budget"/>; <paramref name="totalAborts"/> is
    /// monotonic and gated only by <see cref="AbortCeiling"/>, a runaway
    /// backstop. A walk that progresses between aborts is therefore no longer
    /// capped by its length, while a futile walk still fails after
    /// <paramref name="budget"/> reopens and <c>budget == 0</c> still fails fast.
    /// </summary>
    /// <param name="consecutiveAborts">Reopens taken since the scan last yielded a record.</param>
    /// <param name="totalAborts">Reopens taken over the scan's lifetime.</param>
    /// <param name="budget">The caller's non-negative reconnect budget.</param>
    internal static bool MayReopenAfterAbort(int consecutiveAborts, int totalAborts, int budget) =>
        consecutiveAborts < budget && totalAborts < AbortCeiling(budget);

    /// <summary>
    /// The lifetime abort ceiling for a scan with the given reconnect
    /// <paramref name="budget"/>: <see cref="LatticeExtensions.DefaultScanReconnectCeiling"/>,
    /// raised to the budget itself when a caller asks for more, so raising
    /// <c>maxAttempts</c> never lowers a scan's tolerance.
    /// </summary>
    /// <param name="budget">The caller's non-negative reconnect budget.</param>
    internal static int AbortCeiling(int budget) =>
        Math.Max(budget, LatticeExtensions.DefaultScanReconnectCeiling);
}
