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
    /// <param name="attempt">The 1-based reconnect attempt.</param>
    internal static int ReconnectDelayMs(int attempt) =>
        attempt <= 1 ? 0 : Math.Min(100, 10 * attempt);
}
