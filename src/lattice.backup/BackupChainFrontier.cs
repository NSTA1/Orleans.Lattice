namespace Orleans.Lattice.Backup;

/// <summary>
/// The dependency-free rules a backup chain applies to what it records about the
/// writes it captured: the per-origin provenance high-water and the chain's HLC
/// frontier. Both capture collectors (<see cref="RawEntryCollector"/> and
/// <see cref="IncrementalDeltaCollector"/>) and the capture service's consistency
/// cuts route through it, so the rules the backup specification checks
/// (<c>spec/backup/BackupProvenance.tla</c>) are the ones that run.
/// </summary>
internal static class BackupChainFrontier
{
    /// <summary>
    /// Normalises a captured row's origin stamp. An unstamped, locally authored row
    /// arrives as <see cref="string.Empty"/> from the core default origin resolver
    /// and contributes no per-origin provenance (#2621), so it maps to <c>null</c>.
    /// </summary>
    /// <param name="stamp">The row's <c>OriginClusterId</c>.</param>
    /// <returns>The origin id, or <c>null</c> for an unstamped row.</returns>
    public static string? NormalizeOrigin(string? stamp) =>
        string.IsNullOrEmpty(stamp) ? null : stamp;

    /// <summary>
    /// Raises <paramref name="highWater"/>'s entry for <paramref name="originId"/>
    /// to <paramref name="wallClockTicks"/> when that is higher, clamping a negative
    /// tick count to zero.
    /// </summary>
    /// <param name="highWater">The per-origin high-water being accumulated.</param>
    /// <param name="originId">A normalised, non-empty origin id.</param>
    /// <param name="wallClockTicks">The captured row's HLC wall-clock ticks.</param>
    public static void Observe(Dictionary<string, long> highWater, string originId, long wallClockTicks)
    {
        var ticks = wallClockTicks < 0 ? 0 : wallClockTicks;
        if (!highWater.TryGetValue(originId, out var current) || ticks > current)
        {
            highWater[originId] = ticks;
        }
    }

    /// <summary>
    /// The HLC frontier of a full capture: the larger of the registry-snapshot
    /// anchor and the highest HLC the capture read, never negative. The anchor
    /// alone is not a frontier - the core records it as zero (#3758).
    /// </summary>
    /// <param name="registryAnchorTicks">The snapshot coordinate's registry anchor.</param>
    /// <param name="capturedHighestTicks">The highest HLC over the captured rows.</param>
    /// <returns>The full capture's cut HLC.</returns>
    public static long FullCut(long registryAnchorTicks, long capturedHighestTicks)
    {
        var frontier = Math.Max(registryAnchorTicks, capturedHighestTicks);
        return frontier < 0 ? 0 : frontier;
    }

    /// <summary>
    /// The HLC frontier of an increment: the highest HLC its delta read, never
    /// below its base's frontier, so a chain's frontier never regresses (an
    /// increment with no intervening writes carries the base forward), and never
    /// negative.
    /// </summary>
    /// <param name="baseCutTicks">The base link's cut HLC.</param>
    /// <param name="deltaHighestTicks">The highest HLC the delta read.</param>
    /// <returns>The increment's cut HLC.</returns>
    public static long IncrementalCut(long baseCutTicks, long deltaHighestTicks)
    {
        var frontier = deltaHighestTicks < baseCutTicks ? baseCutTicks : deltaHighestTicks;
        return frontier < 0 ? 0 : frontier;
    }
}
