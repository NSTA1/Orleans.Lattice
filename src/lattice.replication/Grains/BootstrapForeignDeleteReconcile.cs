using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The gates for reconciling rows a receiver holds from origins other than the
/// bootstrap source (issue #4549). The source's applied frontier, read when its
/// export opened, says which writes of each origin it had applied by then: a
/// write of origin <c>o</c> stamped below <c>S(o)</c> and not held is
/// reflected in the export, carried or superseded. A live receiver row of such a
/// write whose key the export lacks was deleted at the source, so the receiver
/// deletes it at the row's own timestamp; any later write still wins.
/// </summary>
internal static class BootstrapForeignDeleteReconcile
{
    /// <summary>
    /// Whether <paramref name="frontier"/> may drive the drop floor: it was read
    /// under the lineage the export opened under.
    /// </summary>
    public static bool FrontierMatchesOpen(SnapshotSourceFrontier? frontier, SnapshotSourceGeneration? openGeneration) =>
        frontier?.Lineage is { } lineage
        && openGeneration?.Lineage is { } openLineage
        && lineage == openLineage;

    /// <summary>
    /// Whether the export is whole and stable enough to reconcile foreign rows
    /// against: a last-writer-wins tree, the frontier read under the opening
    /// lineage, and every generation field known, live and unchanged from open
    /// to close.
    /// </summary>
    public static bool IsEligible(
        SnapshotSourceFrontier? frontier,
        SnapshotSourceGeneration? openGeneration,
        SnapshotSourceGeneration? closeGeneration,
        LatticeMergeMode mergeMode)
    {
        if (mergeMode != LatticeMergeMode.LwwRegister || !FrontierMatchesOpen(frontier, openGeneration))
        {
            return false;
        }

        return IsStable(openGeneration, closeGeneration);
    }

    /// <summary>Whether the source tree generation is known, live and unchanged from open to close.</summary>
    public static bool IsStable(SnapshotSourceGeneration? openGeneration, SnapshotSourceGeneration? closeGeneration)
    {
        if (openGeneration is not { } open || closeGeneration is not { } close)
        {
            return false;
        }

        if (open.PhysicalTreeId is null || open.ShardMapVersion is null || open.Lineage is null
            || open.DeleteEpoch is null || open.IsDeleted is null || open.IsDeleted == true
            || close.IsDeleted != false)
        {
            return false;
        }

        return string.Equals(open.PhysicalTreeId, close.PhysicalTreeId, StringComparison.Ordinal)
            && open.ShardMapVersion == close.ShardMapVersion
            && open.Lineage == close.Lineage
            && open.DeleteEpoch == close.DeleteEpoch;
    }

    /// <summary>
    /// Whether the receiver's live row of <paramref name="origin"/> stamped
    /// <paramref name="timestamp"/>, whose key the export lacks, is a delete the
    /// source performed: the source had applied the write before the export
    /// opened.
    /// </summary>
    public static bool ShouldDelete(SnapshotSourceFrontier frontier, string origin, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(frontier);
        if (string.IsNullOrEmpty(origin)
            || !frontier.LowWatermarks.TryGetValue(origin, out var lowWatermark)
            || lowWatermark == HybridLogicalClock.Zero
            || timestamp.CompareTo(lowWatermark) >= 0)
        {
            return false;
        }

        return !frontier.Held.TryGetValue(origin, out var held) || Array.IndexOf(held, timestamp) < 0;
    }
}
