namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Decides what a completed bootstrap installs on the receiver tree frontier
/// (issue #4586 part 2b): the export's per-origin low watermarks and held
/// writes, but only when they describe the contents the receiver imported - the
/// source generation was known and identical at open and close, the tree was
/// not deleted, and the frontier was read under that generation's lineage.
/// Otherwise nothing, so every origin starts again from zero, which is sound.
/// </summary>
internal static class BootstrapFrontierInstall
{
    /// <summary>The frontier to install, or <see langword="null"/> to install none.</summary>
    public static SnapshotSourceFrontier? Decide(
        SnapshotSourceGeneration? openGeneration,
        SnapshotSourceGeneration? closeGeneration,
        SnapshotSourceFrontier? exported)
    {
        if (exported is null
            || openGeneration is not { } open
            || closeGeneration is not { } close
            || open.Lineage is not { } lineage
            || open.PhysicalTreeId is null
            || open.ShardMapVersion is null
            || open.DeleteEpoch is null
            || open.IsDeleted is not false
            || close.IsDeleted is not false
            || exported.Lineage != lineage
            || close.Lineage != lineage
            || !string.Equals(open.PhysicalTreeId, close.PhysicalTreeId, StringComparison.Ordinal)
            || open.ShardMapVersion != close.ShardMapVersion
            || open.DeleteEpoch != close.DeleteEpoch)
        {
            return null;
        }

        return exported;
    }
}