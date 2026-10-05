namespace Orleans.Lattice.Replication.Grains;

internal enum BootstrapReconcileOutcome
{
    Reconciled,
    SkippedScoped,
    SkippedUnstable,
    SkippedDeleted,
    SkippedUnknown,
    SkippedLineageMismatch,
    SkippedNeverAligned,
    SkippedNotLww,
    Aligned,
}

internal readonly record struct BootstrapReconcileDecision(
    BootstrapReconcileOutcome Outcome,
    bool ShouldReconcile,
    bool OweRetry,
    bool RecordAlignedLineage);

internal static class BootstrapDeleteReconcile
{
    public static BootstrapReconcileDecision Decide(
        bool isScopedExport,
        SnapshotSourceGeneration? openGeneration,
        SnapshotSourceGeneration? closeGeneration,
        Guid? alignedLineage,
        bool heldNoSourceRowsAtImportStart,
        bool anyCapturedKeyAbsentFromExport,
        bool anySourceRowAbsentFromExportAtEnd,
        LatticeMergeMode mergeMode)
    {
        if (isScopedExport)
        {
            return new(BootstrapReconcileOutcome.SkippedScoped, false, false, false);
        }

        if (mergeMode != LatticeMergeMode.LwwRegister)
        {
            return new(BootstrapReconcileOutcome.SkippedNotLww, false, false, false);
        }

        // Unknown is owed, not permanent: a sender that predates the generation
        // reconciles on the first retry after it upgrades.
        if (openGeneration is not { } open || closeGeneration is not { } close)
        {
            return new(BootstrapReconcileOutcome.SkippedUnknown, false, true, false);
        }

        if (HasUnknown(open) || HasUnknown(close))
        {
            return new(BootstrapReconcileOutcome.SkippedUnknown, false, true, false);
        }

        if (open.IsDeleted == true || close.IsDeleted == true)
        {
            return new(BootstrapReconcileOutcome.SkippedDeleted, false, true, false);
        }

        if (!StringEquals(open.PhysicalTreeId, close.PhysicalTreeId)
            || open.ShardMapVersion != close.ShardMapVersion
            || open.Lineage != close.Lineage
            || open.DeleteEpoch != close.DeleteEpoch)
        {
            return new(BootstrapReconcileOutcome.SkippedUnstable, false, true, false);
        }

        // Issue #4549: a source-origin row the receiver took during the drain -
        // from an older lineage, after a source restore - is not in the
        // pre-capture. Aligning over it would let a later pass vouch for it and
        // delete a value the source never deleted, so every alignment also needs
        // the end-of-drain scan to find no source-origin row the export lacks.
        var anyOrphaned = anyCapturedKeyAbsentFromExport || anySourceRowAbsentFromExportAtEnd;
        if (alignedLineage is null)
        {
            // The reconcile only touches source-origin keys, so a receiver that
            // held none of them when the import began is aligned by this import,
            // whatever local or third-origin rows it holds - unless one arrived
            // during the drain that the export does not carry.
            if (heldNoSourceRowsAtImportStart && !anySourceRowAbsentFromExportAtEnd)
            {
                return new(BootstrapReconcileOutcome.Reconciled, true, false, true);
            }

            return AlignWhenNothingIsOrphaned(BootstrapReconcileOutcome.SkippedNeverAligned, anyOrphaned);
        }

        if (alignedLineage != open.Lineage)
        {
            return AlignWhenNothingIsOrphaned(BootstrapReconcileOutcome.SkippedLineageMismatch, anyOrphaned);
        }

        return new(BootstrapReconcileOutcome.Reconciled, true, false, false);
    }

    /// <summary>
    /// A receiver that cannot prove its copy derives from the export's lineage
    /// adopts it only when every source-origin key it held was carried by this
    /// whole-tree export: there is then nothing it could wrongly delete, and every
    /// source-origin key it holds is one the lineage carries. Otherwise it skips.
    /// </summary>
    private static BootstrapReconcileDecision AlignWhenNothingIsOrphaned(
        BootstrapReconcileOutcome skipped,
        bool anyCapturedKeyAbsentFromExport) =>
        anyCapturedKeyAbsentFromExport
            ? new(skipped, false, false, false)
            : new(BootstrapReconcileOutcome.Aligned, false, false, true);

    private static bool HasUnknown(SnapshotSourceGeneration generation) =>
        generation.PhysicalTreeId is null
        || generation.ShardMapVersion is null
        || generation.Lineage is null
        || generation.DeleteEpoch is null
        || generation.IsDeleted is null;

    private static bool StringEquals(string? left, string? right) =>
        string.Equals(left, right, StringComparison.Ordinal);
}
