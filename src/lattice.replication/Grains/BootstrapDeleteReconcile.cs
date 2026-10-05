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
        bool receiverWasEmpty,
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

        if (openGeneration is not { } open || closeGeneration is not { } close)
        {
            return new(BootstrapReconcileOutcome.SkippedUnknown, false, false, false);
        }

        if (HasUnknown(open) || HasUnknown(close))
        {
            return new(BootstrapReconcileOutcome.SkippedUnknown, false, false, false);
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

        if (alignedLineage is null)
        {
            if (!receiverWasEmpty)
            {
                return new(BootstrapReconcileOutcome.SkippedNeverAligned, false, false, false);
            }

            return new(BootstrapReconcileOutcome.Reconciled, true, false, true);
        }

        if (alignedLineage != open.Lineage)
        {
            return new(BootstrapReconcileOutcome.SkippedLineageMismatch, false, false, false);
        }

        return new(BootstrapReconcileOutcome.Reconciled, true, false, false);
    }

    private static bool HasUnknown(SnapshotSourceGeneration generation) =>
        generation.PhysicalTreeId is null
        || generation.ShardMapVersion is null
        || generation.Lineage is null
        || generation.DeleteEpoch is null
        || generation.IsDeleted is null;

    private static bool StringEquals(string? left, string? right) =>
        string.Equals(left, right, StringComparison.Ordinal);
}
