using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// One long-running tree operation's progress as a Cluster page draws it: the
/// step it is on, the units done out of the units it consists of (or no total
/// when the cluster does not report one), and a line naming those units. Built
/// from each operation's own status, so every page and the tree summary say the
/// same thing about the same operation.
/// </summary>
/// <param name="Label">What is progressing, as the bar's accessible name.</param>
/// <param name="Phase">The step, in words.</param>
/// <param name="Value">The units done.</param>
/// <param name="Maximum">The units in total, or <see langword="null"/> when not known.</param>
/// <param name="Detail">A line naming the units, or <see langword="null"/>.</param>
internal sealed record ClusterOperationProgress(string Label, string Phase, long Value, long? Maximum, string? Detail)
{
    /// <summary>A resize's progress, or <see langword="null"/> when no resize or undo is running.</summary>
    /// <param name="status">The resize status.</param>
    /// <returns>The progress.</returns>
    public static ClusterOperationProgress? Of(TreeResizeStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);

        // UndoRequested is read before InProgress: an accepted undo of a
        // finished resize leaves InProgress false while it still unwinds.
        if (status.UndoRequested)
        {
            return new ClusterOperationProgress("Undo progress", ResizePhaseText(TreeResizePhase.Undo), 0, null, "The resize is being unwound: its copy is discarded and the tree returns to its old size.");
        }

        if (!status.InProgress)
        {
            return null;
        }

        var phase = ResizePhaseText(status.Phase);
        if (status.TotalUnits is not { } total || total <= 0)
        {
            return new ClusterOperationProgress("Resize progress", phase, 0, null, null);
        }

        var shards = total - ResizeStepsAfterCopy;
        var detail = status.Phase is null or TreeResizePhase.Copy
            ? $"{ClusterFormat.Count(Math.Min(status.CompletedUnits, shards))} of {ClusterFormat.Plural(shards, "shard")} copied, then {ResizeStepsAfterCopy} steps to finish."
            : $"Copy complete. Step {ClusterFormat.Count(Math.Clamp(status.CompletedUnits - shards + 1, 1, ResizeStepsAfterCopy))} of {ResizeStepsAfterCopy} to finish.";
        return new ClusterOperationProgress("Resize progress", phase, status.CompletedUnits, total, detail);
    }

    /// <summary>A snapshot's progress, or <see langword="null"/> when no snapshot is running.</summary>
    /// <param name="status">The snapshot status.</param>
    /// <returns>The progress.</returns>
    public static ClusterOperationProgress? Of(TreeSnapshotStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (!status.InProgress)
        {
            return null;
        }

        var phase = status.Phase switch
        {
            TreeSnapshotPhase.LockSource => "Taking the source out of service",
            TreeSnapshotPhase.BeginForwarding => "Starting to forward live writes",
            TreeSnapshotPhase.Copy => "Copying shards",
            TreeSnapshotPhase.UnlockSource => "Returning a copied shard to service",
            _ => "Copying",
        };
        return status.ShardCount is { } total && total > 0
            ? new ClusterOperationProgress("Snapshot progress", phase, status.CopiedShardCount, total, $"{ClusterFormat.Count(status.CopiedShardCount)} of {ClusterFormat.Plural(total, "shard")} copied.")
            : new ClusterOperationProgress("Snapshot progress", phase, 0, null, null);
    }

    /// <summary>A reshard's progress, or <see langword="null"/> when no reshard is running.</summary>
    /// <param name="status">The reshard status.</param>
    /// <returns>The progress.</returns>
    public static ClusterOperationProgress? Of(TreeReshardStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (!status.InProgress)
        {
            return null;
        }

        const string Phase = "Splitting shards";
        var current = status.CurrentPhysicalShardCount;
        if ((status.TargetShardCount ?? status.RequestedShardCount) is not { } target || target <= 0)
        {
            return new ClusterOperationProgress("Reshard progress", Phase, 0, null, $"{ClusterFormat.Plural(current, "physical shard")} so far.");
        }

        var detail = $"{ClusterFormat.Count(current)} of {ClusterFormat.Plural(target, "physical shard")}.";
        if (status.StartPhysicalShardCount is not { } start || start >= target)
        {
            return new ClusterOperationProgress("Reshard progress", Phase, 0, null, detail);
        }

        return new ClusterOperationProgress("Reshard progress", Phase, Math.Clamp(current - start, 0, target - start), target - start, detail);
    }

    /// <summary>A purge's progress, or <see langword="null"/> when no purge is running or has finished.</summary>
    /// <param name="status">The deletion status.</param>
    /// <returns>The progress.</returns>
    public static ClusterOperationProgress? Of(TreeDeletionStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (!status.PurgeInProgress && !status.PurgeComplete)
        {
            return null;
        }

        var phase = status.PurgeComplete ? "Purged" : "Purging shards";
        return status.PurgeShardCount > 0
            ? new ClusterOperationProgress("Purge progress", phase, status.PurgedShardCount, status.PurgeShardCount, $"{ClusterFormat.Count(status.PurgedShardCount)} of {ClusterFormat.Plural(status.PurgeShardCount, "shard")} purged.")
            : new ClusterOperationProgress("Purge progress", phase, status.PurgeComplete ? 1 : 0, status.PurgeComplete ? 1 : null, null);
    }

    /// <summary>The steps a resize takes after its copy, each one unit of <see cref="TreeResizeStatus.TotalUnits"/>.</summary>
    internal const int ResizeStepsAfterCopy = 3;

    /// <summary>A resize step in words.</summary>
    /// <param name="phase">The step, or <see langword="null"/> when the cluster does not report it.</param>
    /// <returns>The words.</returns>
    internal static string ResizePhaseText(TreeResizePhase? phase) => phase switch
    {
        TreeResizePhase.Copy => "Copying the tree at the new size",
        TreeResizePhase.Swap => "Pointing the tree's name at the copy",
        TreeResizePhase.RejectOldShards => "Turning requests away from the old copy",
        TreeResizePhase.RetireOldCopy => "Retiring the old copy",
        TreeResizePhase.Undo => "Undoing the resize",
        _ => "Resizing",
    };
}
