using System.Globalization;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Reads a cluster-tracked backup operation (#4122, #4125) for the Backups area:
/// its title, its phases in the area's words, what it produced as links, figures
/// and identifiers, a one-sentence outcome, and - for a finished point-in-time
/// restore - the result a revert needs. Everything is derived from the shared
/// <see cref="LatticeOperationStatus"/>, so it reads the same whichever circuit,
/// tab or reload opened it.
/// </summary>
internal static class BackupClusterOperation
{
    private const string HealthCheckPrefix = "health.";

    // The cluster's operation-id limit (Orleans.Lattice.Api.Operations).
    private const int MaxOperationIdLength = 128;

    /// <summary>The operation's title, such as "Capture a full backup of orders".</summary>
    /// <param name="status">The status.</param>
    /// <returns>The title.</returns>
    public static string Title(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        var trees = status.Scope.TreeIds;
        var first = trees.Count > 0 ? BackupTreeName.Parse(trees[0]).Name : "a tree";
        return status.Kind switch
        {
            BackupOperationKinds.Capture => "Capture a full backup of " + first,
            BackupOperationKinds.IncrementalCapture => "Capture an incremental backup of " + first,
            BackupOperationKinds.SetCapture => "Capture a backup set of " + trees.Count.ToString(CultureInfo.InvariantCulture) + (trees.Count == 1 ? " tree" : " trees"),
            BackupOperationKinds.Restore => "Restore " + first,
            BackupOperationKinds.ColdRestore => "Cold-restore " + first,
            BackupOperationKinds.HealthCheck => "Check the health of a backup of " + first,
            BackupOperationKinds.CatalogRebuild => "Rebuild the catalogue from the backup store",
            BackupOperationKinds.CatalogScrub => "Check the catalogue against the backup store",
            _ => "Backup operation",
        };
    }

    /// <summary>Links to what a succeeded operation produced; empty otherwise.</summary>
    /// <param name="status">The status.</param>
    /// <returns>The links.</returns>
    public static IReadOnlyList<BackupOperationLink> Links(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (status.State != LatticeOperationState.Succeeded)
        {
            return [];
        }

        switch (status.Kind)
        {
            case BackupOperationKinds.Capture or BackupOperationKinds.IncrementalCapture
                when status.ResultReference is { Length: > 0 } backupId:
                return [new BackupOperationLink("The captured backup", BackupsAddresses.Backup(backupId))];

            case BackupOperationKinds.SetCapture:
                var members = BackupOperationResults.ReadMemberBackupIds(status.Result);
                var links = new List<BackupOperationLink>(members.Count);
                for (var i = 0; i < members.Count; i++)
                {
                    var tree = i < status.Scope.TreeIds.Count ? BackupTreeName.Parse(status.Scope.TreeIds[i]).Name : members[i];
                    links.Add(new BackupOperationLink("The backup of " + tree, BackupsAddresses.Backup(members[i])));
                }

                return links;

            case BackupOperationKinds.Restore or BackupOperationKinds.ColdRestore
                when RestoreResult(status) is { } restore:
                return [new BackupOperationLink("The restored backup", BackupsAddresses.Backup(restore.BackupId))];

            case BackupOperationKinds.HealthCheck when status.ResultReference is { Length: > 0 } checkedId:
                return
                [
                    new BackupOperationLink("The backup's health", BackupsAddresses.HealthOf(checkedId)),
                    new BackupOperationLink("The backup", BackupsAddresses.Backup(checkedId)),
                ];

            default:
                return [];
        }
    }

    /// <summary>The figures a succeeded operation reports; empty otherwise.</summary>
    /// <param name="status">The status.</param>
    /// <returns>The figures.</returns>
    public static IReadOnlyList<KeyValuePair<string, string>> Facts(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (status.State != LatticeOperationState.Succeeded)
        {
            return [];
        }

        if (RestoreResult(status) is { } restore)
        {
            return BackupActions.RestoreFacts(restore);
        }

        switch (status.Kind)
        {
            case BackupOperationKinds.SetCapture:
                return [new("Backups captured", BackupsFormat.Count(BackupOperationResults.ReadMemberBackupIds(status.Result).Count))];

            case BackupOperationKinds.HealthCheck when status.Result.TryGetValue(BackupOperationResultKeys.HealthStatus, out var verdict):
                return
                [
                    new("Verdict", verdict),
                    new("Missing or uncommitted artifacts", status.Result.GetValueOrDefault(BackupOperationResultKeys.MissingArtifactCount, "0")),
                    new("Hash mismatches", status.Result.GetValueOrDefault(BackupOperationResultKeys.HashMismatchArtifactCount, "0")),
                ];

            case BackupOperationKinds.CatalogRebuild when BackupOperationResults.TryReadCatalogRebuildReport(status.Result, out var rebuild):
                return
                [
                    new("Manifests scanned", BackupsFormat.Count(rebuild.ScannedCount)),
                    new("Added to the catalogue", BackupsFormat.Count(rebuild.RegisteredCount)),
                    new("Reconciled in place", BackupsFormat.Count(rebuild.ReconciledCount)),
                ];

            case BackupOperationKinds.CatalogScrub when ScrubReport(status) is { } scrub:
                return
                [
                    new("Rows scanned", BackupsFormat.Count(scrub.ScannedCount)),
                    new("Orphan rows", BackupsFormat.Count(scrub.OrphanCount)),
                    new("Rows removed", BackupsFormat.Count(scrub.RemovedCount)),
                ];

            default:
                return [];
        }
    }

    /// <summary>
    /// The restore result a succeeded restore or cold restore recorded, or
    /// <see langword="null"/> for any other operation. It carries physical tree ids,
    /// which are never shown; it is kept only to revert the restore.
    /// </summary>
    /// <param name="status">The status.</param>
    /// <returns>The result, or <see langword="null"/>.</returns>
    public static LatticeRestoreResult? RestoreResult(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        return status.State == LatticeOperationState.Succeeded
            && status.Kind is BackupOperationKinds.Restore or BackupOperationKinds.ColdRestore
            && BackupOperationResults.TryReadRestoreResult(status.Result, out var restore)
                ? restore
                : null;
    }

    /// <summary>Whether a status is a finished point-in-time restore, which can be reverted.</summary>
    /// <param name="status">The status.</param>
    /// <returns><see langword="true"/> when it can be reverted.</returns>
    public static bool CanRevert(LatticeOperationStatus status) =>
        RestoreResult(status) is { Mode: LatticeRestoreMode.ShadowCutover };

    /// <summary>A phase name in the area's words.</summary>
    /// <param name="phase">The phase name.</param>
    /// <returns>The words.</returns>
    public static string PhaseName(string phase) => phase switch
    {
        BackupOperationPhases.Verifying => "Verifying artifacts",
        BackupOperationPhases.RebuildingCatalog => "Rebuilding the catalogue",
        BackupOperationPhases.ScrubbingCatalog => "Checking the catalogue",
        BackupOperationPhases.PruningOrphans => "Removing orphan rows",
        _ => OperationText.Phase(phase),
    };

    /// <summary>
    /// The identifiers a succeeded operation reported for the operator to see: the
    /// orphan rows a catalogue scrub found. Empty for every other operation.
    /// </summary>
    /// <param name="status">The status.</param>
    /// <returns>The identifiers.</returns>
    public static IReadOnlyList<string> Items(LatticeOperationStatus status) =>
        ScrubReport(status) is { } scrub ? scrub.OrphanBackupIds : [];

    /// <summary>
    /// One sentence for the operation's outcome: what a succeeded health check,
    /// rebuild or scrub found, and otherwise its state in a word.
    /// </summary>
    /// <param name="status">The status.</param>
    /// <returns>The sentence.</returns>
    public static string Summary(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        if (status.State == LatticeOperationState.Succeeded)
        {
            switch (status.Kind)
            {
                case BackupOperationKinds.HealthCheck when status.Result.TryGetValue(BackupOperationResultKeys.HealthStatus, out var verdict):
                    return verdict == nameof(BackupHealthStatus.Healthy)
                        ? "The backup is healthy."
                        : "The backup needs attention: " + verdict + ".";

                case BackupOperationKinds.CatalogRebuild when BackupOperationResults.TryReadCatalogRebuildReport(status.Result, out _):
                    return "Rebuilt the catalogue from the backup store.";

                case BackupOperationKinds.CatalogScrub when ScrubReport(status) is { } scrub:
                    return scrub.OrphanCount == 0
                        ? "Every catalogue row has its backup in the store."
                        : scrub.Pruned
                            ? "Removed " + BackupsFormat.Count(scrub.RemovedCount) + " orphan rows from the catalogue."
                            : "Found " + BackupsFormat.Count(scrub.OrphanCount) + " orphan rows. They are never offered as restore points; remove them from the maintenance page.";
            }
        }

        return OperationText.State(status);
    }

    /// <summary>Whether a succeeded check of the catalogue found orphan rows it did not remove.</summary>
    /// <param name="status">The status.</param>
    /// <returns><see langword="true"/> when there are orphan rows left to remove.</returns>
    public static bool HasOrphansToRemove(LatticeOperationStatus status) =>
        ScrubReport(status) is { Pruned: false, OrphanCount: > 0 };

    /// <summary>
    /// Whether <paramref name="status"/> is a health check of <paramref name="backupId"/>:
    /// one started here under <see cref="HealthCheckId"/>, or one that succeeded
    /// naming the backup as its result, wherever it was started.
    /// </summary>
    /// <param name="status">The status.</param>
    /// <param name="backupId">The backup id.</param>
    /// <returns><see langword="true"/> when it is.</returns>
    public static bool IsHealthCheckOf(LatticeOperationStatus status, string backupId)
    {
        ArgumentNullException.ThrowIfNull(status);
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (status.Kind != BackupOperationKinds.HealthCheck)
        {
            return false;
        }

        if (string.Equals(status.ResultReference, backupId, StringComparison.Ordinal))
        {
            return true;
        }

        // health.{backupId}.{ticks}: the backup id is everything between the prefix
        // and the last dot, so a longer id that merely starts the same never matches.
        var id = status.OperationId;
        var lastDot = id.LastIndexOf('.');
        return id.StartsWith(HealthCheckPrefix, StringComparison.Ordinal)
            && lastDot > HealthCheckPrefix.Length
            && id.AsSpan(HealthCheckPrefix.Length, lastDot - HealthCheckPrefix.Length).SequenceEqual(backupId);
    }

    /// <summary>
    /// The operation id a health check of <paramref name="backupId"/> started here
    /// takes, <c>health.{backupId}.{ticks}</c>, so a page reopened later can find a
    /// check still running. <see langword="null"/> when that id would not be a valid
    /// operation id (too long, or a character the cluster refuses); the cluster then
    /// generates one, and the check is found again only once it has succeeded.
    /// </summary>
    /// <param name="backupId">The backup id.</param>
    /// <param name="startedAt">When it starts.</param>
    /// <returns>The id, or <see langword="null"/>.</returns>
    public static string? HealthCheckId(string backupId, DateTimeOffset startedAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        var id = HealthCheckPrefix + backupId + "." + startedAt.UtcTicks.ToString(CultureInfo.InvariantCulture);
        if (id.Length > MaxOperationIdLength)
        {
            return null;
        }

        foreach (var ch in backupId)
        {
            if (!(char.IsAsciiLetterOrDigit(ch) || ch is '-' or '_' or '.'))
            {
                return null;
            }
        }

        return id;
    }

    private static BackupCatalogScrubReport? ScrubReport(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        return status.State == LatticeOperationState.Succeeded
            && status.Kind == BackupOperationKinds.CatalogScrub
            && BackupOperationResults.TryReadCatalogScrubReport(status.Result, out var report)
                ? report
                : null;
    }
}
