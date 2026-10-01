using System.Globalization;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Reads a cluster-tracked backup or restore operation (#4122) for the Backups
/// area: its title, what it produced as links and figures, and - for a finished
/// point-in-time restore - the result a revert needs. Everything is derived from
/// the shared <see cref="LatticeOperationStatus"/>, so it reads the same whichever
/// circuit, tab or reload opened it.
/// </summary>
internal static class BackupClusterOperation
{
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

        if (status.Kind == BackupOperationKinds.SetCapture)
        {
            return [new("Backups captured", BackupsFormat.Count(BackupOperationResults.ReadMemberBackupIds(status.Result).Count))];
        }

        return [];
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
}
