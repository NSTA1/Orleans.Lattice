using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// The restore and delete concern: the confirmation prompts, the restore target
/// and mode, the point-in-time choice on an incremental chain, and the two
/// runners every backup operation folds its result through.
/// </summary>
public partial class BackupsPanel
{
    private readonly Dictionary<string, string> _restoreTargets = new(StringComparer.Ordinal);

    // The chain member the user picked to restore, per chain-tip id. Defaults to
    // the tip (the most recent increment) so restoring a chain row without a
    // choice restores its latest point in time.
    private readonly Dictionary<string, string> _restorePointByTip = new(StringComparer.Ordinal);

    private LatticeRestoreMode _restoreMode = LatticeRestoreMode.InPlace;
    private BackupRow? _pendingDelete;
    private BackupRow? _pendingRestore;

    /// <summary>The row awaiting delete confirmation, or <see langword="null"/>.</summary>
    internal BackupRow? PendingDelete => _pendingDelete;

    /// <summary>The row awaiting restore confirmation, or <see langword="null"/>.</summary>
    internal BackupRow? PendingRestore => _pendingRestore;

    /// <summary>How a restore installs the backup into its target tree.</summary>
    internal LatticeRestoreMode RestoreMode => _restoreMode;

    /// <summary>
    /// A one-line description of the selected restore mode for the confirmation
    /// dialog, so the user sees exactly what the restore will do to the target.
    /// </summary>
    internal string RestoreModeDescription => _restoreMode == LatticeRestoreMode.ShadowCutover
        ? "shadow-cutover (point-in-time: the tree is rebuilt from the backup and post-backup writes are dropped)"
        : "in-place (merge: the backup is merged by last-writer-wins and post-backup writes are kept)";

    /// <summary>
    /// The restore target for a backup: its own scope tree by default, so an
    /// in-place restore back into the original tree is one click, or whatever
    /// the operator retyped. Tracked per backup id so each row keeps its own
    /// edit.
    /// </summary>
    /// <param name="manifest">The backup being restored. Must not be <see langword="null"/>.</param>
    internal string RestoreTargetFor(BackupManifest manifest) =>
        _restoreTargets.TryGetValue(manifest.Id, out var target) ? target : manifest.Scope.TreeId;

    /// <summary>Retargets a restore at a different tree.</summary>
    /// <param name="backupId">The backup whose target is being edited.</param>
    /// <param name="value">The target tree id; <see langword="null"/> clears it.</param>
    internal void SetRestoreTarget(string backupId, string? value) =>
        _restoreTargets[backupId] = value ?? string.Empty;

    /// <summary>Switches how a restore installs the backup.</summary>
    /// <param name="value">The chosen <see cref="LatticeRestoreMode"/> name.</param>
    internal void SetRestoreMode(string? value)
    {
        _restoreMode = Enum.TryParse<LatticeRestoreMode>(value, out var mode)
            ? mode
            : LatticeRestoreMode.InPlace;
        NotifyStateChanged();
    }

    /// <summary>The chain member a chain row would restore: the operator's choice, else the tip.</summary>
    /// <param name="row">The chain tip row.</param>
    internal string SelectedRestorePoint(BackupRow row) =>
        _restorePointByTip.TryGetValue(row.Members[0].Id, out var id) ? id : row.Members[0].Id;

    /// <summary>Picks the point in time a chain row restores to.</summary>
    /// <param name="row">The chain tip row.</param>
    /// <param name="value">The chosen chain member's backup id.</param>
    internal void SetRestorePoint(BackupRow row, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            _restorePointByTip[row.Members[0].Id] = value;
        }
    }

    /// <summary>Opens the restore confirmation for a row.</summary>
    /// <param name="row">The row to restore.</param>
    internal void RequestRestore(BackupRow row)
    {
        _pendingRestore = row;
        NotifyStateChanged();
    }

    /// <summary>Dismisses the restore confirmation without restoring.</summary>
    internal void CancelRestore()
    {
        _pendingRestore = null;
        NotifyStateChanged();
    }

    /// <summary>Runs the confirmed restore.</summary>
    internal async Task ConfirmRestoreAsync()
    {
        var row = _pendingRestore;
        if (row is null)
        {
            return;
        }

        _pendingRestore = null;
        await RestoreRowAsync(row);
        NotifyStateChanged();
    }

    /// <summary>Opens the delete confirmation for a row.</summary>
    /// <param name="row">The row to delete.</param>
    internal void RequestDelete(BackupRow row)
    {
        _pendingDelete = row;
        NotifyStateChanged();
    }

    /// <summary>Dismisses the delete confirmation without deleting.</summary>
    internal void CancelDelete()
    {
        _pendingDelete = null;
        NotifyStateChanged();
    }

    /// <summary>Runs the confirmed delete.</summary>
    internal async Task ConfirmDeleteAsync()
    {
        var row = _pendingDelete;
        if (row is null)
        {
            return;
        }

        _pendingDelete = null;

        if (row.IsIncrementalChain)
        {
            // Deleting an incremental-chain row removes every backup in the chain
            // (the base full backup and all increments), so no orphaned members
            // remain. Fall back to the tip alone if the chain has not loaded.
            var chain = _chainCache.TryGetValue(row.Members[0].Id, out var loaded)
                ? loaded
                : row.Members;
            await RunManyAsync(
                chain.Select(m => (Func<Task<BackupOperationResult>>)(() => Reader.DeleteAsync(m.Id))),
                $"Deleted {chain.Count} backups in the incremental chain.");
            NotifyStateChanged();
            return;
        }

        if (!row.IsSet)
        {
            await RunAsync(() => Reader.DeleteAsync(row.Members[0].Id));
            NotifyStateChanged();
            return;
        }

        // Deleting a set removes every member backup.
        var members = row.Members;
        await RunManyAsync(
            members.Select(m => (Func<Task<BackupOperationResult>>)(() => Reader.DeleteAsync(m.Id))),
            $"Deleted {members.Count} member backups.");
        NotifyStateChanged();
    }

    private async Task RestoreRowAsync(BackupRow row)
    {
        if (row.IsIncrementalChain)
        {
            // Restore the chosen point in time: the selected member id replays its
            // own base-first chain server-side, into the chosen target tree.
            var manifest = row.Members[0];
            var restoreId = SelectedRestorePoint(row);
            await RunAsync(() => Reader.RestoreAsync(restoreId, RestoreTargetFor(manifest), _restoreMode));
            return;
        }

        if (!row.IsSet)
        {
            var manifest = row.Members[0];
            await RunAsync(() => Reader.RestoreAsync(manifest.Id, RestoreTargetFor(manifest), _restoreMode));
            return;
        }

        // A set restores every member back to its own tree in the chosen mode.
        var members = row.Members;
        await RunManyAsync(
            members.Select(m => (Func<Task<BackupOperationResult>>)(
                () => Reader.RestoreAsync(m.Id, m.Scope.TreeId, _restoreMode))),
            $"Restored {members.Count} member backups to their trees.");
    }

    private async Task RunAsync(Func<Task<BackupOperationResult>> action)
    {
        BeginBusy();
        try
        {
            _lastResult = await action();
            await RefreshListAsync();
        }
        finally
        {
            EndBusy();
        }
    }

    // Runs a sequence of operations for a backup set, stopping at the first
    // non-success, then reloads the list once. On full success the aggregate
    // message is reported; otherwise the failing / denied result is surfaced.
    private async Task RunManyAsync(IEnumerable<Func<Task<BackupOperationResult>>> actions, string successMessage)
    {
        BeginBusy();
        try
        {
            BackupOperationResult? last = null;
            foreach (var action in actions)
            {
                last = await action();
                if (last.Status != BackupOperationStatus.Succeeded)
                {
                    break;
                }
            }

            _lastResult = last is { Status: BackupOperationStatus.Succeeded }
                ? BackupOperationResult.Success(successMessage)
                : last;
            await RefreshListAsync();
        }
        finally
        {
            EndBusy();
        }
    }
}
