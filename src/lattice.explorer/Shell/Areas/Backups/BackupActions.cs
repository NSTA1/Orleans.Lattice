using System.Globalization;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// Starts the Backups area's staged operations (epic decision E15). Each one
/// first checks access with the capability probe, so a caller the probe denies
/// is told so before anything is attempted, and then makes the one facade call
/// that does the work; the server still authorizes that call fail-closed.
/// </summary>
internal sealed class BackupActions
{
    /// <summary>The first stage of every operation that checks access first.</summary>
    public const string CheckAccessStage = "Check access";

    private readonly ILatticeBackupControl _control;
    private readonly BackupsAccess _access;
    private readonly BackupOperations _operations;

    /// <summary>Creates the actions.</summary>
    /// <param name="control">The backup facade.</param>
    /// <param name="access">The area's probes.</param>
    /// <param name="operations">The circuit's operations.</param>
    public BackupActions(ILatticeBackupControl control, BackupsAccess access, BackupOperations operations)
    {
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(access);
        ArgumentNullException.ThrowIfNull(operations);
        _control = control;
        _access = access;
        _operations = operations;
    }

    /// <summary>Starts capturing a full backup of <paramref name="scope"/>.</summary>
    /// <param name="name">The backup's name.</param>
    /// <param name="scope">What to capture.</param>
    public BackupOperation CaptureFull(string name, BackupScopeSelector scope)
    {
        var request = new LatticeBackupCaptureRequest(name, scope);
        return _operations.Start(
            BackupOperationKind.FullCapture,
            "Capture a full backup of " + BackupTreeName.Parse(scope.TreeId).Name,
            [CheckAccessStage, "Capture", "Record in the catalogue"],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanCapture, cancellationToken).ConfigureAwait(false);
                operation.Advance(1);
                var result = await _control.CreateBackupAsync(request, cancellationToken).ConfigureAwait(false);
                operation.Advance(2);
                ReportCapture(operation, [result]);
                operation.Succeed("Captured backup " + BackupsFormat.Name(result.Manifest) + ".");
            });
    }

    /// <summary>Starts capturing an incremental backup of <paramref name="scope"/> on <paramref name="baseBackupId"/>.</summary>
    /// <param name="name">The backup's name.</param>
    /// <param name="scope">What to capture.</param>
    /// <param name="baseBackupId">The backup it builds on.</param>
    public BackupOperation CaptureIncremental(string name, BackupScopeSelector scope, string baseBackupId)
    {
        var request = new LatticeBackupIncrementalCaptureRequest(name, scope, baseBackupId);
        return _operations.Start(
            BackupOperationKind.IncrementalCapture,
            "Capture an incremental backup of " + BackupTreeName.Parse(scope.TreeId).Name,
            [CheckAccessStage, "Capture the changes since the base", "Record in the catalogue"],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanCaptureIncremental, cancellationToken).ConfigureAwait(false);
                operation.Advance(1);
                var result = await _control.CreateIncrementalBackupAsync(request, cancellationToken).ConfigureAwait(false);
                operation.Advance(2);
                ReportCapture(operation, [result]);
                operation.Succeed("Captured incremental backup " + BackupsFormat.Name(result.Manifest) + ".");
            });
    }

    /// <summary>Starts capturing a backup set: one full backup per scope under one set manifest.</summary>
    /// <param name="name">The set's name.</param>
    /// <param name="scopes">The trees to capture, one scope each.</param>
    /// <param name="crossTreeConsistent">Whether every member is captured at one causal fence.</param>
    public BackupOperation CaptureSet(string name, IReadOnlyList<BackupScopeSelector> scopes, bool crossTreeConsistent)
    {
        var request = new LatticeBackupSetCaptureRequest(name, scopes, crossTreeConsistent);
        return _operations.Start(
            BackupOperationKind.SetCapture,
            "Capture a backup set of " + scopes.Count.ToString(CultureInfo.InvariantCulture) + (scopes.Count == 1 ? " tree" : " trees"),
            [CheckAccessStage, crossTreeConsistent ? "Capture every tree at one fence" : "Capture every tree", "Record in the catalogue"],
            async (operation, cancellationToken) =>
            {
                foreach (var scope in scopes)
                {
                    await RequireAsync(scope, static capabilities => capabilities.CanCapture, cancellationToken).ConfigureAwait(false);
                }

                operation.Advance(1);
                var result = await _control.CreateBackupSetAsync(request, cancellationToken).ConfigureAwait(false);
                operation.Advance(2);
                ReportCapture(operation, result.Members);
                operation.Succeed("Captured backup set " + result.SetManifest.Name + " with "
                    + result.Members.Count.ToString(CultureInfo.InvariantCulture) + (result.Members.Count == 1 ? " member." : " members."));
            });
    }

    /// <summary>Starts restoring <paramref name="backupId"/> into <paramref name="targetTreeId"/>.</summary>
    /// <param name="backupId">The backup to restore.</param>
    /// <param name="targetTreeId">The tree it lands in.</param>
    /// <param name="mode">In place (repair missing items) or shadow cut-over (point-in-time replace).</param>
    /// <param name="cold">Whether to restore from the backup store alone.</param>
    public BackupOperation Restore(string backupId, string targetTreeId, LatticeRestoreMode mode, bool cold)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(targetTreeId);

        var request = new LatticeRestoreRequest(backupId, targetTreeId, mode: mode);
        var target = BackupTreeName.Parse(targetTreeId);
        var scope = BackupScopeSelector.WholeTree(targetTreeId);
        return _operations.Start(
            cold ? BackupOperationKind.ColdRestore : BackupOperationKind.Restore,
            (cold ? "Cold-restore " : "Restore ") + target.Name + (mode == LatticeRestoreMode.ShadowCutover ? " to a point in time" : " by repairing missing items"),
            [CheckAccessStage, cold ? "Read the backup from the store" : "Validate the restore chain", mode == LatticeRestoreMode.ShadowCutover ? "Build and cut over" : "Apply missing items", "Done"],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanRestore, cancellationToken).ConfigureAwait(false);
                operation.Advance(1);
                if (!cold)
                {
                    _ = await _control.DescribeBackupAsync(backupId, cancellationToken).ConfigureAwait(false)
                        ?? throw new KeyNotFoundException("No backup with this id exists.");
                }

                operation.Advance(2);
                LatticeRestoreResult result;
                try
                {
                    result = cold
                        ? await _control.ColdRestoreAsync(request, cancellationToken).ConfigureAwait(false)
                        : await _control.RestoreBackupAsync(request, cancellationToken).ConfigureAwait(false);
                }
                catch (NotSupportedException)
                {
                    _access.MarkExtensionsNotServed();
                    throw;
                }

                operation.KeepRestore(result);
                operation.Report(
                    links: [new BackupOperationLink("The restored backup", BackupsAddresses.Backup(backupId))],
                    facts: RestoreFacts(result));
                operation.Succeed(mode == LatticeRestoreMode.ShadowCutover
                    ? "Restored " + target.Name + " to the backup's point in time. The previous tree is kept, so this can be reverted."
                    : "Repaired " + target.Name + " from the backup.");
            });
    }

    /// <summary>Starts reverting the point-in-time restore <paramref name="restore"/>.</summary>
    /// <param name="restore">The restore operation to revert.</param>
    public BackupOperation Revert(BackupOperation restore)
    {
        ArgumentNullException.ThrowIfNull(restore);
        if (!restore.CanRevert || restore.RestoreResult is not { } result)
        {
            throw new InvalidOperationException("Only a finished point-in-time restore that has not been reverted can be reverted.");
        }

        var target = BackupTreeName.Parse(result.TargetTreeId);
        var scope = BackupScopeSelector.WholeTree(result.TargetTreeId);
        return _operations.Start(
            BackupOperationKind.RevertRestore,
            "Revert the restore of " + target.Name,
            [CheckAccessStage, "Swap back to the previous tree", "Done"],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanRestore, cancellationToken).ConfigureAwait(false);
                operation.Advance(1);
                await _control.RevertRestoreAsync(result, cancellationToken).ConfigureAwait(false);
                restore.MarkReverted(operation.Id);
                operation.Report(links: [new BackupOperationLink("The reverted restore", BackupsAddresses.Operation(restore.Id))]);
                operation.Succeed("Reverted " + target.Name + " to the tree it held before the restore.");
            });
    }

    /// <summary>Starts rebuilding the catalogue from the backup store.</summary>
    public BackupOperation RebuildCatalogue() =>
        _operations.Start(
            BackupOperationKind.RebuildCatalogue,
            "Rebuild the catalogue from the backup store",
            ["Scan the backup store", "Done"],
            async (operation, cancellationToken) =>
            {
                BackupCatalogRebuildReport report;
                try
                {
                    report = await _control.RebuildCatalogFromSinkAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (NotSupportedException)
                {
                    _access.MarkExtensionsNotServed();
                    throw;
                }

                operation.Report(facts:
                [
                    new("Manifests scanned", BackupsFormat.Count(report.ScannedCount)),
                    new("Added to the catalogue", BackupsFormat.Count(report.RegisteredCount)),
                    new("Reconciled in place", BackupsFormat.Count(report.ReconciledCount)),
                ]);
                operation.Succeed("Rebuilt the catalogue from the backup store.");
            });

    /// <summary>Starts checking the catalogue against the backup store.</summary>
    /// <param name="pruneOrphans">Whether to remove orphan rows rather than only report them.</param>
    public BackupOperation ScrubCatalogue(bool pruneOrphans) =>
        _operations.Start(
            BackupOperationKind.ScrubCatalogue,
            pruneOrphans ? "Remove orphan rows from the catalogue" : "Check the catalogue against the backup store",
            [pruneOrphans ? "Find and remove orphan rows" : "Find orphan rows", "Done"],
            async (operation, cancellationToken) =>
            {
                BackupCatalogScrubReport report;
                try
                {
                    report = await _control.ScrubCatalogAgainstSinkAsync(pruneOrphans, cancellationToken).ConfigureAwait(false);
                }
                catch (NotSupportedException)
                {
                    _access.MarkExtensionsNotServed();
                    throw;
                }

                operation.Report(
                    facts:
                    [
                        new("Rows scanned", BackupsFormat.Count(report.ScannedCount)),
                        new("Orphan rows", BackupsFormat.Count(report.OrphanCount)),
                        new("Rows removed", BackupsFormat.Count(report.RemovedCount)),
                    ],
                    items: report.OrphanBackupIds);
                operation.Succeed(report.OrphanCount == 0
                    ? "Every catalogue row has its backup in the store."
                    : report.Pruned
                        ? "Removed " + BackupsFormat.Count(report.RemovedCount) + " orphan rows from the catalogue."
                        : "Found " + BackupsFormat.Count(report.OrphanCount) + " orphan rows. They are never offered as restore points; remove them from the maintenance page.");
            });

    private async Task RequireAsync(BackupScopeSelector scope, Func<BackupScopeCapabilities, bool> allowed, CancellationToken cancellationToken)
    {
        var capabilities = await _access.ProbeAsync(scope, cancellationToken).ConfigureAwait(false);
        if (!allowed(capabilities))
        {
            throw new LatticeAuthorizationDeniedException("The capability probe denied this operation.");
        }
    }

    private static void ReportCapture(BackupOperation operation, IReadOnlyList<LatticeBackupCaptureResult> results) =>
        operation.Report(
            links: [.. results.Select(result => new BackupOperationLink(
                BackupsFormat.Name(result.Manifest) + " (" + BackupTreeName.Parse(result.Manifest.Scope.TreeId).Name + ")",
                BackupsAddresses.Backup(result.BackupId)))],
            facts:
            [
                new("Backups captured", BackupsFormat.Count(results.Count)),
                new("Artifacts", BackupsFormat.Count(results.Sum(result => result.Manifest.ContentDescriptors.Count))),
                new("Size", BackupsFormat.Bytes(results.Sum(result => result.Manifest.ContentDescriptors.Sum(content => content.ByteLength)))),
            ]);

    private static IReadOnlyList<KeyValuePair<string, string>> RestoreFacts(LatticeRestoreResult result)
    {
        var facts = new List<KeyValuePair<string, string>>
        {
            new("Tree", BackupTreeName.Parse(result.TargetTreeId).Name),
            new("Backups replayed", BackupsFormat.Count(result.ManifestChain.Count)),
            new("Entries applied", BackupsFormat.Count(result.EntriesApplied)),
        };

        if (result.DeadLetteredCrossTenant > 0)
        {
            facts.Add(new("Set aside: another tenant's keys", BackupsFormat.Count(result.DeadLetteredCrossTenant)));
        }

        if (result.DeadLetteredOverQuota > 0)
        {
            facts.Add(new("Set aside: over quota", BackupsFormat.Count(result.DeadLetteredOverQuota)));
        }

        return facts;
    }
}
