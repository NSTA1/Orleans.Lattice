using System.Globalization;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Transport;

// Still calls the deprecated blocking backup verbs (LATTICE0002); the Explorer moves to
// ILatticeBackupOperations in the second #4122 change, which removes this suppression.
#pragma warning disable LATTICE0002

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// Starts the Backups area's operations (epic decision E15). Each one first checks
/// access with the capability probe, so a caller the probe denies is told so before
/// anything is attempted; the server still authorizes every call fail-closed.
/// </summary>
/// <remarks>
/// A capture or restore is started on the cluster as a tracked operation (#4122):
/// the circuit's operation checks access, starts it, and hands off to the cluster's
/// operation id, whose status - with real progress - outlives the circuit, so
/// closing the tab never stops the work. Revert and catalogue maintenance run in
/// the circuit end to end.
/// </remarks>
internal sealed class BackupActions
{
    /// <summary>The first stage of every operation that checks access first.</summary>
    public const string CheckAccessStage = "Check access";

    /// <summary>The stage that hands a capture or restore to the cluster.</summary>
    public const string StartStage = "Start on the cluster";

    private readonly ILatticeBackupControl _control;
    private readonly ILatticeBackupOperations _clusterOperations;
    private readonly BackupsAccess _access;
    private readonly BackupOperations _operations;
    private readonly BackupOperationList? _list;

    /// <summary>Creates the actions.</summary>
    /// <param name="control">The backup facade.</param>
    /// <param name="clusterOperations">The backup operations facade the captures and restores start on.</param>
    /// <param name="access">The area's probes.</param>
    /// <param name="operations">The circuit's operations.</param>
    /// <param name="list">The recent-operations list, forgotten whenever an operation starts; optional.</param>
    public BackupActions(
        [FromKeyedServices(ShellFacades.Key)] ILatticeBackupControl control,
        [FromKeyedServices(ShellFacades.Key)] ILatticeBackupOperations clusterOperations,
        BackupsAccess access,
        BackupOperations operations,
        BackupOperationList? list = null)
    {
        ArgumentNullException.ThrowIfNull(control);
        ArgumentNullException.ThrowIfNull(clusterOperations);
        ArgumentNullException.ThrowIfNull(access);
        ArgumentNullException.ThrowIfNull(operations);
        _control = control;
        _clusterOperations = clusterOperations;
        _access = access;
        _operations = operations;
        _list = list;
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
            [CheckAccessStage, StartStage],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanCapture, cancellationToken).ConfigureAwait(false);
                operation.Advance(1);
                var handle = await _clusterOperations.StartBackupAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
                HandOff(operation, handle.OperationId);
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
            [CheckAccessStage, StartStage],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanCaptureIncremental, cancellationToken).ConfigureAwait(false);
                operation.Advance(1);
                var handle = await _clusterOperations.StartIncrementalBackupAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
                HandOff(operation, handle.OperationId);
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
            [CheckAccessStage, StartStage],
            async (operation, cancellationToken) =>
            {
                foreach (var scope in scopes)
                {
                    await RequireAsync(scope, static capabilities => capabilities.CanCapture, cancellationToken).ConfigureAwait(false);
                }

                operation.Advance(1);
                var handle = await _clusterOperations.StartBackupSetAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
                HandOff(operation, handle.OperationId);
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
            [CheckAccessStage, StartStage],
            async (operation, cancellationToken) =>
            {
                await RequireAsync(scope, static capabilities => capabilities.CanRestore, cancellationToken).ConfigureAwait(false);
                if (!cold)
                {
                    _ = await _control.DescribeBackupAsync(backupId, cancellationToken).ConfigureAwait(false)
                        ?? throw new KeyNotFoundException("No backup with this id exists.");
                }

                operation.Advance(1);
                Api.Operations.LatticeOperationHandle handle;
                try
                {
                    handle = cold
                        ? await _clusterOperations.StartColdRestoreAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false)
                        : await _clusterOperations.StartRestoreAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (NotSupportedException)
                {
                    _access.MarkExtensionsNotServed();
                    throw;
                }

                HandOff(operation, handle.OperationId);
            });
    }

    /// <summary>
    /// Starts reverting the point-in-time restore that cluster operation
    /// <paramref name="restoreOperationId"/> made, from its recorded result.
    /// </summary>
    /// <param name="restoreOperationId">The restore's cluster operation id.</param>
    /// <param name="result">The restore's result, as its operation recorded it.</param>
    public BackupOperation Revert(string restoreOperationId, LatticeRestoreResult result)
    {
        ArgumentException.ThrowIfNullOrEmpty(restoreOperationId);
        ArgumentNullException.ThrowIfNull(result);
        if (result.Mode != LatticeRestoreMode.ShadowCutover)
        {
            throw new InvalidOperationException("Only a point-in-time restore can be reverted.");
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
                operation.Report(links: [new BackupOperationLink("The reverted restore", BackupsAddresses.Operation(restoreOperationId))]);
                operation.Succeed("Reverted " + target.Name + " to the tree it held before the restore.");
            },
            reverts: restoreOperationId);
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

    private void HandOff(BackupOperation operation, string clusterOperationId)
    {
        _list?.Forget();
        operation.HandOff(clusterOperationId);
        operation.Succeed("Started on the cluster. It keeps running if you close this page.");
    }

    /// <summary>The figures a finished restore reports.</summary>
    /// <param name="result">The restore's result.</param>
    /// <returns>The figures.</returns>
    internal static IReadOnlyList<KeyValuePair<string, string>> RestoreFacts(LatticeRestoreResult result)
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
