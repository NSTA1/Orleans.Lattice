using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupColdRestoreService"/>. Orchestrates a
/// catalog-free disaster restore in front of the existing restore engine: it
/// bootstraps the reserved <c>sys-</c> trees, resolves the target backup from the
/// sink (proving the resolution never depends on the catalog), delegates the
/// causal-preserving replay to <see cref="ILatticeBackupRestoreService"/>, then
/// re-projects the catalog from the sink via
/// <see cref="ILatticeBackupCatalogRebuildService"/> so the recovered cluster is
/// left with a correct catalog. The restore engine's own chain walk falls back to
/// the sink when the catalog is empty, so a cold cluster walks the
/// <see cref="BackupManifest.BaseBackupId"/> chain and validates every artifact
/// straight from the sink.
/// </summary>
internal sealed class LatticeBackupColdRestoreService(
    ILatticeBackupSink sink,
    ILatticeBackupRestoreService restore,
    ILatticeBackupCatalogRebuildService catalogRebuild,
    BackupInitializer initializer,
    ILogger<LatticeBackupColdRestoreService> logger) : ILatticeBackupColdRestoreService
{
    private readonly ILatticeBackupSink _sink = sink ?? throw new ArgumentNullException(nameof(sink));
    private readonly ILatticeBackupRestoreService _restore = restore ?? throw new ArgumentNullException(nameof(restore));
    private readonly ILatticeBackupCatalogRebuildService _catalogRebuild =
        catalogRebuild ?? throw new ArgumentNullException(nameof(catalogRebuild));
    private readonly BackupInitializer _initializer = initializer ?? throw new ArgumentNullException(nameof(initializer));
    private readonly ILogger<LatticeBackupColdRestoreService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task<LatticeRestoreResult> ColdRestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Bootstrap the reserved sys- trees so a fresh cluster whose catalog tree
        // has never been touched has its history retention and catalog index in
        // place before anything is registered. Idempotent and safe to re-run.
        await _initializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Resolve the target manifest from the SINK alone - never the catalog. This
        // is the defining property of a cold restore: a cluster that lost its
        // catalog can still find the backup because the manifest is self-describing
        // and lives in the durable sink. A clear, cold-specific error distinguishes
        // "the sink does not have this backup" from an ordinary catalog miss.
        var tip = await _sink.ReadManifestAsync(request.BackupId, cancellationToken).ConfigureAwait(false)
            ?? throw new LatticeRestoreValidationException(
                $"No backup with id '{request.BackupId}' exists in the sink. A cold restore resolves "
                + "backups from the sink alone, so the backup medium must hold the manifest.");

        _logger.LogInformation(
            "Cold-restoring backup {BackupId} (kind {Kind}) from the sink into tree {TreeId}.",
            tip.Id, tip.Kind, request.TargetTreeId ?? tip.Scope.TreeId);

        // Delegate the causal-preserving replay to the existing restore engine. Its
        // chain walk and artifact validation resolve manifests catalog-first then
        // fall back to the sink; on a cold cluster the catalog is empty, so both the
        // BaseBackupId chain walk and the artifact integrity checks run straight
        // from the sink and surface a clear error on a broken chain or a
        // missing / tampered artifact before anything is installed.
        var result = await _restore.RestoreAsync(request, cancellationToken).ConfigureAwait(false);

        // Leave the recovered cluster with a correct catalog: re-project every
        // manifest the sink holds into the reserved catalog tree. Idempotent, and
        // the catalog is a disposable projection over the sink, so this never
        // affects the restored data - it only heals discovery.
        var rebuild = await _catalogRebuild.RebuildFromSinkAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Cold restore of backup {BackupId} applied {EntryCount} entries; catalog re-projected "
            + "from the sink ({Scanned} scanned, {Registered} added, {Reconciled} reconciled).",
            result.BackupId, result.EntriesApplied, rebuild.ScannedCount, rebuild.RegisteredCount, rebuild.ReconciledCount);

        return result;
    }
}
