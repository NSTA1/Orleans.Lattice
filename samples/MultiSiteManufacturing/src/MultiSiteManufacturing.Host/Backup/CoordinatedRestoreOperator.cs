using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Lattice;
using Orleans.Lattice.Backup;

namespace MultiSiteManufacturing.Host.Backup;

/// <summary>
/// Operator-facing facade that captures and restores the sample's replicated
/// fact tree (<see cref="LatticeFactBackend.FactTreeId"/>) through the shared
/// external backup sink (the Azure blob sink under docker-compose; a local
/// <see cref="FileSystemBackupSink"/> in the single-machine quick-start). It mirrors the seam
/// <c>OperatorActions</c> uses: a small DI-registered facade the UI, a gRPC
/// service, or a test can drive directly, with no new web framework bolted on.
/// <para>
/// The interesting method is <see cref="RestoreFactTreeAsync"/>. Because the
/// fact tree is declared in the sample's replicated-tree set, the backup
/// package's restore entry point promotes the restore into an all-or-nothing
/// coordinated multi-cluster saga automatically: dispatch is decided by the
/// target tree's current replication membership, not by this facade. When the
/// tree is not replicated (the single-cluster quick-start) the same call runs as
/// a plain local restore. The facade therefore stays transport-agnostic and free
/// of any saga wiring.
/// </para>
/// </summary>
public sealed class CoordinatedRestoreOperator(
    ILatticeBackupCaptureService capture,
    ILatticeBackupRestoreService restore,
    ILogger<CoordinatedRestoreOperator> logger)
{
    /// <summary>
    /// Captures a full, point-in-time backup of the whole fact tree to the shared
    /// sink and returns the content-addressed backup id. The manifest lands in the
    /// shared sink so any peer cluster can resolve and restore it.
    /// </summary>
    /// <param name="name">The human-readable backup name recorded on the manifest. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the capture.</param>
    /// <returns>The content-addressed id of the captured backup.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    public async Task<string> CaptureFactTreeAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        var result = await capture.CaptureAsync(
            new LatticeBackupCaptureRequest(name, BackupScopeSelector.WholeTree(LatticeFactBackend.FactTreeId)),
            cancellationToken);

        logger.LogInformation(
            "Captured backup '{BackupId}' of fact tree '{TreeId}' to the shared sink.",
            result.BackupId, LatticeFactBackend.FactTreeId);

        return result.BackupId;
    }

    /// <summary>
    /// Restores the backup identified by <paramref name="backupId"/> back into the
    /// fact tree via an atomic shadow-cutover. When the fact tree is currently
    /// replicated this restore runs as a coordinated multi-cluster saga so every
    /// participating cluster flips together (all-or-nothing) and no peer re-advances
    /// the restored cut; when it is not replicated it runs as a plain local restore.
    /// </summary>
    /// <param name="backupId">The content-addressed backup id to restore. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the restore.</param>
    /// <returns>The restore outcome.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    public async Task<LatticeRestoreResult> RestoreFactTreeAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var result = await restore.RestoreAsync(
            new LatticeRestoreRequest(
                backupId,
                targetTreeId: LatticeFactBackend.FactTreeId,
                mode: LatticeRestoreMode.ShadowCutover),
            cancellationToken);

        logger.LogInformation(
            "Restored backup '{BackupId}' into fact tree '{TreeId}' ({Entries} entries).",
            result.BackupId, result.TargetTreeId, result.EntriesApplied);

        return result;
    }
}
