using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeBackupControl"/> over gRPC: a per-circuit adapter
/// over <see cref="LatticeBackupApiGrpcClient"/>, ported from the Backups plugin's
/// <c>GrpcBackupControlClient</c>. Faults map through <see cref="ShellTransportFaults"/>.
/// </summary>
/// <remarks>
/// The catalogue-maintenance and cold-restore verbs
/// (<see cref="GetInventoryAsync"/>, <see cref="RebuildCatalogFromSinkAsync"/>,
/// <see cref="ScrubCatalogAgainstSinkAsync"/> and <see cref="ColdRestoreAsync"/>)
/// are in-cluster operator verbs the backup binding does not serve over the wire,
/// so they fail with <see cref="NotSupportedException"/> - the same shape the
/// shared fault table gives a verb a cluster answers <c>Unimplemented</c> for.
/// </remarks>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellBackupControlTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeBackupApiGrpcClient>(channel, LatticeBackupApiGrpcClient.Create), ILatticeBackupControl
{
    /// <summary>The message the verbs the backup binding does not serve fail with.</summary>
    internal const string NotServedMessage = "The backup control API does not serve this operation over the wire.";

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.CreateBackupAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.CreateIncrementalBackupAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.CreateBackupSetAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task ScheduleBackupAsync(LatticeBackupScheduleRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            request,
            static (client, state, ct) => client.ScheduleBackupAsync(state.Scope, state.Incremental, state.Interval, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return CallAsync(
            (Scope: scope, Incremental: incremental),
            static (client, state, ct) => client.CancelScheduleAsync(state.Scope, state.Incremental, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ListBackupsAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default) =>
        StreamAsync((object?)null, static (client, _, ct) => client.StreamBackupsAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return CallAsync(backupId, static (client, state, ct) => client.DescribeBackupAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return CallAsync(backupId, static (client, state, ct) => client.DeleteBackupAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.RestoreBackupAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        return CallAsync(restore, static (client, state, ct) => client.RevertRestoreAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(
        string backupId,
        string artifactId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentException.ThrowIfNullOrEmpty(artifactId);
        return StreamAsync(
            (BackupId: backupId, ArtifactId: artifactId),
            static (client, state, ct) => client.ExportArtifactAsync(state.BackupId, state.ArtifactId, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupInventoryReport> GetInventoryAsync(CancellationToken cancellationToken = default) =>
        Task.FromException<BackupInventoryReport>(new NotSupportedException(NotServedMessage));

    /// <inheritdoc />
    public Task<BackupCatalogRebuildReport> RebuildCatalogFromSinkAsync(CancellationToken cancellationToken = default) =>
        Task.FromException<BackupCatalogRebuildReport>(new NotSupportedException(NotServedMessage));

    /// <inheritdoc />
    public Task<BackupCatalogScrubReport> ScrubCatalogAgainstSinkAsync(bool pruneOrphans = false, CancellationToken cancellationToken = default) =>
        Task.FromException<BackupCatalogScrubReport>(new NotSupportedException(NotServedMessage));

    /// <inheritdoc />
    public Task<LatticeRestoreResult> ColdRestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return Task.FromException<LatticeRestoreResult>(new NotSupportedException(NotServedMessage));
    }

    /// <inheritdoc />
    public Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return CallAsync(scope, static (client, state, ct) => client.GetScopeStatusAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return CallAsync(scope, static (client, state, ct) => client.ProbeCapabilitiesAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default) =>
        CallAsync((object?)null, static (client, _, ct) => client.IsHealthMonitoringAvailableAsync(ct), null, cancellationToken);

    /// <inheritdoc />
    public Task<BackupHealthReport> CheckBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return CallAsync(backupId, static (client, state, ct) => client.CheckBackupHealthAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        return CallAsync(backupId, static (client, state, ct) => client.GetBackupHealthAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(config);
        return CallAsync(
            (BackupId: backupId, Config: config),
            static (client, state, ct) => client.ConfigureBackupHealthAsync(state.BackupId, state.Config, ct),
            null,
            cancellationToken);
    }
}
