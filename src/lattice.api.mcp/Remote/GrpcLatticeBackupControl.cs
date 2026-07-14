using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the backup control facade
/// (<see cref="ILatticeBackupControl"/>) by delegating to the backup-API gRPC
/// client (<see cref="LatticeBackupApiGrpcClient"/>), so the topology-agnostic
/// backup tool module works unchanged against a cluster reached over gRPC.
/// Streaming members (<see cref="StreamBackupsAsync"/>,
/// <see cref="ExportArtifactAsync"/>) preserve their <see cref="IAsyncEnumerable{T}"/>
/// semantics, and cancellation flows through every call.
/// </summary>
/// <remarks>
/// Four backup facade members have no gRPC binding yet and throw
/// <see cref="NotSupportedException"/>: <see cref="GetInventoryAsync"/>,
/// <see cref="RebuildCatalogFromSinkAsync"/>,
/// <see cref="ScrubCatalogAgainstSinkAsync"/>, and <see cref="ColdRestoreAsync"/>.
/// The remaining members are wire-backed.
/// </remarks>
internal sealed class GrpcLatticeBackupControl : ILatticeBackupControl
{
    private readonly LatticeBackupApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied backup-API gRPC client.</summary>
    public GrpcLatticeBackupControl(LatticeBackupApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)
        => _client.CreateBackupAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)
        => _client.CreateIncrementalBackupAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)
        => _client.CreateBackupSetAsync(request, cancellationToken);

    /// <inheritdoc />
    public async Task ScheduleBackupAsync(LatticeBackupScheduleRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await _client.ScheduleBackupAsync(request.Scope, request.Incremental, request.Interval, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental, CancellationToken cancellationToken = default)
        => _client.CancelScheduleAsync(scope, incremental, cancellationToken);

    /// <inheritdoc />
    public Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)
        => _client.ListBackupsAsync(request, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default)
        => _client.StreamBackupsAsync(cancellationToken);

    /// <inheritdoc />
    public Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default)
        => _client.DescribeBackupAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)
        => _client.DeleteBackupAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
        => _client.RestoreBackupAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)
        => _client.RevertRestoreAsync(restore, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(string backupId, string artifactId, CancellationToken cancellationToken = default)
        => _client.ExportArtifactAsync(backupId, artifactId, cancellationToken);

    /// <inheritdoc />
    public Task<BackupInventoryReport> GetInventoryAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "GetInventoryAsync has no gRPC binding on the backup-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<BackupCatalogRebuildReport> RebuildCatalogFromSinkAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "RebuildCatalogFromSinkAsync has no gRPC binding on the backup-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<BackupCatalogScrubReport> ScrubCatalogAgainstSinkAsync(bool pruneOrphans = false, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "ScrubCatalogAgainstSinkAsync has no gRPC binding on the backup-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<LatticeRestoreResult> ColdRestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "ColdRestoreAsync has no gRPC binding on the backup-API surface; it cannot be served under the remote-host topology.");

    /// <inheritdoc />
    public Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
        => _client.GetScopeStatusAsync(scope, cancellationToken);

    /// <inheritdoc />
    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
        => _client.ProbeCapabilitiesAsync(scope, cancellationToken);

    /// <inheritdoc />
    public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)
        => _client.IsHealthMonitoringAvailableAsync(cancellationToken);

    /// <inheritdoc />
    public Task<BackupHealthReport> CheckBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
        => _client.CheckBackupHealthAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
        => _client.GetBackupHealthAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)
        => _client.ConfigureBackupHealthAsync(backupId, config, cancellationToken);
}
