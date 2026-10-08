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
/// The inventory read (<see cref="GetInventoryAsync"/>) has no gRPC binding and
/// throws <see cref="NotSupportedException"/>.
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
    public Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
        => _client.GetScopeStatusAsync(scope, cancellationToken);

    /// <inheritdoc />
    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
        => _client.ProbeCapabilitiesAsync(scope, cancellationToken);

    /// <inheritdoc />
    public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)
        => _client.IsHealthMonitoringAvailableAsync(cancellationToken);

    /// <inheritdoc />
    public Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
        => _client.GetBackupHealthAsync(backupId, cancellationToken);

    /// <inheritdoc />
    public Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)
        => _client.ConfigureBackupHealthAsync(backupId, config, cancellationToken);
}
