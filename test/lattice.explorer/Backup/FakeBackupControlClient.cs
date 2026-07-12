using Grpc.Core;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// A hand-rolled <see cref="IBackupControlClient"/> fake that lets a test script
/// the outcome of each call: a canned value, a translated
/// <see cref="LatticeAuthorizationDeniedException"/> (a server denial), or a
/// residual <see cref="RpcException"/> (a transport failure).
/// </summary>
internal sealed class FakeBackupControlClient : IBackupControlClient
{
    public BackupScopeCapabilities? CapabilitiesResult { get; set; }
    public BackupCatalogPage? ListResult { get; set; }
    public Exception? ListThrows { get; set; }
    public Exception? CapabilitiesThrows { get; set; }
    public Exception? MutationThrows { get; set; }
    public bool DeleteResult { get; set; } = true;

    public int ListCallCount { get; private set; }
    public BackupCatalogRequest? LastListRequest { get; private set; }
    public BackupScopeSelector? LastProbedScope { get; private set; }
    public LatticeBackupSetCaptureRequest? LastSetRequest { get; private set; }
    public LatticeRestoreRequest? LastRestoreRequest { get; private set; }

    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        LastProbedScope = scope;
        if (CapabilitiesThrows is not null)
        {
            throw CapabilitiesThrows;
        }

        return Task.FromResult(CapabilitiesResult ?? new BackupScopeCapabilities { Scope = scope });
    }

    public Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)
    {
        ListCallCount++;
        LastListRequest = request;
        if (ListThrows is not null)
        {
            throw ListThrows;
        }

        return Task.FromResult(ListResult ?? new BackupCatalogPage());
    }

    public Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default) =>
        Task.FromResult<BackupChainDescription?>(null);

    public Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(new LatticeBackupCaptureResult("full-1", SampleBackup.Manifest("full-1")));
    }

    public Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(new LatticeBackupCaptureResult("inc-1", SampleBackup.Manifest("inc-1", BackupKind.Incremental)));
    }

    public Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)
    {
        LastSetRequest = request;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        var memberIds = request.Scopes.Select((_, i) => $"set-member-{i}").ToArray();
        return Task.FromResult(SampleBackup.SetResult("set-1", memberIds));
    }

    public Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        LastRestoreRequest = request;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(SampleBackup.RestoreResult(request.BackupId, request.TargetTreeId ?? string.Empty, 7));
    }

    public BackupScopeSelector? LastScheduledScope { get; private set; }
    public bool? LastScheduledIncremental { get; private set; }
    public TimeSpan? LastScheduledInterval { get; private set; }
    public TimeSpan ScheduleResult { get; set; } = TimeSpan.FromMinutes(1);

    public Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(DeleteResult);
    }

    public Task<TimeSpan> ScheduleBackupAsync(BackupScopeSelector scope, bool incremental, TimeSpan interval, CancellationToken cancellationToken = default)
    {
        LastScheduledScope = scope;
        LastScheduledIncremental = incremental;
        LastScheduledInterval = interval;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(ScheduleResult);
    }
}
