using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// In-memory test double for the backup restore engine seams the restore
/// participant drives. Records every phase call (probe / build / commit / delete /
/// revert) and lets a test force a build failure to exercise the capacity-
/// exhaustion abort path. Implements both the fine-grained
/// <see cref="ILatticeCoordinatedRestoreEngine"/> the participant uses and the
/// coarse <see cref="ILatticeBackupRestoreService"/> the participant calls to
/// revert on abort, mirroring the single production class that implements both.
/// </summary>
internal sealed class FakeCoordinatedRestoreEngine
    : ILatticeCoordinatedRestoreEngine, ILatticeBackupRestoreService
{
    /// <summary>The tree the fake reports as the restore target.</summary>
    public string TargetTree { get; set; } = "orders";

    /// <summary>The self-describing size the admission probe reports.</summary>
    public long ProbeByteLength { get; set; } = 1024;

    /// <summary>The shard count the admission probe reports.</summary>
    public int ProbeShardCount { get; set; } = 1;

    /// <summary>When set, <see cref="BuildShadowAsync"/> throws this on every call.</summary>
    public Exception? BuildFailure { get; set; }

    /// <summary>When set, <see cref="BuildShadowAsync"/> throws for its first N calls then succeeds.</summary>
    public int TransientBuildFailures { get; set; }

    /// <summary>When set, <see cref="ProbeAdmissionAsync"/> throws this.</summary>
    public Exception? ProbeFailure { get; set; }

    /// <summary>Number of admission probes.</summary>
    public int ProbeCount { get; private set; }

    /// <summary>Number of shadow build attempts.</summary>
    public int BuildCount { get; private set; }

    /// <summary>Number of shadow commits (atomic alias swaps).</summary>
    public int CommitCount { get; private set; }

    /// <summary>Number of shadow deletes (garbage collection).</summary>
    public int DeleteCount { get; private set; }

    /// <summary>Number of reverts.</summary>
    public int RevertCount { get; private set; }

    /// <summary>Every shadow physical tree id passed to <see cref="DeleteShadowAsync"/>.</summary>
    public List<string> DeletedShadows { get; } = [];

    private string ShadowId => $"{TargetTree}-bkprestore-shadow";

    /// <inheritdoc />
    public Task<RestoreAdmissionReport> ProbeAdmissionAsync(
        LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        ProbeCount++;
        if (ProbeFailure is not null)
        {
            throw ProbeFailure;
        }

        return Task.FromResult(new RestoreAdmissionReport(
            backupId: request.BackupId,
            targetTreeId: request.TargetTreeId ?? TargetTree,
            totalByteLength: ProbeByteLength,
            totalChunkCount: 1,
            shardCount: ProbeShardCount,
            manifestChain: [request.BackupId]));
    }

    /// <inheritdoc />
    public Task<LatticeRestoreResult> BuildShadowAsync(
        LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        BuildCount++;
        if (BuildFailure is not null)
        {
            throw BuildFailure;
        }

        if (TransientBuildFailures > 0)
        {
            TransientBuildFailures--;
            throw new InvalidOperationException("Transient capacity exhaustion.");
        }

        return Task.FromResult(new LatticeRestoreResult(
            backupId: request.BackupId,
            targetTreeId: request.TargetTreeId ?? TargetTree,
            mode: LatticeRestoreMode.ShadowCutover,
            operationId: "op-" + request.BackupId,
            manifestChain: [request.BackupId],
            entriesApplied: 0,
            shadowPhysicalTreeId: ShadowId,
            previousPhysicalTreeId: request.TargetTreeId ?? TargetTree));
    }

    /// <inheritdoc />
    public Task CommitShadowAsync(LatticeRestoreResult shadow, CancellationToken cancellationToken = default)
    {
        CommitCount++;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteShadowAsync(string shadowPhysicalTreeId, CancellationToken cancellationToken = default)
    {
        DeleteCount++;
        DeletedShadows.Add(shadowPhysicalTreeId);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public string ResolveShadowTreeId(LatticeRestoreRequest request) => ShadowId;

    /// <inheritdoc />
    public Task<LatticeRestoreResult> RestoreAsync(
        LatticeRestoreRequest request, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("The fake engine does not drive the coarse restore entry point.");

    /// <inheritdoc />
    public Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)
    {
        RevertCount++;
        return Task.CompletedTask;
    }
}
