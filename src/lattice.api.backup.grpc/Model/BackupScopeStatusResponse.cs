using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the scope-status RPC. A missing status is represented by
/// <see cref="Found"/> set to <see langword="false"/>; otherwise the remaining
/// fields mirror <see cref="BackupScopeStatus"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupScopeStatusResponse)]
[Immutable]
public sealed record BackupScopeStatusResponse
{
    /// <summary>Whether a scope status was found.</summary>
    [Id(0)] public bool Found { get; init; }

    /// <summary>The scope this status describes, or <see langword="null"/> when absent.</summary>
    [Id(1)] public BackupScopeSelector? Scope { get; init; }

    /// <summary>Whether a full-backup schedule is registered for the scope.</summary>
    [Id(2)] public bool FullScheduleRegistered { get; init; }

    /// <summary>Whether an incremental-backup schedule is registered for the scope.</summary>
    [Id(3)] public bool IncrementalScheduleRegistered { get; init; }

    /// <summary>The start time of the most recent full-capture cycle, or <c>null</c> when none.</summary>
    [Id(4)] public DateTimeOffset? LastFullRunUtc { get; init; }

    /// <summary>The success time of the most recent full-capture cycle, or <c>null</c> when none.</summary>
    [Id(5)] public DateTimeOffset? LastFullSuccessUtc { get; init; }

    /// <summary>The start time of the most recent incremental-capture cycle, or <c>null</c> when none.</summary>
    [Id(6)] public DateTimeOffset? LastIncrementalRunUtc { get; init; }

    /// <summary>The success time of the most recent incremental-capture cycle, or <c>null</c> when none.</summary>
    [Id(7)] public DateTimeOffset? LastIncrementalSuccessUtc { get; init; }

    /// <summary>The terminal outcome of the most recent capture cycle of either kind.</summary>
    [Id(8)] public BackupScopeRunOutcome LastRunOutcome { get; init; }

    /// <summary>The base-chain length of the scope's latest backup (0 when none exists).</summary>
    [Id(9)] public int ChainDepth { get; init; }

    /// <summary>The runtime-registered full-backup cadence in ticks, or <c>null</c> when none is registered.</summary>
    [Id(10)] public long? RuntimeFullBackupIntervalTicks { get; init; }

    /// <summary>The runtime-registered incremental-backup cadence in ticks, or <c>null</c> when none is registered.</summary>
    [Id(11)] public long? RuntimeIncrementalBackupIntervalTicks { get; init; }
}
