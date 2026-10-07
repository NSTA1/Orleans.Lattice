using Grpc.Core;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Identifies which backup control-API operation an inbound gRPC call invokes.
/// Supplied to <see cref="ILatticeBackupApiAuthorizer.IsAuthorizedAsync"/> so a
/// host can make per-operation decisions (for example, allow listing and
/// describing but deny capture, delete, and restore).
/// </summary>
public enum LatticeBackupApiOperation
{
    /// <summary>The <c>ListBackups</c> cursor-resumable catalog RPC.</summary>
    ListBackups,

    /// <summary>The server-streaming <c>StreamBackups</c> whole-catalog drain RPC.</summary>
    StreamBackups,

    /// <summary>The <c>DescribeBackup</c> chain-inspection RPC.</summary>
    DescribeBackup,

    /// <summary>The <c>DeleteBackup</c> RPC.</summary>
    DeleteBackup,

    /// <summary>The <c>RevertRestore</c> RPC.</summary>
    RevertRestore,

    /// <summary>The server-streaming <c>ExportArtifact</c> RPC.</summary>
    ExportArtifact,

    /// <summary>The <c>ScheduleBackup</c> recurring-schedule RPC.</summary>
    ScheduleBackup,

    /// <summary>The <c>CancelSchedule</c> recurring-schedule removal RPC.</summary>
    CancelSchedule,

    /// <summary>The <c>GetScopeStatus</c> schedule-status RPC.</summary>
    GetScopeStatus,

    /// <summary>The <c>IsHealthMonitoringAvailable</c> capability RPC.</summary>
    IsHealthMonitoringAvailable,

    /// <summary>The <c>GetBackupHealth</c> stored-report read RPC.</summary>
    GetBackupHealth,

    /// <summary>The <c>ConfigureBackupHealth</c> per-backup monitor-config RPC.</summary>
    ConfigureBackupHealth,

    /// <summary>
    /// A backup control-API method the interceptor does not recognise (for
    /// example a future RPC added without updating the operation map). Presented
    /// to the authorizer so a deny-by-default policy can refuse an unmapped call
    /// rather than have it silently masquerade as a benign catalog operation.
    /// </summary>
    Unknown,

    // The values below were appended after Unknown (#4122) so the shipped
    // numeric values of every earlier member, Unknown included, stay stable.

    /// <summary>The <c>StartBackup</c> accept-then-poll full-capture RPC.</summary>
    StartBackup,

    /// <summary>The <c>StartIncrementalBackup</c> accept-then-poll incremental-capture RPC.</summary>
    StartIncrementalBackup,

    /// <summary>The <c>StartBackupSet</c> accept-then-poll backup-set-capture RPC.</summary>
    StartBackupSet,

    /// <summary>The <c>StartRestore</c> accept-then-poll restore RPC.</summary>
    StartRestore,

    /// <summary>The <c>StartColdRestore</c> accept-then-poll cold-restore RPC.</summary>
    StartColdRestore,

    /// <summary>The <c>GetBackupOperationStatus</c> RPC.</summary>
    GetBackupOperationStatus,

    /// <summary>The <c>ListBackupOperations</c> RPC.</summary>
    ListBackupOperations,

    /// <summary>The <c>CancelBackupOperation</c> RPC.</summary>
    CancelBackupOperation,

    // Appended for #4125, again after every earlier member.

    /// <summary>The <c>StartBackupHealthCheck</c> accept-then-poll health-check RPC.</summary>
    StartBackupHealthCheck,

    /// <summary>The <c>StartCatalogRebuild</c> accept-then-poll catalog-rebuild RPC.</summary>
    StartCatalogRebuild,

    /// <summary>The <c>StartCatalogScrub</c> accept-then-poll catalog-scrub RPC.</summary>
    StartCatalogScrub,
}

/// <summary>
/// Describes an inbound backup control-API gRPC call to
/// <see cref="ILatticeBackupApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, an optional <see cref="TargetId"/>
/// (the backup id for a backup-scoped call, or the target/scope tree id for a
/// capture or restore that is not yet keyed by a backup id;
/// <see langword="null"/> for the whole-catalog and discovery operations), and
/// the underlying gRPC <see cref="ServerCallContext"/> for header / identity /
/// peer inspection.
/// </summary>
public readonly struct LatticeBackupApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The backup control-API operation being invoked.</param>
    /// <param name="targetId">
    /// The backup id or target/scope tree id the call targets, or
    /// <see langword="null"/> for operations that are not scoped to a single
    /// backup or tree.
    /// </param>
    public LatticeBackupApiAuthorizationContext(
        ServerCallContext call,
        LatticeBackupApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The backup control-API operation being invoked.</summary>
    public LatticeBackupApiOperation Operation { get; }

    /// <summary>
    /// The backup id or target/scope tree id the call targets, or
    /// <see langword="null"/> for operations that are not scoped to a single
    /// backup or tree.
    /// </summary>
    public string? TargetId { get; }
}
