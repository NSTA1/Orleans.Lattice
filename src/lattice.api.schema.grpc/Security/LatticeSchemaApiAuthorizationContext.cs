using Grpc.Core;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Identifies which schema control-API operation an inbound gRPC call invokes.
/// Supplied to <see cref="ILatticeSchemaApiAuthorizer.IsAuthorizedAsync"/> so a
/// host can make per-operation decisions (for example, allow reads - policy and
/// version inspection, dead-letter and compliance viewing, capability probing -
/// but deny mutations - set / clear policy, version-config changes, remediation).
/// </summary>
public enum LatticeSchemaApiOperation
{
    /// <summary>The <c>SetPolicy</c> RPC.</summary>
    SetPolicy,

    /// <summary>The <c>ClearPolicy</c> RPC.</summary>
    ClearPolicy,

    /// <summary>The <c>GetPolicy</c> RPC.</summary>
    GetPolicy,

    /// <summary>The server-streaming <c>StreamDeadLetters</c> RPC.</summary>
    StreamDeadLetters,

    /// <summary>The <c>CountDeadLetters</c> RPC.</summary>
    CountDeadLetters,

    /// <summary>The <c>SetVersionConfig</c> RPC.</summary>
    SetVersionConfig,

    /// <summary>The <c>GetVersionConfig</c> RPC.</summary>
    GetVersionConfig,

    /// <summary>The <c>AdvanceTargetVersion</c> RPC.</summary>
    AdvanceTargetVersion,

    /// <summary>The <c>AdvanceAndMigrate</c> RPC.</summary>
    AdvanceAndMigrate,

    /// <summary>The <c>MigrateToTargetVersion</c> RPC.</summary>
    MigrateToTargetVersion,

    /// <summary>The <c>ClearVersionConfig</c> RPC.</summary>
    ClearVersionConfig,

    /// <summary>The <c>Remediate</c> RPC.</summary>
    Remediate,

    /// <summary>The <c>GetRemediationStatus</c> RPC.</summary>
    GetRemediationStatus,

    /// <summary>The read-only <c>ScanCompliance</c> compliance-audit RPC.</summary>
    ScanCompliance,

    /// <summary>The read-only <c>ProbeCapabilities</c> capability-probe RPC.</summary>
    ProbeCapabilities,

    /// <summary>
    /// A schema control-API method the interceptor does not recognise (for
    /// example a future RPC added without updating the operation map). Presented
    /// to the authorizer so a deny-by-default policy can refuse an unmapped call
    /// rather than have it silently masquerade as a benign read.
    /// </summary>
    Unknown,

    // The values below were appended after Unknown (#4126) so the shipped numeric
    // values of every earlier member, Unknown included, stay stable.

    /// <summary>The read-only accept-then-poll <c>StartComplianceScan</c> RPC.</summary>
    StartComplianceScan,

    /// <summary>The read-only <c>GetComplianceScanStatus</c> RPC.</summary>
    GetComplianceScanStatus,

    /// <summary>The read-only <c>ListComplianceScans</c> RPC.</summary>
    ListComplianceScans,

    /// <summary>The <c>CancelComplianceScan</c> RPC.</summary>
    CancelComplianceScan,

    /// <summary>The accept-then-poll <c>StartRemediation</c> RPC.</summary>
    StartRemediation,

    /// <summary>The accept-then-poll <c>StartMigration</c> RPC.</summary>
    StartMigration,

    /// <summary>The accept-then-poll <c>StartAdvanceAndMigrate</c> RPC.</summary>
    StartAdvanceAndMigrate,

    /// <summary>The <c>GetSchemaOperationStatus</c> RPC. Carries no target tree: the facade scopes the read to the caller.</summary>
    GetSchemaOperationStatus,

    /// <summary>The <c>ListSchemaOperations</c> RPC. Carries no target tree: the facade scopes the listing to the caller.</summary>
    ListSchemaOperations,

    /// <summary>The <c>CancelSchemaOperation</c> RPC. Carries no target tree: the facade scopes and authorizes the cancel.</summary>
    CancelSchemaOperation,
}

/// <summary>
/// Describes an inbound schema control-API gRPC call to
/// <see cref="ILatticeSchemaApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, an optional <see cref="TargetId"/>
/// (the governed tree id the call targets; <see langword="null"/> for the
/// unauthenticated discovery operation), and the underlying gRPC
/// <see cref="ServerCallContext"/> for header / identity / peer inspection.
/// </summary>
public readonly struct LatticeSchemaApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The schema control-API operation being invoked.</param>
    /// <param name="targetId">
    /// The governed tree id the call targets, or <see langword="null"/> for
    /// operations that are not scoped to a single tree.
    /// </param>
    public LatticeSchemaApiAuthorizationContext(
        ServerCallContext call,
        LatticeSchemaApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The schema control-API operation being invoked.</summary>
    public LatticeSchemaApiOperation Operation { get; }

    /// <summary>
    /// The governed tree id the call targets, or <see langword="null"/> for
    /// operations that are not scoped to a single tree.
    /// </summary>
    public string? TargetId { get; }
}
