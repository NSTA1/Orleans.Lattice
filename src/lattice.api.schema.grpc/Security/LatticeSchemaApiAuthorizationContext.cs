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
