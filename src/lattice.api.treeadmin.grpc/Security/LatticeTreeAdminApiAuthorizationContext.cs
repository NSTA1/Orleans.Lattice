using Grpc.Core;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Identifies which tree-administration control-API operation an inbound gRPC
/// call invokes. Supplied to
/// <see cref="ILatticeTreeAdminApiAuthorizer.IsAuthorizedAsync"/> so a host can
/// make per-operation decisions (for example, allow the read-only capability
/// probe but deny the whole-tree lifecycle operations later releases add).
/// </summary>
public enum LatticeTreeAdminApiOperation
{
    /// <summary>The read-only <c>ProbeCapabilities</c> capability-probe RPC.</summary>
    ProbeCapabilities,

    /// <summary>The read-only <c>GetShardHotness</c> hotness RPC.</summary>
    GetShardHotness,

    /// <summary>The read-only <c>GetDiagnostics</c> diagnostics RPC.</summary>
    GetDiagnostics,

    /// <summary>The read-only <c>InspectShardMap</c> topology RPC.</summary>
    InspectShardMap,

    /// <summary>The read-only <c>GetProjectionDigest</c> digest RPC.</summary>
    GetProjectionDigest,

    /// <summary>The read-only <c>GetTreeStats</c> statistics RPC.</summary>
    GetTreeStats,

    /// <summary>The read-only <c>GetStorageUsage</c> cluster-storage RPC.</summary>
    GetStorageUsage,

    /// <summary>
    /// A tree-administration control-API method the interceptor does not recognise
    /// (for example a future RPC added without updating the operation map).
    /// Presented to the authorizer so a deny-by-default policy can refuse an
    /// unmapped call rather than have it silently masquerade as a benign read.
    /// </summary>
    Unknown,
}

/// <summary>
/// Describes an inbound tree-administration control-API gRPC call to
/// <see cref="ILatticeTreeAdminApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, an optional <see cref="TargetId"/> (the
/// tree id the call targets; <see langword="null"/> for the unauthenticated
/// discovery operation), and the underlying gRPC <see cref="ServerCallContext"/>
/// for header / identity / peer inspection.
/// </summary>
public readonly struct LatticeTreeAdminApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The tree-administration control-API operation being invoked.</param>
    /// <param name="targetId">
    /// The tree id the call targets, or <see langword="null"/> for operations that
    /// are not scoped to a single tree.
    /// </param>
    public LatticeTreeAdminApiAuthorizationContext(
        ServerCallContext call,
        LatticeTreeAdminApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The tree-administration control-API operation being invoked.</summary>
    public LatticeTreeAdminApiOperation Operation { get; }

    /// <summary>
    /// The tree id the call targets, or <see langword="null"/> for operations that
    /// are not scoped to a single tree.
    /// </summary>
    public string? TargetId { get; }
}
