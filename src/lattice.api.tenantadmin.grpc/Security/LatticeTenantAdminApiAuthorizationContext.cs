using Grpc.Core;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Identifies which tenant-administration control-API operation an inbound gRPC
/// call invokes. Supplied to
/// <see cref="ILatticeTenantAdminApiAuthorizer.IsAuthorizedAsync"/> so a host can
/// make per-operation decisions (for example allow suspend/resume but deny the
/// destructive delete).
/// </summary>
public enum LatticeTenantAdminApiOperation
{
    /// <summary>The mutating <c>CreateTenant</c> lifecycle RPC.</summary>
    CreateTenant = 0,

    /// <summary>The mutating <c>SuspendTenant</c> lifecycle RPC.</summary>
    SuspendTenant = 1,

    /// <summary>The mutating <c>ResumeTenant</c> lifecycle RPC.</summary>
    ResumeTenant = 2,

    /// <summary>The mutating, destructive <c>DeleteTenant</c> lifecycle RPC (cascades the tenant's trees).</summary>
    DeleteTenant = 3,

    /// <summary>
    /// A tenant-administration control-API method the interceptor does not
    /// recognise (for example a future RPC added without updating the operation
    /// map). Presented to the authorizer so a deny-by-default policy can refuse an
    /// unmapped call rather than have it silently masquerade as a benign call.
    /// </summary>
    Unknown = 4,
}

/// <summary>
/// Describes an inbound tenant-administration control-API gRPC call to
/// <see cref="ILatticeTenantAdminApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, an optional <see cref="TargetId"/> (the
/// tenant id the call targets), and the underlying gRPC
/// <see cref="ServerCallContext"/> for header / identity / peer inspection.
/// </summary>
public readonly struct LatticeTenantAdminApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The tenant-administration control-API operation being invoked.</param>
    /// <param name="targetId">
    /// The tenant id the call targets, or <see langword="null"/> for operations
    /// that are not scoped to a single tenant.
    /// </param>
    public LatticeTenantAdminApiAuthorizationContext(
        ServerCallContext call,
        LatticeTenantAdminApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The tenant-administration control-API operation being invoked.</summary>
    public LatticeTenantAdminApiOperation Operation { get; }

    /// <summary>
    /// The tenant id the call targets, or <see langword="null"/> for operations
    /// that are not scoped to a single tenant.
    /// </summary>
    public string? TargetId { get; }
}
