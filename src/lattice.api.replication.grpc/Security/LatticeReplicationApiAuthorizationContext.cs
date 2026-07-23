using Grpc.Core;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Identifies which replication control-API operation an inbound gRPC call
/// invokes. Supplied to
/// <see cref="ILatticeReplicationApiAuthorizer.IsAuthorizedAsync"/> so a host can
/// make per-operation decisions (for example, allow reading the config but deny
/// enable and disable).
/// </summary>
public enum LatticeReplicationApiOperation
{
    /// <summary>The <c>EnableReplication</c> RPC.</summary>
    EnableReplication,

    /// <summary>The <c>DisableReplication</c> RPC.</summary>
    DisableReplication,

    /// <summary>The <c>GetReplicationConfig</c> RPC.</summary>
    GetReplicationConfig,

    /// <summary>
    /// A replication control-API method the interceptor does not recognise (for
    /// example a future RPC added without updating the operation map). Presented
    /// to the authorizer so a deny-by-default policy can refuse an unmapped call
    /// rather than have it silently masquerade as a benign read operation.
    /// </summary>
    Unknown,
}

/// <summary>
/// Describes an inbound replication control-API gRPC call to
/// <see cref="ILatticeReplicationApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, an optional <see cref="TargetId"/>
/// (the target tree id for a tree-scoped enable / disable call;
/// <see langword="null"/> for the whole-estate config read and the discovery
/// operations), and the underlying gRPC <see cref="ServerCallContext"/> for
/// header / identity / peer inspection.
/// </summary>
public readonly struct LatticeReplicationApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The replication control-API operation being invoked.</param>
    /// <param name="targetId">
    /// The target tree id the call targets, or <see langword="null"/> for
    /// operations that are not scoped to a single tree.
    /// </param>
    public LatticeReplicationApiAuthorizationContext(
        ServerCallContext call,
        LatticeReplicationApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The replication control-API operation being invoked.</summary>
    public LatticeReplicationApiOperation Operation { get; }

    /// <summary>
    /// The target tree id the call targets, or <see langword="null"/> for
    /// operations that are not scoped to a single tree.
    /// </summary>
    public string? TargetId { get; }
}
