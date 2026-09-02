using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Identifies which data-API operation an inbound gRPC call invokes. Supplied to
/// <see cref="ILatticeDataApiAuthorizer.IsAuthorizedAsync"/> so a host can make
/// per-operation decisions (for example, allow point reads but deny writes).
/// </summary>
public enum LatticeDataApiOperation
{
    /// <summary>The <c>Set</c> point-write RPC.</summary>
    SetPoint,

    /// <summary>The <c>Delete</c> point-delete RPC.</summary>
    DeletePoint,

    /// <summary>The <c>SetManyAtomic</c> single-tree atomic-batch RPC.</summary>
    SetManyAtomic,

    /// <summary>The <c>SetManyAtomicCrossTree</c> cross-tree atomic-batch RPC.</summary>
    SetManyAtomicCrossTree,

    /// <summary>The <c>Get</c> point-read RPC.</summary>
    GetPoint,

    /// <summary>The <c>ReadRange</c> bounded range-read RPC.</summary>
    ReadRange,

    /// <summary>The <c>DeleteRange</c> bounded range-delete RPC.</summary>
    DeleteRange,

    /// <summary>
    /// A data-API method the interceptor does not recognise (for example a
    /// future RPC added without updating the operation map). Presented to the
    /// authorizer so a deny-by-default policy can refuse an unmapped call rather
    /// than have it silently masquerade as a benign operation.
    /// </summary>
    Unknown,

    // The members below are appended after Unknown deliberately: this enum has
    // implicit ordinals, and inserting them in declaration order would renumber
    // Unknown and every member after it, silently changing the meaning of a
    // persisted or logged numeric value.

    /// <summary>The <c>SetMany</c> non-atomic bulk-write RPC.</summary>
    SetMany,

    /// <summary>The <c>CrdtWrite</c> CRDT mutation RPC.</summary>
    CrdtWrite,

    /// <summary>The <c>CrdtRead</c> CRDT read RPC.</summary>
    CrdtRead,
}

/// <summary>
/// Describes an inbound data-API gRPC call to
/// <see cref="ILatticeDataApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, the <see cref="TargetTreeId"/> the
/// call targets (<see langword="null"/> for the cross-tree batch, which spans
/// several trees), and the underlying gRPC <see cref="ServerCallContext"/> for
/// header / identity / peer inspection.
/// </summary>
public readonly struct LatticeDataApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The data-API operation being invoked.</param>
    /// <param name="targetTreeId">
    /// The tree the call targets, or <see langword="null"/> for operations that
    /// are not scoped to a single tree (the cross-tree atomic batch).
    /// </param>
    public LatticeDataApiAuthorizationContext(
        ServerCallContext call,
        LatticeDataApiOperation operation,
        string? targetTreeId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetTreeId = targetTreeId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The data-API operation being invoked.</summary>
    public LatticeDataApiOperation Operation { get; }

    /// <summary>
    /// The tree the call targets, or <see langword="null"/> for operations that
    /// are not scoped to a single tree.
    /// </summary>
    public string? TargetTreeId { get; }
}
