using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Identifies which read-only state-API operation an inbound gRPC call invokes.
/// Supplied to <see cref="ILatticeStateApiAuthorizer.IsAuthorizedAsync"/> so a
/// host can make per-operation decisions (for example, allow discovery but deny
/// entry-level reads).
/// </summary>
public enum LatticeStateApiOperation
{
    /// <summary>The <c>ListTrees</c> tree-catalog discovery RPC.</summary>
    ListTrees,

    /// <summary>The <c>ListViews</c> view-catalog discovery RPC.</summary>
    ListViews,

    /// <summary>The <c>GetTreeStructure</c> node-graph RPC.</summary>
    GetTreeStructure,

    /// <summary>The <c>ScanEntries</c> key-range inspection RPC.</summary>
    ScanEntries,

    /// <summary>The <c>GetEntry</c> single-key inspection RPC.</summary>
    GetEntry,
}

/// <summary>
/// Describes an inbound state-API gRPC call to
/// <see cref="ILatticeStateApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, the <see cref="TargetTreeId"/> the
/// call reads (<see langword="null"/> for cluster-wide catalog operations that
/// are not scoped to a single tree), and the underlying gRPC
/// <see cref="ServerCallContext"/> for header / identity / peer inspection.
/// </summary>
public readonly struct LatticeStateApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The state-API operation being invoked.</param>
    /// <param name="targetTreeId">
    /// The tree the call targets, or <see langword="null"/> for cluster-wide
    /// catalog operations (<see cref="LatticeStateApiOperation.ListTrees"/> /
    /// <see cref="LatticeStateApiOperation.ListViews"/>).
    /// </param>
    public LatticeStateApiAuthorizationContext(
        ServerCallContext call,
        LatticeStateApiOperation operation,
        string? targetTreeId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetTreeId = targetTreeId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The state-API operation being invoked.</summary>
    public LatticeStateApiOperation Operation { get; }

    /// <summary>
    /// The tree the call targets, or <see langword="null"/> for cluster-wide
    /// catalog operations that are not scoped to a single tree.
    /// </summary>
    public string? TargetTreeId { get; }
}
