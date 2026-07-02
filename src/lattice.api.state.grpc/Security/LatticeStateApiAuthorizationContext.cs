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

    /// <summary>The <c>GetEntryHistory</c> per-key change-history RPC.</summary>
    GetEntryHistory,

    /// <summary>The <c>ListTagIndexes</c> tag-index-catalog discovery RPC.</summary>
    ListTagIndexes,

    /// <summary>
    /// The <c>ListTagValues</c> RPC, which enumerates the entry-derived tag
    /// values of a subject tree's tag index.
    /// </summary>
    ListTagValues,

    /// <summary>
    /// The <c>ListCoveredTrees</c> RPC, which enumerates the subject trees a
    /// tag index covers.
    /// </summary>
    ListCoveredTrees,

    /// <summary>
    /// The <c>ListIndexTags</c> RPC, which enumerates a tag index's distinct
    /// tags across every covered tree.
    /// </summary>
    ListIndexTags,

    /// <summary>
    /// The <c>ScanTagMembers</c> RPC, which enumerates the live members of a
    /// tag across a tag index.
    /// </summary>
    ScanTagMembers,

    /// <summary>The <c>CancelScan</c> snapshot-cursor release RPC.</summary>
    CancelScan,

    /// <summary>
    /// The server-streaming <c>ObserveChanges</c> RPC, which exposes a tree's
    /// live entry-level change feed.
    /// </summary>
    ObserveChanges,

    /// <summary>The server-streaming <c>ObserveMetrics</c> live-metrics RPC.</summary>
    ObserveMetrics,

    /// <summary>The unary one-shot <c>GetMetricsSnapshot</c> RPC.</summary>
    GetMetricsSnapshot,

    /// <summary>The unary cluster-info <c>GetClusterInfo</c> RPC.</summary>
    GetClusterInfo,

    /// <summary>
    /// A state-API method the interceptor does not recognise (for example a
    /// future RPC added without updating the operation map). Presented to the
    /// authorizer so a deny-by-default policy can refuse an unmapped call rather
    /// than have it silently masquerade as a benign catalog operation.
    /// </summary>
    Unknown,
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
    /// The tree the call targets, or <see langword="null"/> for operations that
    /// are not scoped to a single tree (the cluster-wide catalog discovery RPCs,
    /// <see cref="LatticeStateApiOperation.GetClusterInfo"/>, and a multi-tree
    /// or unscoped metrics request).
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
    /// The tree the call targets, or <see langword="null"/> for operations that
    /// are not scoped to a single tree.
    /// </summary>
    public string? TargetTreeId { get; }
}
