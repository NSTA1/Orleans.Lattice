using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Identifies which auth-API admin operation an inbound gRPC call invokes.
/// Supplied to <see cref="ILatticeAuthApiAuthorizer.IsAuthorizedAsync"/> so a
/// host can make per-operation decisions (for example, allow policy reads but
/// deny policy writes, or restrict membership administration to a subset of
/// callers).
/// </summary>
public enum LatticeAuthApiOperation
{
    /// <summary>The <c>UpsertGroup</c> RPC.</summary>
    UpsertGroup,

    /// <summary>The <c>GetGroup</c> RPC.</summary>
    GetGroup,

    /// <summary>The <c>RemoveGroup</c> RPC.</summary>
    RemoveGroup,

    /// <summary>The <c>ListGroups</c> RPC.</summary>
    ListGroups,

    /// <summary>The <c>AddMember</c> RPC.</summary>
    AddMember,

    /// <summary>The <c>RemoveMember</c> RPC.</summary>
    RemoveMember,

    /// <summary>The <c>ListGroupMembers</c> RPC.</summary>
    ListGroupMembers,

    /// <summary>The <c>ListSubjectGroups</c> RPC.</summary>
    ListSubjectGroups,

    /// <summary>The <c>PutRule</c> RPC.</summary>
    PutRule,

    /// <summary>The <c>GetRule</c> RPC.</summary>
    GetRule,

    /// <summary>The <c>RemoveRule</c> RPC.</summary>
    RemoveRule,

    /// <summary>The <c>ListRules</c> RPC.</summary>
    ListRules,

    /// <summary>The <c>ListRulesForTree</c> RPC.</summary>
    ListRulesForTree,

    /// <summary>The <c>Explain</c> introspection RPC.</summary>
    Explain,

    /// <summary>The <c>EffectivePermissions</c> introspection RPC.</summary>
    EffectivePermissions,

    /// <summary>The <c>SearchDirectory</c> RPC.</summary>
    SearchDirectory,

    /// <summary>The <c>ResolveDirectoryPrincipal</c> RPC.</summary>
    ResolveDirectoryPrincipal,

    /// <summary>The <c>GetAccessModel</c> RPC.</summary>
    GetAccessModel,

    /// <summary>
    /// An auth-API method the interceptor does not recognise (for example a
    /// future RPC added without updating the operation map). Presented to the
    /// authorizer so a deny-by-default policy can refuse an unmapped call rather
    /// than have it silently masquerade as a benign operation.
    /// </summary>
    Unknown,
}

/// <summary>
/// Describes an inbound auth-API gRPC call to
/// <see cref="ILatticeAuthApiAuthorizer.IsAuthorizedAsync"/>. Carries the
/// <see cref="Operation"/> being invoked, the primary <see cref="TargetId"/> the
/// call administers (a user / group / member / tree / subject id, or
/// <see langword="null"/> for the catalog-wide list operations), and the
/// underlying gRPC <see cref="ServerCallContext"/> for header / identity / peer
/// inspection.
/// </summary>
public readonly struct LatticeAuthApiAuthorizationContext
{
    /// <summary>Initialises the authorization context.</summary>
    /// <param name="call">The underlying gRPC server call context.</param>
    /// <param name="operation">The auth-API operation being invoked.</param>
    /// <param name="targetId">
    /// The primary id the call administers, or <see langword="null"/> for
    /// operations not scoped to a single id (the catalog-wide list operations).
    /// </param>
    public LatticeAuthApiAuthorizationContext(
        ServerCallContext call,
        LatticeAuthApiOperation operation,
        string? targetId)
    {
        ArgumentNullException.ThrowIfNull(call);
        Call = call;
        Operation = operation;
        TargetId = targetId;
    }

    /// <summary>The underlying gRPC server call context (headers, deadline, peer).</summary>
    public ServerCallContext Call { get; }

    /// <summary>The auth-API operation being invoked.</summary>
    public LatticeAuthApiOperation Operation { get; }

    /// <summary>
    /// The primary id the call administers, or <see langword="null"/> for
    /// operations not scoped to a single id.
    /// </summary>
    public string? TargetId { get; }
}
