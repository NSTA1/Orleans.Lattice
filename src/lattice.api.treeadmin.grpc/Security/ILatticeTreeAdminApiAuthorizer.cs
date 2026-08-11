using Grpc.Core;

namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Authorization seam for the tree-administration control-API gRPC surface. A
/// host supplies an implementation to decide whether a given inbound call is
/// permitted to drive tree administration. The control API is designed to expose
/// sensitive and destructive whole-tree operations (the later releases add
/// bulk-load, delete, resize, reshard), so the binding ships with a default-deny
/// posture: unless a host opts in (either by registering
/// <see cref="AllowAllTreeAdminApiAuthorizer"/> / a custom authorizer, or by
/// turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeTreeAdminApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may drive the tree-administration
    /// control API. Implementations typically inspect request headers (a bearer
    /// token, a shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeTreeAdminApiAuthorizationContext.Call"/>, and may scope the
    /// decision to the call's
    /// <see cref="LatticeTreeAdminApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeTreeAdminApiAuthorizationContext.TargetId"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeTreeAdminApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the tree-administration
/// control-API gRPC surface without configuring authorization fails closed rather
/// than exposing tree-administration operations unauthenticated.
/// </summary>
public sealed class DenyTreeAdminApiAuthorizer : ILatticeTreeAdminApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeTreeAdminApiAuthorizer"/> that permits every call.
/// Intended for trusted-network deployments where the tree-administration control
/// API is reachable only from an operator dashboard behind a separate
/// authentication boundary. Register explicitly to override the default-deny
/// posture.
/// </summary>
public sealed class AllowAllTreeAdminApiAuthorizer : ILatticeTreeAdminApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeTreeAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
