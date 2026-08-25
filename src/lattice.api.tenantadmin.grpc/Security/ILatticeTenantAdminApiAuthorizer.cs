using Grpc.Core;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Authorization seam for the tenant-administration control-API gRPC surface. A
/// host supplies an implementation to decide whether a given inbound call is
/// permitted to drive tenant administration. The control API exposes sensitive
/// and destructive tenant lifecycle operations (create, suspend, resume, and
/// delete with tree cascade), so the binding ships with a default-deny posture:
/// unless a host opts in (either by registering
/// <see cref="AllowAllTenantAdminApiAuthorizer"/> / a custom authorizer, or by
/// turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeTenantAdminApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may drive the tenant-administration
    /// control API. Implementations typically inspect request headers (a bearer
    /// token, a shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeTenantAdminApiAuthorizationContext.Call"/>, and may scope
    /// the decision to the call's
    /// <see cref="LatticeTenantAdminApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeTenantAdminApiAuthorizationContext.TargetId"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeTenantAdminApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the tenant-administration
/// control-API gRPC surface without configuring authorization fails closed rather
/// than exposing tenant-administration operations unauthenticated.
/// </summary>
public sealed class DenyTenantAdminApiAuthorizer : ILatticeTenantAdminApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeTenantAdminApiAuthorizer"/> that permits every call.
/// Intended for trusted-network deployments where the tenant-administration
/// control API is reachable only from an operator dashboard behind a separate
/// authentication boundary. Register explicitly to override the default-deny
/// posture.
/// </summary>
public sealed class AllowAllTenantAdminApiAuthorizer : ILatticeTenantAdminApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeTenantAdminApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
