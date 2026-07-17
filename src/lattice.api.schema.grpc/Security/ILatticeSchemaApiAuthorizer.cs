using Grpc.Core;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Authorization seam for the schema control-API gRPC surface. A host supplies
/// an implementation to decide whether a given inbound call is permitted to
/// drive schema management. The control API exposes sensitive and destructive
/// operations (set / clear policy, version-config changes, remediation), so the
/// binding ships with a default-deny posture: unless a host opts in (either by
/// registering <see cref="AllowAllSchemaApiAuthorizer"/> / a custom authorizer,
/// or by turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeSchemaApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may drive the schema control API.
    /// Implementations typically inspect request headers (a bearer token, a
    /// shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeSchemaApiAuthorizationContext.Call"/>, and may scope the
    /// decision to the call's
    /// <see cref="LatticeSchemaApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeSchemaApiAuthorizationContext.TargetId"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeSchemaApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeSchemaApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the schema control-API gRPC
/// surface without configuring authorization fails closed rather than exposing
/// schema-management operations unauthenticated.
/// </summary>
public sealed class DenySchemaApiAuthorizer : ILatticeSchemaApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeSchemaApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeSchemaApiAuthorizer"/> that permits every call.
/// Intended for trusted-network deployments where the schema control API is
/// reachable only from an operator dashboard behind a separate authentication
/// boundary. Register explicitly to override the default-deny posture.
/// </summary>
public sealed class AllowAllSchemaApiAuthorizer : ILatticeSchemaApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeSchemaApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
