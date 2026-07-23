using Grpc.Core;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Authorization seam for the replication control-API gRPC surface. A host
/// supplies an implementation to decide whether a given inbound call is
/// permitted to drive runtime replication configuration. Enabling replication
/// egresses data to another cluster - a powerful, destructive-adjacent
/// operation - so the binding ships with a default-deny posture: unless a host
/// opts in (either by registering <see cref="AllowAllReplicationApiAuthorizer"/>
/// / a custom authorizer, or by turning enforcement off), inbound calls are
/// rejected with <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeReplicationApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may drive the replication control
    /// API. Implementations typically inspect request headers (a bearer token, a
    /// shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeReplicationApiAuthorizationContext.Call"/>, and may scope
    /// the decision to the call's
    /// <see cref="LatticeReplicationApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeReplicationApiAuthorizationContext.TargetId"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeReplicationApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeReplicationApiAuthorizer"/> that rejects every
/// call. Registered automatically so a host that maps the replication
/// control-API gRPC surface without configuring authorization fails closed
/// rather than exposing runtime replication configuration unauthenticated.
/// </summary>
public sealed class DenyAllReplicationApiAuthorizer : ILatticeReplicationApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeReplicationApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeReplicationApiAuthorizer"/> that permits every call.
/// Intended for trusted-network deployments where the replication control API is
/// reachable only from an operator dashboard behind a separate authentication
/// boundary. Register explicitly to override the default-deny posture.
/// </summary>
public sealed class AllowAllReplicationApiAuthorizer : ILatticeReplicationApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeReplicationApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
