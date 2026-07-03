using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Transport-level meta-authorization seam for the auth-API gRPC surface - the
/// remote membership and policy control plane. A host supplies an implementation
/// to decide whether a given inbound admin call may run at all. This is the
/// first of two gates: it runs at the transport edge, before any facade work; the
/// facade's own per-call administrator check then re-authorizes the resolved
/// caller's subject against the reserved policy tree. Administering
/// authorization is the most sensitive surface in the cluster, so the binding
/// ships with a default-deny posture: unless a host opts in (either by
/// registering <see cref="AllowAllAuthApiAuthorizer"/> / a custom authorizer, or
/// by turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeAuthApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may reach the auth-API facade.
    /// Implementations typically inspect request headers (a bearer token, a
    /// shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeAuthApiAuthorizationContext.Call"/>, and may scope the
    /// decision to the call's
    /// <see cref="LatticeAuthApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeAuthApiAuthorizationContext.TargetId"/> (for example,
    /// permit policy reads but deny policy writes, or restrict membership
    /// administration to a subset of callers). Allowing a call here does not
    /// grant it: the facade's administrator check still runs against the resolved
    /// caller's subject.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeAuthApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeAuthApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the auth-API gRPC surface
/// without configuring authorization fails closed rather than exposing the
/// membership and policy control plane at the transport edge.
/// </summary>
public sealed class DenyAllAuthApiAuthorizer : ILatticeAuthApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeAuthApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeAuthApiAuthorizer"/> that permits every call to
/// reach the auth-API facade, deferring all enforcement to the facade's own
/// per-call administrator check applied to the resolved caller's subject.
/// Intended for deployments where the coarse transport gate adds no value beyond
/// the facade's subject-scoped decision (for example a trusted-network endpoint
/// that still stamps a per-caller credential). Register explicitly to override
/// the default-deny posture.
/// </summary>
public sealed class AllowAllAuthApiAuthorizer : ILatticeAuthApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeAuthApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
