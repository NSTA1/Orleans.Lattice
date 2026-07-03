using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Coarse authorization seam for the write-capable data-API gRPC surface. A host
/// supplies an implementation to decide whether a given inbound call is permitted
/// to reach the data plane at all. This is the transport-level gate that runs
/// first; the per-tree / per-key enforcement is applied afterwards by the gated
/// <see cref="ILattice"/> surface using the caller's resolved subject. Because
/// this surface mutates cluster state, the binding ships with a default-deny
/// posture: unless a host opts in (either by registering
/// <see cref="AllowAllDataApiAuthorizer"/> / a custom authorizer, or by turning
/// enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeDataApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may reach the data plane.
    /// Implementations typically inspect request headers (a bearer token, a
    /// shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeDataApiAuthorizationContext.Call"/>, and may scope the
    /// decision to the call's
    /// <see cref="LatticeDataApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeDataApiAuthorizationContext.TargetTreeId"/> (for
    /// example, allow reads but deny writes, or restrict a caller to a specific
    /// set of trees).
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeDataApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeDataApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the data-API gRPC surface
/// without configuring authorization fails closed rather than exposing a
/// write-capable data plane unauthenticated.
/// </summary>
public sealed class DenyAllDataApiAuthorizer : ILatticeDataApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeDataApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeDataApiAuthorizer"/> that permits every call to
/// reach the data plane, deferring all enforcement to the per-tree / per-key
/// access gate on the gated <see cref="ILattice"/> surface. Intended for
/// deployments where the coarse transport gate adds no value beyond the gate's
/// own subject-scoped decisions (for example a trusted-network endpoint that
/// still stamps a per-caller credential). Register explicitly to override the
/// default-deny posture.
/// </summary>
public sealed class AllowAllDataApiAuthorizer : ILatticeDataApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeDataApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
