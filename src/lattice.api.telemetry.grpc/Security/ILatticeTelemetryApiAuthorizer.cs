using Grpc.Core;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Authorization seam for the telemetry gRPC surface. A host supplies an
/// implementation to decide whether a given inbound call may reach the telemetry
/// facade at all. The surface is read-only, but it reads operational time series
/// for a whole cluster, so the binding ships with a default-deny posture: unless
/// a host opts in (either by registering
/// <see cref="AllowAllTelemetryApiAuthorizer"/> / a custom authorizer, or by
/// turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
/// <remarks>
/// This gate is coarse and transport-level. It decides <em>whether the call
/// runs</em>; it never decides <em>what the caller may see</em>. Per-caller
/// entitlement - which catalogue entries are offered, and which tenant's series a
/// query is evaluated over - is derived server-side by the facade from the
/// authenticated caller and is not this seam's concern. An implementation must
/// therefore never attempt to widen or narrow tenant scope from here.
/// </remarks>
public interface ILatticeTelemetryApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may reach the telemetry facade.
    /// Implementations typically inspect request headers (a bearer token, a shared
    /// secret, a client certificate claim) exposed through
    /// <see cref="LatticeTelemetryApiAuthorizationContext.Call"/>, and may scope
    /// the decision to the call's
    /// <see cref="LatticeTelemetryApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeTelemetryApiAuthorizationContext.TargetId"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeTelemetryApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeTelemetryApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the telemetry gRPC surface
/// without configuring authorization fails closed rather than exposing cluster
/// telemetry unauthenticated.
/// </summary>
public sealed class DenyTelemetryApiAuthorizer : ILatticeTelemetryApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeTelemetryApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeTelemetryApiAuthorizer"/> that permits every call.
/// Intended for deployments where the telemetry endpoint is reachable only from
/// behind a separate authentication boundary. Register explicitly to override the
/// default-deny posture. Permitting a call still does not widen what the caller
/// sees: the facade scopes the catalogue and every query to the caller
/// server-side.
/// </summary>
public sealed class AllowAllTelemetryApiAuthorizer : ILatticeTelemetryApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeTelemetryApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
