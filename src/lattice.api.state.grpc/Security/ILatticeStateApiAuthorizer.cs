using Grpc.Core;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Authorization seam for the read-only state-API gRPC surface. A host
/// supplies an implementation to decide whether a given inbound call is
/// permitted to read cluster state. The state API exposes potentially
/// sensitive structural and entry-level data, so the binding ships with a
/// default-deny posture: unless a host opts in (either by registering
/// <see cref="AllowAllStateApiAuthorizer"/> / a custom authorizer, or by
/// turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeStateApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by <paramref name="context"/>
    /// may read cluster state. Implementations typically inspect request
    /// headers (a bearer token, a shared secret, a client certificate claim)
    /// exposed through the call context.
    /// </summary>
    /// <param name="context">The server call context for the inbound RPC.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(ServerCallContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeStateApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the state-API gRPC surface
/// without configuring authorization fails closed rather than exposing
/// cluster state unauthenticated.
/// </summary>
public sealed class DenyAllStateApiAuthorizer : ILatticeStateApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(ServerCallContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return Task.FromResult(false);
    }
}

/// <summary>
/// Opt-in <see cref="ILatticeStateApiAuthorizer"/> that permits every call.
/// Intended for trusted-network deployments where the state API is reachable
/// only from an operator dashboard behind a separate authentication boundary.
/// Register explicitly to override the default-deny posture.
/// </summary>
public sealed class AllowAllStateApiAuthorizer : ILatticeStateApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(ServerCallContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return Task.FromResult(true);
    }
}
