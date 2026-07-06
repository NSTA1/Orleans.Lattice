using Grpc.Core;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Authorization seam for the backup control-API gRPC surface. A host supplies
/// an implementation to decide whether a given inbound call is permitted to
/// drive backup and restore. The control API exposes destructive and sensitive
/// operations (capture, delete, restore, artifact export), so the binding ships
/// with a default-deny posture: unless a host opts in (either by registering
/// <see cref="AllowAllBackupApiAuthorizer"/> / a custom authorizer, or by
/// turning enforcement off), inbound calls are rejected with
/// <see cref="StatusCode.PermissionDenied"/>.
/// </summary>
public interface ILatticeBackupApiAuthorizer
{
    /// <summary>
    /// Decides whether the inbound call described by
    /// <paramref name="authorizationContext"/> may drive the backup control API.
    /// Implementations typically inspect request headers (a bearer token, a
    /// shared secret, a client certificate claim) exposed through
    /// <see cref="LatticeBackupApiAuthorizationContext.Call"/>, and may scope the
    /// decision to the call's
    /// <see cref="LatticeBackupApiAuthorizationContext.Operation"/> and
    /// <see cref="LatticeBackupApiAuthorizationContext.TargetId"/>.
    /// </summary>
    /// <param name="authorizationContext">The decoded inbound call description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> to allow the call; otherwise <see langword="false"/>.</returns>
    Task<bool> IsAuthorizedAsync(LatticeBackupApiAuthorizationContext authorizationContext, CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="ILatticeBackupApiAuthorizer"/> that rejects every call.
/// Registered automatically so a host that maps the backup control-API gRPC
/// surface without configuring authorization fails closed rather than exposing
/// destructive backup operations unauthenticated.
/// </summary>
public sealed class DenyAllBackupApiAuthorizer : ILatticeBackupApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeBackupApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(false);
}

/// <summary>
/// Opt-in <see cref="ILatticeBackupApiAuthorizer"/> that permits every call.
/// Intended for trusted-network deployments where the backup control API is
/// reachable only from an operator dashboard behind a separate authentication
/// boundary. Register explicitly to override the default-deny posture.
/// </summary>
public sealed class AllowAllBackupApiAuthorizer : ILatticeBackupApiAuthorizer
{
    /// <inheritdoc />
    public Task<bool> IsAuthorizedAsync(LatticeBackupApiAuthorizationContext authorizationContext, CancellationToken cancellationToken)
        => Task.FromResult(true);
}
