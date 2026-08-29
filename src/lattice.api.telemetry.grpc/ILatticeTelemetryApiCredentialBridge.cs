using Grpc.Core;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Bridges the caller identity carried on an inbound gRPC
/// <see cref="ServerCallContext"/> (request headers / transport credential) into
/// an ambient <see cref="LatticeCredential"/>, which the telemetry path stamps on
/// the Lattice credential context so the facade can resolve the caller's subject,
/// scope the catalogue to that caller's entitlement, and derive the effective
/// tenant server-side.
/// </summary>
/// <remarks>
/// <para>
/// This is the identity seam for auth-backed telemetry. A host that needs a
/// bespoke identity source (a client TLS certificate, a signed edge header, a
/// pre-resolved principal, and so on) registers its own implementation before
/// <c>AddLatticeTelemetryApiGrpc</c> runs; the built-in default reads a single
/// configurable bearer-style header
/// (<see cref="LatticeTelemetryApiGrpcOptions.CredentialHeaderName"/> /
/// <see cref="LatticeTelemetryApiGrpcOptions.CredentialScheme"/>).
/// </para>
/// <para>
/// <b>Precedence.</b> This runs independently of, and after, the transport-level
/// <see cref="ILatticeTelemetryApiAuthorizer"/> (the coarse allow / deny gate
/// keyed by headers and operation). The transport authorizer decides whether a
/// call may run at all; the credential this bridge resolves then feeds the
/// facade's own fail-closed gate applied to a call that was allowed to run.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no resolvable credential)
/// leaves the caller anonymous. The facade scopes an anonymous caller to its own
/// derived tenant and offers it only the catalogue entries it is entitled to run,
/// so a missing or malformed credential header can never widen a caller's view.
/// </para>
/// </remarks>
public interface ILatticeTelemetryApiCredentialBridge
{
    /// <summary>
    /// Resolves the caller credential from <paramref name="context"/>, or
    /// <see langword="null"/> when the call carries no recognisable credential (the
    /// caller is then treated as anonymous).
    /// </summary>
    /// <param name="context">The inbound gRPC server call context.</param>
    /// <returns>
    /// The resolved <see cref="LatticeCredential"/>, or <see langword="null"/> when
    /// none is present.
    /// </returns>
    LatticeCredential? Resolve(ServerCallContext context);
}
