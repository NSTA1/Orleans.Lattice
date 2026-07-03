using Grpc.Core;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Bridges the caller identity carried on an inbound gRPC
/// <see cref="ServerCallContext"/> (request headers / transport credential) into
/// an ambient <see cref="LatticeCredential"/>, which the auth-API service stamps
/// on the Lattice credential context so the facade's administrator check can
/// resolve the caller's subject and authorize the admin operation.
/// </summary>
/// <remarks>
/// <para>
/// This is the identity seam for the control plane. A host that needs a bespoke
/// identity source (a client TLS certificate, a signed edge header, a
/// pre-resolved principal, and so on) registers its own implementation before
/// <c>AddLatticeAuthApiGrpc</c> runs; the built-in default reads a single
/// configurable bearer-style header
/// (<see cref="LatticeAuthApiGrpcOptions.CredentialHeaderName"/> /
/// <see cref="LatticeAuthApiGrpcOptions.CredentialScheme"/>).
/// </para>
/// <para>
/// <b>Precedence.</b> This runs independently of, and after, the transport-level
/// <see cref="ILatticeAuthApiAuthorizer"/> (the coarse allow / deny gate). The
/// transport authorizer decides whether a call may run at all; the credential
/// this bridge resolves then feeds the facade's own administrator check applied
/// to a call that was allowed to run.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no resolvable
/// credential) leaves the caller anonymous. An anonymous caller is default-denied
/// by the facade's administrator check on every operation, so a missing or
/// malformed credential header can never administer membership or policy.
/// </para>
/// </remarks>
public interface ILatticeAuthApiCredentialBridge
{
    /// <summary>
    /// Resolves the caller credential from <paramref name="context"/>, or
    /// <see langword="null"/> when the call carries no recognisable credential
    /// (the caller is then treated as anonymous).
    /// </summary>
    /// <param name="context">The inbound gRPC server call context.</param>
    /// <returns>
    /// The resolved <see cref="LatticeCredential"/>, or <see langword="null"/>
    /// when none is present.
    /// </returns>
    LatticeCredential? Resolve(ServerCallContext context);
}
