using Grpc.Core;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Bridges the caller identity carried on an inbound gRPC
/// <see cref="ServerCallContext"/> (request headers / transport credential) into
/// an ambient <see cref="LatticeCredential"/>, which the backup control path
/// stamps on the Lattice credential context so the backup access gate can
/// resolve the caller's subject and authorize each operation.
/// </summary>
/// <remarks>
/// <para>
/// This is the identity seam for auth-backed backup control. A host that needs a
/// bespoke identity source (a client TLS certificate, a signed edge header, a
/// pre-resolved principal, and so on) registers its own implementation before
/// <c>AddLatticeBackupApiGrpc</c> runs; the built-in default reads a single
/// configurable bearer-style header
/// (<see cref="LatticeBackupApiGrpcOptions.CredentialHeaderName"/> /
/// <see cref="LatticeBackupApiGrpcOptions.CredentialScheme"/>).
/// </para>
/// <para>
/// <b>Precedence.</b> This runs independently of, and after, the transport-level
/// <see cref="ILatticeBackupApiAuthorizer"/> (the coarse allow / deny gate keyed
/// by headers, operation, and target). The transport authorizer decides whether
/// a call may run at all; the credential this bridge resolves then feeds the
/// backup engine's own fail-closed access gate applied to a call that was
/// allowed to run.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no resolvable
/// credential) leaves the caller anonymous. When auth-backed backup control is
/// active an anonymous caller is denied, so a missing or malformed credential
/// header can never drive a destructive operation.
/// </para>
/// </remarks>
public interface ILatticeBackupApiCredentialBridge
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
