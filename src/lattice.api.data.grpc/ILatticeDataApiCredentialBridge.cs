using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Bridges the caller identity carried on an inbound gRPC
/// <see cref="ServerCallContext"/> (request headers / transport credential) into
/// an ambient <see cref="LatticeCredential"/>, which the data-API service stamps
/// on the Lattice credential context so the data-plane access gate can resolve
/// the caller's subject and authorize every mutation and read.
/// </summary>
/// <remarks>
/// <para>
/// This is the identity seam for the write-capable data API. A host that needs a
/// bespoke identity source (a client TLS certificate, a signed edge header, a
/// pre-resolved principal, and so on) registers its own implementation before
/// <c>AddLatticeDataApiGrpc</c> runs; the built-in default reads a single
/// configurable bearer-style header
/// (<see cref="LatticeDataApiGrpcOptions.CredentialHeaderName"/> /
/// <see cref="LatticeDataApiGrpcOptions.CredentialScheme"/>).
/// </para>
/// <para>
/// <b>Precedence.</b> This runs independently of, and after, the transport-level
/// <see cref="ILatticeDataApiAuthorizer"/> (the coarse allow / deny gate keyed
/// by headers, operation, and target tree). The transport authorizer decides
/// whether a call may run at all; the credential this bridge resolves then feeds
/// the per-tree / per-key enforcement applied by the gated <see cref="ILattice"/>
/// surface to a call that was allowed to run.
/// </para>
/// <para>
/// <b>Fail-closed.</b> Returning <see langword="null"/> (no resolvable
/// credential) leaves the caller anonymous. An anonymous caller is default-denied
/// by the access gate on every mutation and read, so a missing or malformed
/// credential header can never write to, delete from, or read cluster state.
/// </para>
/// </remarks>
public interface ILatticeDataApiCredentialBridge
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
