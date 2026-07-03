using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth.Grpc.Tests;

/// <summary>
/// A deterministic in-test <see cref="ILatticeCredentialAuthenticator"/> for the
/// Api.Auth.Grpc fixtures: it resolves the ambient credential's
/// <see cref="LatticeCredential.Token"/> directly as the subject id. Selected
/// only for credentials stamped with <see cref="Scheme"/>, so it never shadows
/// the anonymous fallback for an unstamped (system-origin) turn. A per-assembly
/// copy of the Auth-package test authenticator (internals are not shared across
/// test assemblies).
/// </summary>
public sealed class ApiAuthGrpcTestCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint this authenticator claims.</summary>
    public const string Scheme = "test-scheme";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.api-auth-grpc.test/";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
        => new(new LatticePrincipal(credential.Token, Issuer));
}
