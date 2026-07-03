using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A deterministic in-test <see cref="ILatticeCredentialAuthenticator"/> that
/// avoids a real identity provider: it resolves the ambient credential's
/// <see cref="LatticeCredential.Token"/> directly as the subject id and reads an
/// optional comma-separated <c>groups</c> metadata entry as the token-asserted
/// groups. Selected only for credentials stamped with
/// <see cref="Scheme"/>, so it never shadows the fallback anonymous
/// authenticator for an unstamped (system-origin) turn.
/// </summary>
public sealed class TestCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    /// <summary>The scheme hint this authenticator claims.</summary>
    public const string Scheme = "test-scheme";

    /// <summary>The issuer stamped on the resolved principal.</summary>
    public const string Issuer = "https://issuer.auth.test/";

    /// <summary>The metadata key carrying the comma-separated token-asserted groups.</summary>
    public const string GroupsMetadataKey = "groups";

    /// <inheritdoc />
    public bool CanHandle(in LatticeCredential credential) =>
        string.Equals(credential.Scheme, Scheme, StringComparison.Ordinal);

    /// <inheritdoc />
    public ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyCollection<string>? groups = null;
        if (credential.Metadata is { } metadata &&
            metadata.TryGetValue(GroupsMetadataKey, out var joined) &&
            !string.IsNullOrEmpty(joined))
        {
            groups = joined.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        return new ValueTask<LatticePrincipal?>(
            new LatticePrincipal(credential.Token, Issuer, assertedGroups: groups));
    }
}
