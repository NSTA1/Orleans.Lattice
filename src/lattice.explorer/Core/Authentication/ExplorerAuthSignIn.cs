using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// The result of a successful <see cref="IExplorerAuthMethod.ChallengeAsync"/>:
/// the scheme that produced it, a friendly display name for the signed-in
/// identity, and the <see cref="LatticeCallAuthentication"/> the connection
/// attaches to every call. For a static scheme (Basic) the authentication
/// carries a fixed header; for a token scheme it carries a live credential
/// provider that refreshes transparently.
/// </summary>
public sealed record ExplorerAuthSignIn
{
    /// <summary>The scheme id that produced this sign-in.</summary>
    public required string SchemeId { get; init; }

    /// <summary>A friendly name for the signed-in identity (a username or account name).</summary>
    public required string DisplayName { get; init; }

    /// <summary>The authentication seam applied to the connection.</summary>
    public required LatticeCallAuthentication Authentication { get; init; }
}
