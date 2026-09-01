namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// The standard OpenID Connect token claim names this package reads by default.
/// Centralized so the authenticator, its default option values, and the
/// documentation agree on the exact claim keys a conformant provider emits.
/// </summary>
/// <remarks>
/// Every one of these is a default, not a hard-coded convention: a deployment
/// whose provider emits group membership under a different claim name overrides
/// <see cref="LatticeOidcAuthenticatorOptions.GroupClaimTypes"/> (or
/// <see cref="LatticeOidcAuthenticatorOptions.SubjectClaimTypes"/>) instead of
/// changing code.
/// </remarks>
public static class OidcClaimNames
{
    /// <summary>
    /// The subject identifier (<c>sub</c>): the locally unique, never-reassigned
    /// identifier the provider asserts for the end user. Section 2 of the OpenID
    /// Connect Core specification makes this the stable subject id.
    /// </summary>
    public const string Subject = "sub";

    /// <summary>
    /// The group memberships claim (<c>groups</c>), the de facto convention
    /// across Okta, Auth0, Keycloak, and Ping for asserting group membership.
    /// </summary>
    public const string Groups = "groups";

    /// <summary>The plural roles claim (<c>roles</c>), emitted by providers that model roles separately from groups.</summary>
    public const string Roles = "roles";

    /// <summary>The singular role claim (<c>role</c>), emitted by providers that repeat a single-valued claim per role.</summary>
    public const string Role = "role";
}
