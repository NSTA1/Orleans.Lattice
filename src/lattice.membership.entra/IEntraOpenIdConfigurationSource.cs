using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// The seam that supplies the cached, auto-refreshing OIDC configuration manager
/// for an Entra metadata endpoint. Abstracted so tests can inject an in-memory
/// configuration (with test signing keys) and avoid any network call, while
/// production discovers and rotates JWKS from the live tenant authority.
/// </summary>
internal interface IEntraOpenIdConfigurationSource
{
    /// <summary>
    /// Returns a <see cref="BaseConfigurationManager"/> for the supplied OIDC
    /// metadata address. The same manager instance is reused across calls for the
    /// same address so its JWKS cache and refresh schedule are shared, never
    /// fetched per authentication.
    /// </summary>
    /// <param name="metadataAddress">The OIDC discovery document address. Must not be <c>null</c> or empty.</param>
    BaseConfigurationManager GetOrCreate(string metadataAddress);
}
