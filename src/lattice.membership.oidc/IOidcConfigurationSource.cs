using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// The seam that supplies the cached, auto-refreshing OpenID Connect
/// configuration manager for a discovery-document address. Abstracted so tests
/// can inject an in-memory configuration (with test signing keys and a test
/// signing-algorithm list) and avoid any network call, while production
/// discovers and rotates JWKS from the live provider authority.
/// </summary>
/// <remarks>
/// This is a deliberate copy of the shape of the equivalent seam in
/// <c>Orleans.Lattice.Membership.Entra</c> rather than a shared type. Coupling
/// two independently versioned, released NuGet packages would force a lockstep
/// version bump on the Entra package for no functional gain, and hoisting the
/// seam into core <c>Orleans.Lattice.Membership</c> would add the
/// <c>Microsoft.IdentityModel.Protocols.OpenIdConnect</c> dependency to a
/// package that today needs only the token primitives.
/// </remarks>
internal interface IOidcConfigurationSource
{
    /// <summary>
    /// Returns a <see cref="BaseConfigurationManager"/> for the supplied OpenID
    /// Connect discovery document address. The same manager instance is reused
    /// across calls for the same address so its JWKS cache and refresh schedule
    /// are shared, never fetched per authentication.
    /// </summary>
    /// <param name="metadataAddress">The OIDC discovery document address. Must not be <c>null</c> or empty.</param>
    BaseConfigurationManager GetOrCreate(string metadataAddress);
}
