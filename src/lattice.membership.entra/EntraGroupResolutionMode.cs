namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// Controls how <see cref="EntraCredentialAuthenticator"/> resolves the caller's
/// group membership when a token's <c>groups</c> claim has overflowed (the Entra
/// groups-overage case).
/// </summary>
public enum EntraGroupResolutionMode
{
    /// <summary>
    /// The default: only the group ids the token carries are used. On overage the
    /// token carries no <c>groups</c> claim, so the authenticator surfaces
    /// whatever the token asserts (roles, and any groups that fit) and leaves full
    /// membership to the directory merge performed upstream by the subject mapper.
    /// No external membership lookup is ever made.
    /// </summary>
    TokenOnly = 0,

    /// <summary>
    /// On overage, resolve the caller's full group membership through the
    /// registered <see cref="IEntraGroupResolver"/>. When no resolver is
    /// registered the authenticator falls back to the token-only behaviour rather
    /// than failing, so authentication never throws because a resolver is absent.
    /// </summary>
    ResolveOnOverage = 1,
}
