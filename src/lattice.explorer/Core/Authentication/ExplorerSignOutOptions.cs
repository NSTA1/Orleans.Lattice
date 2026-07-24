namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Configures how a UI head performs a <b>federated</b> sign-out - one that ends
/// the browser's hosted-web session (for example the OpenID Connect cookie and
/// the identity provider's end-session) in addition to dropping the local State
/// API credential. It carries the head-relative path of a server sign-out
/// endpoint so the core explorer can route the "Sign out" button through it
/// without taking a compile-time dependency on any particular sign-in provider
/// package (for example the hosted-web Entra provider, which maps the endpoint).
/// </summary>
/// <remarks>
/// Registered as a singleton by the auth registration; a provider package that
/// maps a federated sign-out endpoint (such as
/// <c>MapLatticeExplorerEntraWebSignOut</c>) overrides the registration to point
/// <see cref="FederatedSignOutPath"/> at that endpoint. When
/// <see cref="FederatedSignOutPath"/> is <see langword="null"/> (the default) the
/// UI falls back to its in-process sign-out
/// (<c>IExplorerAuthSession.LogoutAsync</c>) or, on the cookie web head, a plain
/// form post to <c>ExplorerAuthUiOptions.LogoutPath</c>. That local-only sign-out
/// is correct for a head with no separate browser session, but on a hosted-web
/// OpenID Connect head it would leave the still-valid session cookie in place and
/// the fallback authorization policy would silently re-authenticate the circuit -
/// which is exactly what the federated path prevents.
/// </remarks>
public sealed class ExplorerSignOutOptions
{
    /// <summary>
    /// The head-relative path of the federated sign-out endpoint the "Sign out"
    /// button posts to (with a full-page form post, guarded by an antiforgery
    /// token), or <see langword="null"/> (the default) when no federated endpoint
    /// is wired, in which case the UI performs its local-only sign-out instead.
    /// </summary>
    public string? FederatedSignOutPath { get; set; }
}
