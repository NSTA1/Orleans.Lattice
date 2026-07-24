namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Carries additional Content-Security-Policy source expressions the explorer web
/// head folds into the security-header middleware's policy. A federated sign-in
/// provider (for example the hosted-web Entra provider) contributes the identity
/// provider's origin here so the "Sign out" form is not blocked by the web head's
/// default <c>form-action 'self'</c> directive: the button POSTs to the local
/// federated sign-out endpoint, which then redirects the browser to the identity
/// provider's end-session URL, and browsers enforce <c>form-action</c> across the
/// whole redirect chain - so that cross-origin end-session target must be an
/// allowed <c>form-action</c> source or the POST is blocked.
/// </summary>
/// <remarks>
/// This is the CSP counterpart to <see cref="ExplorerSignOutOptions"/>: a provider
/// package that maps a federated sign-out endpoint also declares which external
/// origin that endpoint redirects to, without the core explorer taking a
/// compile-time dependency on the provider. A provider contributes with
/// <c>services.Configure&lt;ExplorerContentSecurityPolicyOptions&gt;(...)</c> and the
/// web head's security-header middleware reads the accumulated set once, at
/// construction. With no contributions the emitted policy is byte-identical to the
/// default (<c>form-action 'self'</c>), so a head with no federated provider keeps
/// the tightest policy.
/// </remarks>
public sealed class ExplorerContentSecurityPolicyOptions
{
    /// <summary>
    /// Extra sources appended to the CSP <c>form-action</c> directive (which
    /// already includes <c>'self'</c>). Each entry is a single CSP source
    /// expression - typically an origin such as
    /// <c>https://login.microsoftonline.com</c>. Empty by default. A source that
    /// contains whitespace or a directive/policy separator (<c>;</c> or <c>,</c>)
    /// is dropped when the header is composed, so a malformed contribution cannot
    /// inject an unrelated directive.
    /// </summary>
    public IList<string> AdditionalFormActionSources { get; } = new List<string>();
}
