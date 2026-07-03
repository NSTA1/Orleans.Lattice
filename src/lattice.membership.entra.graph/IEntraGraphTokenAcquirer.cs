namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The seam over the raw MSAL app-only token acquisition. Abstracted so tests can
/// fake token issuance (with controllable expiry and a call counter) without any
/// live Azure AD / MSAL network call. The production implementation wraps
/// <c>IConfidentialClientApplication.AcquireTokenForClient</c>, whose own cache
/// serves and refreshes the token transparently.
/// </summary>
internal interface IEntraGraphTokenAcquirer
{
    /// <summary>Acquires a fresh app-only access token.</summary>
    /// <param name="cancellationToken">Cancels the acquisition.</param>
    Task<EntraGraphToken> AcquireAsync(CancellationToken cancellationToken);
}
