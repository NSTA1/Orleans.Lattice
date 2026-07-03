using Microsoft.Identity.Client;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// The production <see cref="IEntraGraphTokenAcquirer"/>. It calls
/// <see cref="IConfidentialClientApplication.AcquireTokenForClient"/>, whose MSAL
/// token cache serves a valid token when one is cached and transparently acquires
/// a new one when it is not. The <see cref="EntraGraphTokenProvider"/> that wraps
/// this acquirer adds the single shared in-flight guard so concurrent lookups do
/// not stampede this call on a cold cache.
/// </summary>
internal sealed class MsalEntraGraphTokenAcquirer : IEntraGraphTokenAcquirer
{
    private readonly IConfidentialClientApplication _application;
    private readonly string[] _scopes;

    /// <summary>
    /// Initializes a new <see cref="MsalEntraGraphTokenAcquirer"/>.
    /// </summary>
    /// <param name="application">The confidential-client application. Must not be <c>null</c>.</param>
    /// <param name="scopes">The scopes requested for the app-only token. Must not be <c>null</c>.</param>
    public MsalEntraGraphTokenAcquirer(IConfidentialClientApplication application, IEnumerable<string> scopes)
    {
        ArgumentNullException.ThrowIfNull(application);
        ArgumentNullException.ThrowIfNull(scopes);
        _application = application;
        _scopes = scopes.ToArray();
    }

    /// <inheritdoc />
    public async Task<EntraGraphToken> AcquireAsync(CancellationToken cancellationToken)
    {
        var result = await _application
            .AcquireTokenForClient(_scopes)
            .ExecuteAsync(cancellationToken)
            .ConfigureAwait(false);

        return new EntraGraphToken(result.AccessToken, result.ExpiresOn);
    }
}
