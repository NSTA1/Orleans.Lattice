using Microsoft.Kiota.Abstractions;
using Microsoft.Kiota.Abstractions.Authentication;

namespace Orleans.Lattice.Membership.Entra.Graph;

/// <summary>
/// A Kiota <see cref="IAuthenticationProvider"/> that stamps each Microsoft Graph
/// request with the app-only bearer token managed by
/// <see cref="EntraGraphTokenProvider"/>. Because it defers to the shared token
/// provider, every Graph request rides the same cached, transparently refreshed,
/// single-flight token.
/// </summary>
internal sealed class EntraGraphTokenAuthenticationProvider : IAuthenticationProvider
{
    private const string AuthorizationHeader = "Authorization";
    private readonly EntraGraphTokenProvider _tokenProvider;

    /// <summary>
    /// Initializes a new <see cref="EntraGraphTokenAuthenticationProvider"/>.
    /// </summary>
    /// <param name="tokenProvider">The shared app-only token provider. Must not be <c>null</c>.</param>
    public EntraGraphTokenAuthenticationProvider(EntraGraphTokenProvider tokenProvider)
    {
        ArgumentNullException.ThrowIfNull(tokenProvider);
        _tokenProvider = tokenProvider;
    }

    /// <inheritdoc />
    public async Task AuthenticateRequestAsync(
        RequestInformation request,
        Dictionary<string, object>? additionalAuthenticationContext = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var token = await _tokenProvider.GetAccessTokenAsync(cancellationToken).ConfigureAwait(false);
        request.Headers.Remove(AuthorizationHeader);
        request.Headers.Add(AuthorizationHeader, $"Bearer {token}");
    }
}
