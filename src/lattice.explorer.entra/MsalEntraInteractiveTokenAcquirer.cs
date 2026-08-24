using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Client;

namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// The production <see cref="IEntraInteractiveTokenAcquirer"/>, backed by MSAL's
/// public-client application. It drives the interactive browser (auth-code +
/// PKCE) or device-code flow for the first acquisition and MSAL's silent
/// acquisition for renewals. MSAL owns the in-memory token cache; no token or
/// refresh material is written to the explorer's config store.
/// </summary>
public sealed class MsalEntraInteractiveTokenAcquirer : IEntraInteractiveTokenAcquirer
{
    private readonly ExplorerEntraOptions _options;
    private readonly ConcurrentDictionary<string, IPublicClientApplication> _apps = new(StringComparer.Ordinal);

    /// <summary>Creates the acquirer over the Entra options.</summary>
    /// <param name="options">The Entra options (used for the device-code callback).</param>
    public MsalEntraInteractiveTokenAcquirer(IOptions<ExplorerEntraOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value;
    }

    /// <inheritdoc />
    public async Task<EntraTokenResult> AcquireInteractiveAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var app = GetOrCreateApp(request);

        AuthenticationResult result;
        if (request.UseDeviceCode)
        {
            result = await app
                .AcquireTokenWithDeviceCode(request.Scopes, info => OnDeviceCode(info, cancellationToken))
                .ExecuteAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            result = await app
                .AcquireTokenInteractive(request.Scopes)
                .ExecuteAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        return Map(result);
    }

    /// <inheritdoc />
    public async Task<EntraTokenResult?> AcquireSilentAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var app = GetOrCreateApp(request);

        var accounts = await app.GetAccountsAsync().ConfigureAwait(false);

        // Bind renewal to the account that actually signed in. When the request
        // carries no username (e.g. a hand-built request) fall back to the sole
        // cached account. When it names an account but that account is absent
        // from the cache, return null to force a re-challenge rather than
        // silently renewing with a different operator's identity - the shared
        // in-memory MSAL cache can hold more than one account, and grabbing an
        // arbitrary one is a cross-identity credential-confusion primitive.
        var account = string.IsNullOrEmpty(request.Username)
            ? accounts.FirstOrDefault()
            : accounts.FirstOrDefault(a => string.Equals(a.Username, request.Username, StringComparison.OrdinalIgnoreCase));
        if (account is null)
        {
            return null;
        }

        try
        {
            var result = await app
                .AcquireTokenSilent(request.Scopes, account)
                .ExecuteAsync(cancellationToken)
                .ConfigureAwait(false);
            return Map(result);
        }
        catch (MsalUiRequiredException)
        {
            // The refresh material is expired/revoked or consent was withdrawn:
            // signal the caller to re-challenge interactively.
            return null;
        }
    }

    private IPublicClientApplication GetOrCreateApp(EntraTokenRequest request)
        => _apps.GetOrAdd($"{request.ClientId}|{request.Authority}", _ =>
            PublicClientApplicationBuilder
                .Create(request.ClientId)
                .WithAuthority(request.Authority)
                .WithDefaultRedirectUri()
                .Build());

    private Task OnDeviceCode(DeviceCodeResult info, CancellationToken cancellationToken)
    {
        var callback = _options.DeviceCodeCallback;
        return callback is null
            ? Console.Out.WriteLineAsync(info.Message.AsMemory(), cancellationToken)
            : callback(info.Message, cancellationToken);
    }

    private static EntraTokenResult Map(AuthenticationResult result) => new()
    {
        AccessToken = result.AccessToken,
        ExpiresOn = result.ExpiresOn,
        Username = result.Account?.Username ?? string.Empty,
    };
}
