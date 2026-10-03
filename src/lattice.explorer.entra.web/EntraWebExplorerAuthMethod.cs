using Microsoft.Extensions.Options;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// The hosted-web Entra ID <see cref="IExplorerAuthMethod"/> for the <c>entra</c>
/// scheme. Where the interactive MSAL provider
/// (<c>Orleans.Lattice.Explorer.Entra</c>) runs a browser flow from the host
/// process, this provider serves a remote Blazor Server
/// circuit: the browser has already signed in through the ASP.NET OpenID Connect
/// middleware, so the challenge simply exchanges that session for a downstream
/// State API token via <see cref="IExplorerWebTokenAcquirer"/> and wires silent
/// renewal into an <see cref="ExplorerAccessTokenSource"/>.
/// </summary>
/// <remarks>
/// Registered <em>scoped</em> (per circuit), because the acquirer it depends on
/// reads the per-circuit authenticated user. Its scopes come from statically
/// configured <see cref="ExplorerEntraWebOptions.Scopes"/> when present; the
/// endpoint's advertised audience fills only an otherwise empty scope set.
/// </remarks>
public sealed class EntraWebExplorerAuthMethod : IExplorerAuthMethod
{
    private readonly IExplorerWebTokenAcquirer _acquirer;
    private readonly IOptionsMonitor<ExplorerEntraWebOptions> _options;

    /// <summary>Creates the hosted-web Entra auth method.</summary>
    /// <param name="acquirer">The web token acquirer (real Microsoft.Identity.Web, or a fake in tests).</param>
    /// <param name="options">Static fallback web-Entra configuration.</param>
    public EntraWebExplorerAuthMethod(
        IExplorerWebTokenAcquirer acquirer,
        IOptionsMonitor<ExplorerEntraWebOptions> options)
    {
        ArgumentNullException.ThrowIfNull(acquirer);
        ArgumentNullException.ThrowIfNull(options);
        _acquirer = acquirer;
        _options = options;
    }

    /// <inheritdoc />
    public string SchemeId => ExplorerAuthSchemes.Entra;

    /// <inheritdoc />
    public bool CanHandle(string advertisedScheme)
        => string.Equals(advertisedScheme, SchemeId, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public async Task<ExplorerAuthSignIn> ChallengeAsync(
        ExplorerAuthChallengeContext context,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        var scopes = ResolveScopes(context.Parameters, _options.CurrentValue, context.Endpoint);
        if (scopes.Count == 0)
        {
            throw new InvalidOperationException(
                "The hosted-web Entra login method needs at least one scope (the State API audience). Configure "
                + "ExplorerEntraWebOptions.Scopes, or connect to a State API that advertises the Entra audience.");
        }

        var initial = await _acquirer.AcquireTokenAsync(scopes, cancellationToken).ConfigureAwait(false);

        var source = new ExplorerAccessTokenSource(
            new ExplorerAccessToken { Token = initial.AccessToken, ExpiresOn = initial.ExpiresOn },
            async ct =>
            {
                try
                {
                    var renewed = await _acquirer.AcquireTokenAsync(scopes, ct).ConfigureAwait(false);
                    return new ExplorerAccessToken { Token = renewed.AccessToken, ExpiresOn = renewed.ExpiresOn };
                }
                catch (ExplorerWebReauthRequiredException)
                {
                    // Silent renewal is no longer possible; latch the source into
                    // its revoked state so the user is re-challenged (a fresh OIDC
                    // redirect on the next full page load) rather than dropped into
                    // a broken session.
                    return null;
                }
            },
            context.TimeProvider);

        var displayName = string.IsNullOrWhiteSpace(initial.Username) ? "Entra user" : initial.Username;
        return new ExplorerAuthSignIn
        {
            SchemeId = SchemeId,
            DisplayName = displayName,
            Authentication = LatticeCallAuthentication.Bearer(source),
        };
    }

    private static IReadOnlyList<string> ResolveScopes(
        IReadOnlyDictionary<string, string> parameters,
        ExplorerEntraWebOptions options,
        string? endpoint)
    {
        if (options.Scopes.Count > 0)
        {
            return options.Scopes.ToArray();
        }

        var audience = parameters.GetValueOrDefault(ExplorerAuthSchemes.AudienceParameter);
        if (string.IsNullOrWhiteSpace(audience))
        {
            return Array.Empty<string>();
        }

        // Nothing is configured locally in this branch, so the advertisement is
        // the only thing naming the resource the operator's token is minted for.
        // It is fetched over an unauthenticated RPC from the very endpoint the
        // token is then handed to, so it is admitted or refused, never trusted.
        EnsureAdvertisedAudienceIsAdmitted(audience, options.AllowedAudiences, endpoint);

        // An audience is a resource identifier that maps to the resource's default
        // scope; a value that already names a scope (ends with "/.default") is used
        // verbatim.
        var scope = audience.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? audience
            : $"{audience}/.default";
        return new[] { scope };
    }

    /// <summary>
    /// Admits an audience that came from the endpoint's advertisement, or refuses
    /// it. A hostile endpoint could otherwise name a resource of its choosing (for
    /// example Microsoft Graph) and have the console mint a delegated token for it
    /// on the signed-in operator's behalf, then collect that token as the bearer
    /// credential for its own calls.
    /// </summary>
    private static void EnsureAdvertisedAudienceIsAdmitted(
        string audience,
        IList<string> allowedAudiences,
        string? endpoint)
    {
        if (allowedAudiences.Count > 0)
        {
            foreach (var allowed in allowedAudiences)
            {
                if (string.Equals(allowed, audience, StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }

            throw new InvalidOperationException(
                $"The endpoint advertised the Entra audience '{audience}', which is not in "
                + "ExplorerEntraWebOptions.AllowedAudiences. A hostile endpoint could otherwise choose the resource "
                + "your token is minted for. Configure ExplorerEntraWebOptions.Scopes to pin the scope yourself, or "
                + "add the audience to AllowedAudiences to accept it.");
        }

        if (IsEndpointBoundResource(audience, endpoint))
        {
            return;
        }

        throw new InvalidOperationException(
            $"The endpoint advertised the Entra audience '{audience}', which is not an admitted resource. An "
            + "advertised audience is accepted only when it is an 'api://' resource identifier, or an https "
            + "resource whose host is the host of the endpoint being signed in to. A hostile endpoint could "
            + "otherwise choose the resource your token is minted for. Configure "
            + "ExplorerEntraWebOptions.Scopes to pin the scope yourself, or add the audience to "
            + "ExplorerEntraWebOptions.AllowedAudiences to accept it.");
    }

    /// <summary>
    /// <c>true</c> when <paramref name="audience"/> names a resource that belongs
    /// to the endpoint being signed in to: an <c>api://</c> application id URI, or
    /// an https resource sharing the endpoint's host (the verified-domain shape of
    /// an Entra application id URI). Every first-party Microsoft resource is an
    /// https identifier on a foreign host, so this admits the deployment shapes a
    /// State API actually takes and refuses the ones worth stealing a token for.
    /// </summary>
    private static bool IsEndpointBoundResource(string audience, string? endpoint)
    {
        var resource = audience.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? audience[..^"/.default".Length]
            : audience;

        if (!Uri.TryCreate(resource, UriKind.Absolute, out var uri))
        {
            return false;
        }

        if (string.Equals(uri.Scheme, "api", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(endpoint))
        {
            return false;
        }

        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var endpointUri)
            && !Uri.TryCreate($"https://{endpoint}", UriKind.Absolute, out endpointUri))
        {
            return false;
        }

        return string.Equals(uri.Host, endpointUri.Host, StringComparison.OrdinalIgnoreCase);
    }
}
