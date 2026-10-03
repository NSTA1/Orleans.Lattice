using Microsoft.Extensions.Options;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// The Entra ID <see cref="IExplorerAuthMethod"/>: runs an interactive OIDC
/// sign-in (auth-code + PKCE, or device-code) for the configured audience, then
/// hands the connection a live bearer credential that refreshes silently and
/// transparently. Its parameters (authority, tenant, client id, audience) come
/// from the statically configured <see cref="ExplorerEntraOptions"/> where they
/// are set, falling back to the endpoint's advertised scheme where they are
/// not; an advertised authority is admitted only when it is https and names a
/// recognised Entra login host.
/// </summary>
public sealed class EntraExplorerAuthMethod : IExplorerAuthMethod
{
    private readonly IEntraInteractiveTokenAcquirer _acquirer;
    private readonly IOptionsMonitor<ExplorerEntraOptions> _options;

    /// <summary>Creates the Entra auth method over the token acquirer and options.</summary>
    /// <param name="acquirer">The interactive/silent token acquirer (real MSAL, or a fake in tests).</param>
    /// <param name="options">Static fallback Entra configuration.</param>
    public EntraExplorerAuthMethod(
        IEntraInteractiveTokenAcquirer acquirer,
        IOptionsMonitor<ExplorerEntraOptions> options)
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

        var options = _options.CurrentValue;
        var request = BuildRequest(context, options);

        // Interactive acquisition first; on success the silent-renewal delegate
        // keeps the token fresh for the life of the session.
        var initial = await _acquirer.AcquireInteractiveAsync(request, cancellationToken).ConfigureAwait(false);

        // Bind silent renewal to the account that just signed in, so a shared
        // MSAL token cache holding more than one account never renews this
        // connection with a different operator's token.
        var renewalRequest = request with { Username = initial.Username };

        var source = new ExplorerAccessTokenSource(
            new ExplorerAccessToken { Token = initial.AccessToken, ExpiresOn = initial.ExpiresOn },
            async ct =>
            {
                var renewed = await _acquirer.AcquireSilentAsync(renewalRequest, ct).ConfigureAwait(false);
                return renewed is { } value
                    ? new ExplorerAccessToken { Token = value.AccessToken, ExpiresOn = value.ExpiresOn }
                    : null;
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

    private static EntraTokenRequest BuildRequest(ExplorerAuthChallengeContext context, ExplorerEntraOptions options)
    {
        var authority = ResolveAuthority(context.Parameters, options);
        var clientId = Resolve(options.ClientId, context.Parameters, ExplorerAuthSchemes.ClientIdParameter);
        var scopes = ResolveScopes(context.Parameters, options, context.Endpoint);

        if (string.IsNullOrWhiteSpace(authority))
        {
            throw new InvalidOperationException(
                "The Entra login method needs an authority. Configure ExplorerEntraOptions.Authority (or TenantId), "
                + "or connect to a State API that advertises the Entra authority.");
        }

        if (string.IsNullOrWhiteSpace(clientId))
        {
            throw new InvalidOperationException(
                "The Entra login method needs a client id. Configure ExplorerEntraOptions.ClientId, or connect to a "
                + "State API that advertises the Entra client id.");
        }

        if (scopes.Count == 0)
        {
            throw new InvalidOperationException(
                "The Entra login method needs at least one scope (the State API audience). Configure "
                + "ExplorerEntraOptions.Scopes, or connect to a State API that advertises the Entra audience.");
        }

        return new EntraTokenRequest
        {
            Authority = authority,
            ClientId = clientId,
            Scopes = scopes,
            UseDeviceCode = options.UseDeviceCode,
        };
    }

    private static string? ResolveAuthority(IReadOnlyDictionary<string, string> parameters, ExplorerEntraOptions options)
    {
        // Locally configured values win over the advertisement. The endpoint's
        // auth-scheme advertisement is fetched over an unauthenticated RPC from
        // the very endpoint the minted token is then handed to, so it is not a
        // trustworthy source for the identity provider the operator signs in
        // against. Configuration must not be silently overridden by it - the
        // same precedence ResolveScopes already applies, and the same one the
        // hosted-web provider applies.
        if (!string.IsNullOrWhiteSpace(options.Authority))
        {
            return options.Authority;
        }

        if (!string.IsNullOrWhiteSpace(options.TenantId))
        {
            return ComposeAuthority(options.TenantId);
        }

        var advertised = parameters.GetValueOrDefault(ExplorerAuthSchemes.AuthorityParameter);
        if (!string.IsNullOrWhiteSpace(advertised))
        {
            EnsureAdvertisedAuthorityIsAdmitted(advertised, options);
            return advertised;
        }

        var advertisedTenant = parameters.GetValueOrDefault(ExplorerAuthSchemes.TenantIdParameter);
        return string.IsNullOrWhiteSpace(advertisedTenant) ? null : ComposeAuthority(advertisedTenant);
    }

    private static string ComposeAuthority(string tenant) => $"https://login.microsoftonline.com/{tenant}";

    /// <summary>
    /// Admits an authority that came from the endpoint's advertisement, or
    /// refuses it. Nothing is configured locally in this branch, so the host is
    /// the only thing pinning which directory the operator is about to
    /// authenticate against; an unrecognised one is refused rather than used.
    /// </summary>
    private static void EnsureAdvertisedAuthorityIsAdmitted(string authority, ExplorerEntraOptions options)
    {
        if (!Uri.TryCreate(authority, UriKind.Absolute, out var uri)
            || !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The endpoint advertised the Entra authority '{authority}', which is not an absolute https URL. "
                + "An advertised authority is refused unless it is https and its host is admitted; configure "
                + "ExplorerEntraOptions.Authority (or TenantId) to pin the authority yourself.");
        }

        var allowed = options.AllowedAuthorityHosts.Count > 0
            ? options.AllowedAuthorityHosts
            : DefaultAuthorityHosts;

        foreach (var host in allowed)
        {
            if (string.Equals(host, uri.Host, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"The endpoint advertised the Entra authority '{authority}', whose host '{uri.Host}' is not an admitted "
            + "Entra login host. A hostile endpoint could otherwise choose the identity provider you sign in "
            + "against. Configure ExplorerEntraOptions.Authority (or TenantId) to pin the authority, or add the "
            + "host to ExplorerEntraOptions.AllowedAuthorityHosts to accept it.");
    }

    private static IReadOnlyList<string> ResolveScopes(IReadOnlyDictionary<string, string> parameters, ExplorerEntraOptions options, string? endpoint)
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
        // token is then handed to, so it is admitted or refused, never trusted -
        // the same rule EnsureAdvertisedAuthorityIsAdmitted applies to the
        // authority leg.
        EnsureAdvertisedAudienceIsAdmitted(audience, options, endpoint);

        // An audience is a resource identifier that maps to the resource's
        // default scope; a value that already names a scope (ends with
        // "/.default") is used verbatim.
        var scope = audience.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? audience
            : $"{audience}/.default";
        return new[] { scope };
    }

    /// <summary>
    /// Admits an audience that came from the endpoint's advertisement, or refuses
    /// it. Nothing is configured locally in this branch, so the advertisement is
    /// the only thing naming the resource the operator is about to mint a token
    /// for; a hostile endpoint could otherwise name a resource of its choosing
    /// (for example Microsoft Graph) and collect the resulting delegated token as
    /// the bearer credential for its own calls.
    /// </summary>
    private static void EnsureAdvertisedAudienceIsAdmitted(string audience, ExplorerEntraOptions options, string? endpoint)
    {
        if (options.AllowedAudiences.Count > 0)
        {
            foreach (var allowed in options.AllowedAudiences)
            {
                if (string.Equals(allowed, audience, StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }

            throw new InvalidOperationException(
                $"The endpoint advertised the Entra audience '{audience}', which is not in "
                + "ExplorerEntraOptions.AllowedAudiences. A hostile endpoint could otherwise choose the resource "
                + "your token is minted for. Configure ExplorerEntraOptions.Scopes to pin the scope yourself, or "
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
            + "otherwise choose the resource your token is minted for. Configure ExplorerEntraOptions.Scopes to "
            + "pin the scope yourself, or add the audience to ExplorerEntraOptions.AllowedAudiences to accept it.");
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

    private static string? Resolve(string? configured, IReadOnlyDictionary<string, string> parameters, string key)
        => string.IsNullOrWhiteSpace(configured) ? parameters.GetValueOrDefault(key) : configured;

    /// <summary>
    /// The well-known Entra login hosts an advertised authority may name when
    /// the operator has not supplied an allow-list of their own. Covers the
    /// public cloud and the sovereign clouds Entra serves.
    /// </summary>
    private static readonly string[] DefaultAuthorityHosts =
    [
        "login.microsoftonline.com",
        "login.microsoftonline.us",
        "login.partner.microsoftonline.cn",
        "login.microsoftonline.de",
    ];
}
