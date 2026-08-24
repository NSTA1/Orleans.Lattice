using Microsoft.Extensions.Options;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// The Entra ID <see cref="IExplorerAuthMethod"/>: runs an interactive OIDC
/// sign-in (auth-code + PKCE, or device-code) for the configured audience, then
/// hands the connection a live bearer credential that refreshes silently and
/// transparently. It resolves its parameters from the endpoint's advertised
/// scheme (authority, tenant, client id, audience), falling back to statically
/// configured <see cref="ExplorerEntraOptions"/>.
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
        var clientId = Resolve(context.Parameters, ExplorerAuthSchemes.ClientIdParameter, options.ClientId);
        var scopes = ResolveScopes(context.Parameters, options);

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
        var advertised = parameters.GetValueOrDefault(ExplorerAuthSchemes.AuthorityParameter);
        if (!string.IsNullOrWhiteSpace(advertised))
        {
            return advertised;
        }

        if (!string.IsNullOrWhiteSpace(options.Authority))
        {
            return options.Authority;
        }

        var tenant = Resolve(parameters, ExplorerAuthSchemes.TenantIdParameter, options.TenantId);
        return string.IsNullOrWhiteSpace(tenant) ? null : $"https://login.microsoftonline.com/{tenant}";
    }

    private static IReadOnlyList<string> ResolveScopes(IReadOnlyDictionary<string, string> parameters, ExplorerEntraOptions options)
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

        // An audience is a resource identifier that maps to the resource's
        // default scope; a value that already names a scope (ends with
        // "/.default") is used verbatim.
        var scope = audience.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? audience
            : $"{audience}/.default";
        return new[] { scope };
    }

    private static string? Resolve(IReadOnlyDictionary<string, string> parameters, string key, string? fallback)
    {
        var advertised = parameters.GetValueOrDefault(key);
        return string.IsNullOrWhiteSpace(advertised) ? fallback : advertised;
    }
}
