using Microsoft.Extensions.Options;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// The hosted-web Entra ID <see cref="IExplorerAuthMethod"/> for the <c>entra</c>
/// scheme. Where the desktop provider
/// (<c>Orleans.Lattice.Explorer.Entra</c>) runs an interactive browser flow on
/// the machine hosting the UI, this provider serves a remote Blazor Server
/// circuit: the browser has already signed in through the ASP.NET OpenID Connect
/// middleware, so the challenge simply exchanges that session for a downstream
/// State API token via <see cref="IExplorerWebTokenAcquirer"/> and wires silent
/// renewal into an <see cref="ExplorerAccessTokenSource"/>.
/// </summary>
/// <remarks>
/// Registered <em>scoped</em> (per circuit), because the acquirer it depends on
/// reads the per-circuit authenticated user. Its scopes come from the endpoint's
/// advertised audience, falling back to statically configured
/// <see cref="ExplorerEntraWebOptions.Scopes"/>.
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

        var scopes = ResolveScopes(context.Parameters, _options.CurrentValue);
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
        ExplorerEntraWebOptions options)
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

        // An audience is a resource identifier that maps to the resource's default
        // scope; a value that already names a scope (ends with "/.default") is used
        // verbatim.
        var scope = audience.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? audience
            : $"{audience}/.default";
        return new[] { scope };
    }
}
