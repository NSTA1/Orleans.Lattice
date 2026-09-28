using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// What the sign-in dialog offers for one endpoint: every advertised scheme a
/// registered auth method can service, in the endpoint's preference order, or
/// the schemes the endpoint asked for when none of them can be serviced.
/// </summary>
/// <remarks>
/// Matching goes through Core's seam, <see cref="IExplorerAuthMethod.CanHandle"/>,
/// never through a list of known scheme names, so a custom method - including the
/// OIDC methods of #1802 - that accepts aliases or a family of scheme names is
/// offered exactly as Basic and Entra are.
/// </remarks>
internal sealed class SessionSignInChoice
{
    private SessionSignInChoice(IReadOnlyList<SessionSignInMethod> methods, IReadOnlyList<string> unsupportedSchemes)
    {
        Methods = methods;
        UnsupportedSchemes = unsupportedSchemes;
    }

    /// <summary>The methods to offer, in the endpoint's preference order. Empty when <see cref="IsUnsupported"/>.</summary>
    public IReadOnlyList<SessionSignInMethod> Methods { get; }

    /// <summary>The advertised schemes, when no registered method can service any of them.</summary>
    public IReadOnlyList<string> UnsupportedSchemes { get; }

    /// <summary>Whether the endpoint asked only for schemes no registered method services.</summary>
    public bool IsUnsupported => Methods.Count == 0;

    /// <summary>
    /// Resolves the choice for an endpoint's advertisement. An endpoint that
    /// advertised nothing (an older server, or one the probe could not reach) is
    /// offered every method that accepts an empty advertisement - Core's Basic
    /// method does - so the username and password flow is always the fallback.
    /// </summary>
    /// <param name="advertisement">What the endpoint advertised.</param>
    /// <param name="methods">Every registered auth method, in registration order.</param>
    /// <returns>The methods to offer.</returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public static SessionSignInChoice Resolve(ExplorerAuthSchemeAdvertisement advertisement, IEnumerable<IExplorerAuthMethod> methods)
    {
        ArgumentNullException.ThrowIfNull(advertisement);
        ArgumentNullException.ThrowIfNull(methods);

        var registered = methods.ToArray();
        var offered = new List<SessionSignInMethod>();
        var used = new HashSet<IExplorerAuthMethod>(ReferenceEqualityComparer.Instance);

        if (!advertisement.HasSchemes)
        {
            foreach (var method in registered.Where(method => method.CanHandle(string.Empty)))
            {
                if (used.Add(method))
                {
                    offered.Add(new SessionSignInMethod(method.SchemeId, DisplayNameFor(method.SchemeId, displayName: null), IsBasic(method)));
                }
            }

            if (offered.Count == 0)
            {
                offered.Add(new SessionSignInMethod(ExplorerAuthSchemes.Basic, DisplayNameFor(ExplorerAuthSchemes.Basic, displayName: null), UsesPassword: true));
            }

            return new SessionSignInChoice(offered, []);
        }

        foreach (var scheme in advertisement.Schemes)
        {
            var method = registered.FirstOrDefault(candidate => candidate.CanHandle(scheme.SchemeId));
            if (method is not null && used.Add(method))
            {
                offered.Add(new SessionSignInMethod(scheme.SchemeId, DisplayNameFor(scheme.SchemeId, scheme.DisplayName), IsBasic(method)));
            }
        }

        return offered.Count > 0
            ? new SessionSignInChoice(offered, [])
            : new SessionSignInChoice([], advertisement.Schemes.Select(scheme => scheme.SchemeId).ToArray());
    }

    private static bool IsBasic(IExplorerAuthMethod method) =>
        string.Equals(method.SchemeId, ExplorerAuthSchemes.Basic, StringComparison.OrdinalIgnoreCase);

    private static string DisplayNameFor(string schemeId, string? displayName)
    {
        if (!string.IsNullOrWhiteSpace(displayName))
        {
            return displayName;
        }

        return string.Equals(schemeId, ExplorerAuthSchemes.Basic, StringComparison.OrdinalIgnoreCase)
            ? "Username and password"
            : schemeId;
    }
}
