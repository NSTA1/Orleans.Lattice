namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Configures how a UI head drives graceful re-authentication when the current
/// sign-in latches into its revoked state (see
/// <see cref="Orleans.Lattice.Explorer.Core.Connection.IReauthRequiredSource"/>).
/// It carries the head-relative path of a forced-interactive challenge endpoint
/// so the core explorer can send the browser through a fresh interactive sign-in
/// without taking a compile-time dependency on any particular sign-in provider
/// package (for example the hosted-web Entra provider, which maps the endpoint).
/// </summary>
/// <remarks>
/// Registered as a singleton by the auth registration; a provider package that
/// maps a challenge endpoint (such as
/// <c>MapLatticeExplorerEntraWebReauth</c>) overrides the registration to point
/// <see cref="ChallengePath"/> at that endpoint. When <see cref="ChallengePath"/>
/// is <see langword="null"/> (the default) the UI degrades to a plain full-page
/// reload, which is still preferable to a stuck circuit but cannot force a fresh
/// authorization-code redemption.
/// </remarks>
public sealed class ExplorerReauthOptions
{
    /// <summary>The default name of the query-string parameter carrying the local return URL.</summary>
    public const string DefaultReturnUrlParameter = "returnUrl";

    /// <summary>
    /// The head-relative path of the forced-interactive challenge endpoint the UI
    /// navigates to (with <c>forceLoad</c>) when re-authentication is required, or
    /// <see langword="null"/> (the default) when no challenge endpoint is wired,
    /// in which case the UI performs a plain full-page reload instead.
    /// </summary>
    public string? ChallengePath { get; set; }

    /// <summary>
    /// When <see langword="true"/> (the default) the UI appends the current local
    /// page path to the challenge URL as a return URL so the operator lands back
    /// where they were after re-authenticating. The challenge endpoint is
    /// responsible for validating the return URL as a local path.
    /// </summary>
    public bool AppendReturnUrl { get; set; } = true;

    /// <summary>
    /// The query-string parameter name used to carry the return URL. Defaults to
    /// <see cref="DefaultReturnUrlParameter"/> and must match the parameter the
    /// challenge endpoint reads.
    /// </summary>
    public string ReturnUrlParameter { get; set; } = DefaultReturnUrlParameter;
}
