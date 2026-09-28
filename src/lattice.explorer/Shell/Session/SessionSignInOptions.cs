namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// How the session chrome submits a username and password, and where its local
/// sign-out posts: the successor to the old UI's <c>ExplorerAuthUiOptions</c>.
/// </summary>
/// <remarks>
/// <para>
/// Blazor Server is the only head (epic #3807, E12), so the default is a native
/// form post to the web head's server endpoints: the password never crosses the
/// SignalR circuit and rests only in the head's encrypted, <c>HttpOnly</c>
/// cookie. <see cref="UseServerFormPost"/> set to <see langword="false"/> signs in
/// inside the circuit through <c>IExplorerAuthSession.LoginAsync</c> instead,
/// for a host that maps no such endpoints.
/// </para>
/// <para>
/// The default paths are relative, so they resolve against the document's
/// <c>&lt;base href&gt;</c> and work unchanged under a path base. A head that
/// maps the endpoints elsewhere registers its own instance before the Shell's
/// registration, which adds this default with <c>TryAdd</c>. The instance is
/// immutable once built and carries no per-circuit state, which is what makes a
/// singleton registration safe.
/// </para>
/// </remarks>
internal sealed class SessionSignInOptions
{
    /// <summary>The default login path, relative to the document base.</summary>
    public const string DefaultLoginPath = "auth/login";

    /// <summary>The default local sign-out path, relative to the document base.</summary>
    public const string DefaultLogoutPath = "auth/logout";

    /// <summary>
    /// Whether the password form posts to <see cref="LoginPath"/> (the default) or
    /// signs in inside the circuit.
    /// </summary>
    public bool UseServerFormPost { get; init; } = true;

    /// <summary>The path the password form posts to when <see cref="UseServerFormPost"/> is set.</summary>
    public string LoginPath { get; init; } = DefaultLoginPath;

    /// <summary>The path a local sign-out posts to when <see cref="UseServerFormPost"/> is set.</summary>
    public string LogoutPath { get; init; } = DefaultLogoutPath;
}
