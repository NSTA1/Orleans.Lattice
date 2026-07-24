namespace Orleans.Lattice.Explorer.UI.Authentication;

/// <summary>
/// The shape of the explorer's "Sign out" control for the current head: whether
/// it renders as a server form post (web heads) or an in-circuit button (the
/// desktop head), and the server path a form post targets. Computed by
/// <see cref="ExplorerSignOut.Resolve"/>.
/// </summary>
/// <param name="UseServerFormPost">
/// When <see langword="true"/> the control renders an HTML form (with an
/// antiforgery token) that posts to <see cref="FormAction"/>, so the sign-out is
/// handled by a real server request. When <see langword="false"/> the control is
/// an in-circuit button that calls <c>IExplorerAuthSession.LogoutAsync</c>.
/// </param>
/// <param name="FormAction">
/// The server path the form posts to when <see cref="UseServerFormPost"/> is set.
/// Empty when the control is an in-circuit button.
/// </param>
public readonly record struct ExplorerSignOutTarget(bool UseServerFormPost, string FormAction);
