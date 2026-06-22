namespace Orleans.Lattice.Explorer.UI.Authentication;

/// <summary>
/// Per-head options controlling how the shared login / logout UI submits
/// credentials. The desktop head signs in fully in-process through
/// <c>IExplorerAuthSession</c>; the web head instead posts to a server endpoint
/// so the password never crosses the SignalR circuit and is stored in an
/// encrypted, <c>HttpOnly</c> server cookie. Each head registers an instance in
/// DI; when none is registered the UI defaults to the in-process desktop flow.
/// </summary>
public sealed class ExplorerAuthUiOptions
{
    /// <summary>
    /// When <see langword="true"/>, the login dialog renders a native HTML form
    /// that POSTs to <see cref="LoginPath"/> (the web head). When
    /// <see langword="false"/> (the default), the dialog signs in in-process via
    /// <c>IExplorerAuthSession.LoginAsync</c> (the desktop head).
    /// </summary>
    public bool UseServerFormPost { get; set; }

    /// <summary>The server path the login form posts to when <see cref="UseServerFormPost"/> is set.</summary>
    public string LoginPath { get; set; } = "/auth/login";

    /// <summary>The server path the logout form posts to when <see cref="UseServerFormPost"/> is set.</summary>
    public string LogoutPath { get; set; } = "/auth/logout";
}
