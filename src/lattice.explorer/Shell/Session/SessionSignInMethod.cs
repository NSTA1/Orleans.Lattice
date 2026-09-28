namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// One way the sign-in dialog offers to sign in: an advertised scheme paired
/// with the registered auth method that services it.
/// </summary>
/// <param name="SchemeId">
/// The scheme to sign in with, as the endpoint advertised it (or the method's own
/// id when the endpoint advertised nothing). It is what the dialog hands to
/// <c>IExplorerAuthSession.LoginWithMethodAsync</c>, so the advertised parameters
/// for that scheme reach the method's challenge.
/// </param>
/// <param name="DisplayName">The name shown for the method: the advertised display name, or the scheme id.</param>
/// <param name="UsesPassword">
/// Whether the method is Core's Basic method, whose challenge reads a username
/// and password (<c>ExplorerAuthSchemes.UsernameInput</c> and
/// <c>PasswordInput</c>). Every other method runs its own interactive challenge
/// and takes no input from the dialog.
/// </param>
internal sealed record SessionSignInMethod(string SchemeId, string DisplayName, bool UsesPassword);
