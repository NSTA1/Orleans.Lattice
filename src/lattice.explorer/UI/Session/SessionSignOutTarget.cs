namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// The shape of the identity menu's "Sign out" control: a server form post to
/// <paramref name="FormAction"/>, or an in-circuit button. Computed by
/// <see cref="SessionSignOut.Resolve"/>.
/// </summary>
/// <param name="UseServerFormPost">
/// <see langword="true"/> when the control is a form (with an antiforgery token)
/// that posts to <paramref name="FormAction"/>; <see langword="false"/> when it
/// calls <c>IExplorerAuthSession.LogoutAsync</c> inside the circuit.
/// </param>
/// <param name="FormAction">The path the form posts to, or empty for the in-circuit button.</param>
internal readonly record struct SessionSignOutTarget(bool UseServerFormPost, string FormAction);
