using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.UI.Authentication;

/// <summary>
/// Resolves the shape of the explorer's "Sign out" control from the per-head
/// <see cref="ExplorerAuthUiOptions"/> and the optional
/// <see cref="ExplorerSignOutOptions"/>. The logic is factored out of the
/// <c>AuthButton</c> component so it can be verified in isolation: it is a pure
/// function of the two option objects.
/// </summary>
public static class ExplorerSignOut
{
    /// <summary>
    /// Returns the sign-out control's shape. A configured
    /// <see cref="ExplorerSignOutOptions.FederatedSignOutPath"/> wins: it forces a
    /// server form post to that federated endpoint, so a hosted-web head ends the
    /// browser session (cookie + identity-provider) rather than only dropping the
    /// local API credential. Otherwise the shape falls back to
    /// <paramref name="uiOptions"/>: a form post to
    /// <see cref="ExplorerAuthUiOptions.LogoutPath"/> when
    /// <see cref="ExplorerAuthUiOptions.UseServerFormPost"/> is set (the cookie web
    /// head), or an in-circuit button otherwise (the desktop head).
    /// </summary>
    /// <param name="uiOptions">The per-head login / logout UI options.</param>
    /// <param name="signOutOptions">
    /// The federated sign-out options, or <see langword="null"/> when none are
    /// registered (the UI degrades to its local-only sign-out).
    /// </param>
    /// <returns>The resolved sign-out control shape.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="uiOptions"/> is <see langword="null"/>.</exception>
    public static ExplorerSignOutTarget Resolve(ExplorerAuthUiOptions uiOptions, ExplorerSignOutOptions? signOutOptions)
    {
        ArgumentNullException.ThrowIfNull(uiOptions);

        if (signOutOptions?.FederatedSignOutPath is { Length: > 0 } federatedPath)
        {
            return new ExplorerSignOutTarget(UseServerFormPost: true, FormAction: federatedPath);
        }

        return new ExplorerSignOutTarget(
            UseServerFormPost: uiOptions.UseServerFormPost,
            FormAction: uiOptions.UseServerFormPost ? uiOptions.LogoutPath : string.Empty);
    }
}
