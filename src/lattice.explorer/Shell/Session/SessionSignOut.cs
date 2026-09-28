using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// Decides how the identity menu signs out, as a pure function of the sign-in
/// options and Core's <see cref="ExplorerSignOutOptions"/>, so the decision is
/// verified without rendering anything.
/// </summary>
internal static class SessionSignOut
{
    /// <summary>
    /// Returns the sign-out control's shape. A configured
    /// <see cref="ExplorerSignOutOptions.FederatedSignOutPath"/> wins and forces a
    /// form post to it, so a hosted-web head ends the browser's identity-provider
    /// session and not only the API credential. Otherwise a head that posts its
    /// password form also posts its sign-out to
    /// <see cref="SessionSignInOptions.LogoutPath"/>, and any other head signs out
    /// inside the circuit.
    /// </summary>
    /// <param name="options">The session chrome's sign-in options.</param>
    /// <param name="signOutOptions">Core's federated sign-out options, or <see langword="null"/> when none are registered.</param>
    /// <returns>The resolved control shape.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public static SessionSignOutTarget Resolve(SessionSignInOptions options, ExplorerSignOutOptions? signOutOptions)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (signOutOptions?.FederatedSignOutPath is { Length: > 0 } federatedPath)
        {
            return new SessionSignOutTarget(UseServerFormPost: true, FormAction: federatedPath);
        }

        return new SessionSignOutTarget(
            UseServerFormPost: options.UseServerFormPost,
            FormAction: options.UseServerFormPost ? options.LogoutPath : string.Empty);
    }
}
