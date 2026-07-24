namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// Builds the URL a UI head navigates to (with a full-page load) when a sign-in
/// latches into its revoked state and graceful re-authentication is required.
/// The logic is factored out of the interstitial component so it can be verified
/// in isolation: it is a pure function of the configured
/// <see cref="ExplorerReauthOptions"/> and the current page's local path.
/// </summary>
public static class ExplorerReauthChallenge
{
    /// <summary>
    /// Returns the URL to navigate to for re-authentication. When
    /// <paramref name="options"/> is <see langword="null"/> or carries no
    /// <see cref="ExplorerReauthOptions.ChallengePath"/>, the safe fallback is a
    /// plain reload of <paramref name="currentLocalPath"/> (no challenge endpoint
    /// is wired). Otherwise the challenge path is returned, with the current local
    /// path appended as a URL-encoded return-URL query parameter when
    /// <see cref="ExplorerReauthOptions.AppendReturnUrl"/> is set.
    /// </summary>
    /// <param name="options">The re-authentication options, or <see langword="null"/>.</param>
    /// <param name="currentLocalPath">
    /// The current page's local path-and-query (for example <c>/state/tree?x=1</c>),
    /// used as the fallback reload target and the return URL.
    /// </param>
    /// <returns>The URL to navigate to.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="currentLocalPath"/> is <see langword="null"/>.</exception>
    public static string BuildUrl(ExplorerReauthOptions? options, string currentLocalPath)
    {
        ArgumentNullException.ThrowIfNull(currentLocalPath);

        // No challenge endpoint wired (no sign-in provider published one): a plain
        // reload of the current page is the safe fallback.
        if (options?.ChallengePath is not { Length: > 0 } challengePath)
        {
            return currentLocalPath;
        }

        if (!options.AppendReturnUrl)
        {
            return challengePath;
        }

        var separator = challengePath.Contains('?', StringComparison.Ordinal) ? '&' : '?';
        var parameter = string.IsNullOrEmpty(options.ReturnUrlParameter)
            ? ExplorerReauthOptions.DefaultReturnUrlParameter
            : options.ReturnUrlParameter;
        return $"{challengePath}{separator}{parameter}={Uri.EscapeDataString(currentLocalPath)}";
    }
}
