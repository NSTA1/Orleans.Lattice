namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Normalises the configured ref into the two forms the transport needs: the
/// fully-qualified remote ref to fetch, and the local remote-tracking ref the fetch
/// writes so the resolved commit can be looked up afterwards.
/// </summary>
internal static class RepoContextGitReference
{
    /// <summary>The ref tracked when none is configured.</summary>
    internal const string DefaultReference = "refs/heads/main";

    private const string HeadsPrefix = "refs/heads/";
    private const string TagsPrefix = "refs/tags/";
    private const string RemotesPrefix = "refs/remotes/origin/";

    /// <summary>
    /// Expands <paramref name="reference"/> to a fully-qualified ref. A bare name is
    /// treated as a branch, which is what an operator writing <c>main</c> means.
    /// </summary>
    /// <param name="reference">The configured ref or bare branch name. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>A fully-qualified ref such as <c>refs/heads/main</c>.</returns>
    internal static string Qualify(string reference)
    {
        ArgumentNullException.ThrowIfNull(reference);
        var trimmed = reference.Trim();
        if (trimmed.Length == 0)
        {
            return DefaultReference;
        }

        return trimmed.StartsWith("refs/", StringComparison.Ordinal)
            ? trimmed
            : HeadsPrefix + trimmed;
    }

    /// <summary>
    /// The local ref the fetch refspec writes the resolved tip into. A branch is
    /// mirrored under <c>refs/remotes/origin/</c>; a tag or any other fully-qualified
    /// ref is mirrored under its own name.
    /// </summary>
    /// <param name="reference">The configured ref or bare branch name. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The local ref name to look the resolved commit up by.</returns>
    internal static string LocalTrackingRef(string reference)
    {
        var qualified = Qualify(reference);
        return qualified.StartsWith(HeadsPrefix, StringComparison.Ordinal)
            ? RemotesPrefix + qualified[HeadsPrefix.Length..]
            : qualified;
    }

    /// <summary>
    /// The forced refspec that fetches <paramref name="reference"/> into its local
    /// tracking ref. Forced so a rewritten branch (a force-push upstream) still
    /// converges instead of wedging the hub on a non-fast-forward rejection.
    /// </summary>
    /// <param name="reference">The configured ref or bare branch name. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>A refspec of the form <c>+source:destination</c>.</returns>
    internal static string RefSpec(string reference) =>
        "+" + Qualify(reference) + ":" + LocalTrackingRef(reference);

    /// <summary>
    /// Whether <paramref name="reference"/> names a tag, which is fetched into its
    /// own namespace rather than a remote-tracking branch.
    /// </summary>
    /// <param name="reference">The configured ref or bare branch name. Must not be
    /// <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the ref is under <c>refs/tags/</c>.</returns>
    internal static bool IsTag(string reference) =>
        Qualify(reference).StartsWith(TagsPrefix, StringComparison.Ordinal);
}
