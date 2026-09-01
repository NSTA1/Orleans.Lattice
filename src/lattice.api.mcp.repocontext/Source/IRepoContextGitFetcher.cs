namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The git transport seam: fetches a configured ref into a staging work tree and
/// resolves the commit that anchors the next index generation. Substituted in tests
/// to simulate an unreachable remote or a partial fetch without a network.
/// <para>
/// The contract is synchronous because the shipped transport is; the caller offloads
/// it and bounds it with a timeout so a hung remote cannot wedge the per-repository
/// singleton grain.
/// </para>
/// </summary>
internal interface IRepoContextGitFetcher
{
    /// <summary>
    /// Fetches and, when the ref moved, checks out the staging work tree.
    /// </summary>
    /// <param name="request">The fetch inputs. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the fetch between its steps.</param>
    /// <returns>The resolved commit and change summary.</returns>
    /// <exception cref="RepoContextGitSourceException">The remote was unreachable,
    /// the credential was rejected, the ref did not resolve, or the checkout failed.
    /// The message is already secret-redacted.</exception>
    RepoContextGitFetchResult Fetch(RepoContextGitFetchRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Enumerates the files of a commit as scan entries, reading blob content from
    /// the object database rather than the working tree, so the indexed set is
    /// exactly the commit's tree.
    /// </summary>
    /// <param name="workTreePath">The staging work tree holding the object database.
    /// Must not be <see langword="null"/>.</param>
    /// <param name="commitSha">The commit whose tree to enumerate. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="includeGlobs">Optional include globs; when non-empty a file is
    /// kept only if it matches at least one.</param>
    /// <param name="excludeGlobs">Optional exclude globs; a match always removes the
    /// file.</param>
    /// <param name="excludeBinary">Whether to drop blobs whose leading bytes look
    /// non-text.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>One entry per indexed file in the commit tree.</returns>
    /// <exception cref="RepoContextGitSourceException">The commit or its tree could
    /// not be read.</exception>
    IReadOnlyList<RepoFileEntry> ScanCommit(
        string workTreePath,
        string commitSha,
        IReadOnlyList<string>? includeGlobs,
        IReadOnlyList<string>? excludeGlobs,
        bool excludeBinary,
        CancellationToken cancellationToken);
}
