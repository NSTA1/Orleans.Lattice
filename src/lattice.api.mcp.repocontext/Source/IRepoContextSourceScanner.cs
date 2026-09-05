namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Lets a source strategy supply the scan set for a bootstrap run instead of the
/// filesystem walk. A git-sourced run enumerates the resolved commit's tree, so the
/// reconcile's add / modify / delete changeset is computed against the commit rather
/// than inferred from absence on disk and modification times.
/// <para>
/// Returning <see langword="null"/> means "no pre-computed scan", which is what the
/// mounted-workspace default does and is why that path is byte-for-byte unchanged.
/// </para>
/// </summary>
internal interface IRepoContextSourceScanner
{
    /// <summary>
    /// The scan set for <paramref name="request"/>, or <see langword="null"/> to let
    /// the caller walk the directory tree.
    /// </summary>
    /// <param name="request">The bootstrap request being run. Must not be
    /// <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The pre-computed scan set, or <see langword="null"/>.</returns>
    IReadOnlyList<RepoFileEntry>? TryScan(
        RepoContextBootstrapRequest request,
        CancellationToken cancellationToken);
}
