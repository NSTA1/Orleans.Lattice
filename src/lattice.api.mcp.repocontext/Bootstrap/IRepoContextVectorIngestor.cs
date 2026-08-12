namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The clean, injected seam through which a bootstrap run hands its changed files
/// to the (later) vectorisation path. Structural ingestion is complete on its own
/// - walker, digest, idempotent diff, prune, and the mutating fail-closed tool -
/// and the vector <b>record shape</b> and the vector write / retrieval path are
/// owned by separate work. So this seam exists only to keep that boundary clean:
/// bootstrap always calls it for the files it added or updated, and the default
/// binding is a no-op.
/// <para>
/// When the retrieval surface lands, its implementation of this interface embeds
/// each changed file and persists the vectors onto the reserved vector trees,
/// with no change to the bootstrap coordinator or the tool.
/// </para>
/// </summary>
internal interface IRepoContextVectorIngestor
{
    /// <summary>
    /// Offers the files a bootstrap run added or updated to the vectorisation
    /// path. The default binding ignores them (vector persistence is out of scope
    /// for structural ingestion); a later binding embeds and stores them.
    /// </summary>
    /// <param name="repoId">The repository identity the files belong to.</param>
    /// <param name="repoRoot">The absolute repository root, so an implementation
    /// can re-read file content to embed it.</param>
    /// <param name="changedFiles">The files added or updated by the run.</param>
    /// <param name="onProgress">An optional callback invoked after each batch of
    /// vectors is stored, with the running count of files embedded so far, so a long
    /// vectorisation pass can report incremental progress. May be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the ingest.</param>
    /// <returns>The number of files whose vectors were embedded and stored. The
    /// no-op binding returns zero, and a real binding returns fewer than the
    /// offered count when it fails closed or skips contentless files.</returns>
    ValueTask<int> IngestAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> changedFiles,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken);
}
