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
    /// path, together with the files it left unchanged, so an interrupted prior
    /// vectorise can be self-healed. The default binding ignores them (vector
    /// persistence is out of scope for structural ingestion); a later binding
    /// embeds the changed files and back-fills any unchanged file that has no
    /// embedding yet.
    /// </summary>
    /// <param name="repoId">The repository identity the files belong to.</param>
    /// <param name="repoRoot">The absolute repository root, so an implementation
    /// can re-read file content to embed it.</param>
    /// <param name="changedFiles">The files added or updated by the run. These are
    /// always embedded (their content changed, so any prior vector is stale).</param>
    /// <param name="unchangedFiles">The files the run left unchanged. A binding
    /// that embeds only re-embeds one of these when it has no embedding yet, which
    /// heals a vectorise that a prior run left incomplete without redundant work.</param>
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
        IReadOnlyList<RepoFileEntry> unchangedFiles,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken);

    /// <summary>
    /// Retires the vectors of the files a bootstrap run removed, so a deleted file
    /// naturally drops its embedding and the live vector count stays honest. The
    /// default binding ignores them; the embedding binding deletes each source's
    /// vector presence keys and observed-removes it from the membership set. This
    /// is independent of the embedding provider - it only deletes stored records -
    /// so retirement still happens when the provider is unavailable.
    /// </summary>
    /// <param name="repoId">The repository identity the files belonged to.</param>
    /// <param name="removedPaths">The repository-relative paths of the files the
    /// run removed.</param>
    /// <param name="cancellationToken">Cancels the retirement.</param>
    Task RetireAsync(
        string repoId,
        IReadOnlyList<string> removedPaths,
        CancellationToken cancellationToken);
}
