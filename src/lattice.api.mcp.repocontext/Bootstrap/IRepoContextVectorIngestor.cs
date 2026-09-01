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

    /// <summary>
    /// Embeds the repository's per-symbol records as their own passages so a
    /// symbol-level query lands on the declaring symbol. A symbol upserted this pass
    /// (in <paramref name="changedSymbolKeys"/>) is re-embedded so its passage
    /// reflects the new declaration; a symbol pruned this pass (in
    /// <paramref name="prunedSymbolKeys"/>) has its embedding retired; and any symbol
    /// with no live embedding yet - a new symbol, or one captured before symbol
    /// embedding existed - is back-filled, so a repository indexed earlier gains
    /// symbol passages without a re-walk. The default binding ignores the call.
    /// Retirement runs even when the embedding provider is unavailable (it only
    /// deletes stored records); embedding is skipped and returns zero when no
    /// provider is bound or it is unreachable.
    /// </summary>
    /// <param name="repoId">The repository identity the symbols belong to.</param>
    /// <param name="changedSymbolKeys">The canonical record keys of the symbols
    /// upserted this pass, whose embeddings should be refreshed.</param>
    /// <param name="prunedSymbolKeys">The canonical record keys of the symbols pruned
    /// this pass, whose embeddings should be retired.</param>
    /// <param name="cancellationToken">Cancels the ingest.</param>
    /// <returns>The number of symbols whose vectors were embedded and stored.</returns>
    Task<int> IngestSymbolsAsync(
        string repoId,
        IReadOnlyCollection<string> changedSymbolKeys,
        IReadOnlyCollection<string> prunedSymbolKeys,
        CancellationToken cancellationToken);

    /// <summary>
    /// Embeds the repository's durable agent-memory entries (decisions, gotchas,
    /// conventions, ...) as their own passages, so a natural-language
    /// <c>repocontext_search</c> ranks captured memory alongside code instead of
    /// silently omitting it.
    /// <para>
    /// <b>Why this exists (issue #1878).</b> Only files and symbols were ever
    /// embedded, so a healthy semantic index could not return a memory entry at
    /// all - memory was reachable only through the degraded BM25 keyword path.
    /// The effect was backwards from every agent's intuition: the better the
    /// index, the less findable the memory. Worse, a session that searched for an
    /// entry, found nothing, and concluded it had never been captured would then
    /// write it again - the observed cost was duplicate capture, not merely a
    /// failed lookup.
    /// </para>
    /// <para>
    /// An entry changed this pass is re-embedded; one retired this pass has its
    /// embedding retired; and any entry with no live embedding - including every
    /// entry captured before memory embedding existed - is back-filled, so an
    /// existing store converges without a re-walk. Retirement runs even when the
    /// provider is unavailable; embedding returns zero.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository identity the memory belongs to.</param>
    /// <param name="changedMemoryKeys">The canonical record keys of the entries
    /// written this pass, whose embeddings should be refreshed.</param>
    /// <param name="retiredMemoryKeys">The canonical record keys of the entries
    /// forgotten or expired this pass, whose embeddings should be retired.</param>
    /// <param name="cancellationToken">Cancels the ingest.</param>
    /// <returns>The number of memory entries whose vectors were embedded and stored.</returns>
    Task<int> IngestMemoryAsync(
        string repoId,
        IReadOnlyCollection<string> changedMemoryKeys,
        IReadOnlyCollection<string> retiredMemoryKeys,
        CancellationToken cancellationToken);
}
