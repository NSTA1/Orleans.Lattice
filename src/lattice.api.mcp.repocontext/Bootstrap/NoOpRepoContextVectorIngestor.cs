namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The default <see cref="IRepoContextVectorIngestor"/>: a pass-through no-op.
/// Structural ingestion does not persist vectors - the vector record shape and
/// the vector write / retrieval path are owned by the retrieval work - so the
/// shipped binding accepts the changed files and does nothing, letting bootstrap
/// deliver a fully structural baseline without racing the vector surface. A host
/// that wires the retrieval surface replaces this registration with a real
/// embedding-and-store implementation.
/// </summary>
internal sealed class NoOpRepoContextVectorIngestor : IRepoContextVectorIngestor
{
    /// <inheritdoc />
    public ValueTask<int> IngestAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> changedFiles,
        IReadOnlyList<RepoFileEntry> unchangedFiles,
        Func<int, CancellationToken, ValueTask>? onProgress,
        CancellationToken cancellationToken) => ValueTask.FromResult(0);

    /// <inheritdoc />
    public Task RetireAsync(
        string repoId,
        IReadOnlyList<string> removedPaths,
        CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task<int> IngestSymbolsAsync(
        string repoId,
        IReadOnlyCollection<string> changedSymbolKeys,
        IReadOnlyCollection<string> prunedSymbolKeys,
        CancellationToken cancellationToken) => Task.FromResult(0);
}
