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
    public ValueTask IngestAsync(
        string repoId,
        string repoRoot,
        IReadOnlyList<RepoFileEntry> changedFiles,
        CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
