namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Resolves the optional vector payload for a key at snapshot-export time. The
/// portability primitive calls this once per enumerated record so a host can
/// attach the (opaque) vector and embedding-space tag that live outside the
/// enumerated tree, keeping the vector storage a pluggable, package-independent
/// concern. Return <see langword="null"/> when the key carries no vector.
/// </summary>
/// <param name="key">The store key currently being exported.</param>
/// <param name="cancellationToken">Cancels the lookup.</param>
/// <returns>The vector payload for the key, or <see langword="null"/> when there is none.</returns>
internal delegate ValueTask<RepoContextVectorPayload?> RepoContextVectorExport(
    string key,
    CancellationToken cancellationToken);
