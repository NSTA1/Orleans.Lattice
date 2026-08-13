namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Applies the optional vector payload carried by a snapshot record at
/// import time. The portability primitive calls this once per imported record
/// that carries a vector, so a host can re-seed the (opaque) vector and
/// embedding-space tag into whatever store owns them, keeping vector storage a
/// pluggable, package-independent concern. Implementations should be idempotent
/// so a re-import converges rather than duplicating.
/// </summary>
/// <param name="key">The store key the vector belongs to.</param>
/// <param name="payload">The opaque vector payload to apply.</param>
/// <param name="cancellationToken">Cancels the apply.</param>
internal delegate ValueTask RepoContextVectorImport(
    string key,
    RepoContextVectorPayload payload,
    CancellationToken cancellationToken);
