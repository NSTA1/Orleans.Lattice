namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The grain-storage operation a SQLite lock failure is attributed to by
/// <see cref="RepoContextLockAttributingGrainStorage"/>.
/// </summary>
public enum RepoContextGrainStorageOperation
{
    /// <summary>A <c>ReadStateAsync</c> call. Reads do not join the write convoy.</summary>
    Read,

    /// <summary>A <c>WriteStateAsync</c> call, which needs the database write lock.</summary>
    Write,

    /// <summary>
    /// A <c>ClearStateAsync</c> call, which deletes the grain row and so needs the
    /// database write lock exactly as a write does.
    /// </summary>
    Clear,
}
