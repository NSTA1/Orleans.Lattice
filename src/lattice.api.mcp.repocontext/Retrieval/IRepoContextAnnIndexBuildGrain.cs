namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The durable scheduler for one <c>(repository, embedding space)</c>
/// approximate-index build. Its grain key is
/// <see cref="RepoContextAnnIndexKeys.BuildGrainKey"/> - the repository followed
/// by the embedding-space fingerprint - so exactly one coordinator exists per pair
/// across the cluster and Orleans' single-threaded activation is what keeps two
/// builds off the same index, in place of an in-process dedupe flag that a process
/// death forgets.
/// <para>
/// The build used to be armed by a declining query through a fire-and-forget
/// <c>Task.Run</c>. That made the work which makes queries fast reachable only
/// from a query: it died with the process and nothing resumed it, the first query
/// after a restart both paid the un-indexed cost and was the trigger, and a
/// repository nobody queried never indexed itself at all. This grain replaces that
/// with the reminder-anchored coordinator pattern the tree coordinators already
/// use, so the build is crash-safe, survives a silo restart, and starts with no
/// traffic whatsoever.
/// </para>
/// </summary>
internal interface IRepoContextAnnIndexBuildGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records the embedding space to build for and arms the coordinator: registers
    /// the keep-alive reminder and starts the phase pump. Idempotent - calling it
    /// on an already-armed or already-converged coordinator does no extra work, so
    /// the startup sweep can call it for every registered repository on every
    /// start.
    /// </summary>
    /// <param name="space">The embedding space the index must cover.</param>
    Task EnsureBuildingAsync(EmbeddingSpaceTag space);

    /// <summary>
    /// Whether the persisted intent says the index reached <c>Ready</c>. This is
    /// the deterministic state assertion the crash-resume and cold-convergence
    /// coverage reads, so neither has to issue a query to learn whether the build
    /// finished - which is the whole property this grain exists to provide.
    /// </summary>
    /// <returns><see langword="true"/> once the index has converged.</returns>
    Task<bool> IsConvergedAsync();
}
