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
    /// on an already-armed coordinator does not create duplicate work; calling it on
    /// a converged coordinator may still run one confirming step in that activation
    /// before the coordinator stands down, so the startup sweep can call it safely.
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

    /// <summary>
    /// Disarms the coordinator: clears the persisted build intent, stops the phase
    /// pump, unregisters the keep-alive reminder, and deactivates. Idempotent - a
    /// coordinator that was never armed, or was already stopped, completes without
    /// doing anything.
    /// <para>
    /// This is the counterpart to <see cref="EnsureBuildingAsync"/> and exists
    /// because the keep-alive reminder is <b>durable</b> while the switch that
    /// arms it is not. A deployment that ran with approximate-index scheduling on
    /// and later turned it off still carries the registered reminder, so the
    /// coordinator keeps reactivating after every restart and rebuilding an index
    /// nobody asked for. <see cref="EnsureBuildingAsync"/> deliberately cannot fix
    /// that: it returns early when the switch is off, which is exactly the
    /// configuration in which the orphaned reminder exists. The reminder is only
    /// retired opportunistically, the next time it happens to fire and the base
    /// class finds no work outstanding - and that firing is itself an activation
    /// that re-opens the index into the process. Tearing a repository down must
    /// therefore be able to say "stop", not merely "do not start".
    /// </para>
    /// <para>
    /// It is <b>not</b> gated on the scheduling switch, for the same reason. A stop
    /// verb that refused to run whenever scheduling was disabled would be unable to
    /// clean up in the only state that needs cleaning up.
    /// </para>
    /// </summary>
    Task StopAsync();
}
