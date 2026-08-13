namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The durable coordinator for a single repository's indexing job, keyed by
/// <c>repoId</c>. It owns the job's persisted progress state, starts the
/// asynchronous run off the request thread, and anchors an Orleans reminder that
/// resumes an interrupted run after a host restart. Callers observe a job through
/// <see cref="GetProgressAsync"/>; the background runner reports into it; and the
/// removal path clears it through <see cref="CancelAndClearAsync"/>.
/// </summary>
internal interface IRepoIndexJobGrain : IGrainWithStringKey
{
    /// <summary>
    /// Starts indexing the repository, or re-attaches to an already-running job.
    /// Returns immediately with the current progress; the run proceeds
    /// asynchronously. Idempotent while a run is in flight: a second call does not
    /// start a duplicate run.
    /// </summary>
    /// <param name="request">The durable job inputs (resolved path, id, filters).</param>
    /// <returns>The progress snapshot at acceptance (status running).</returns>
    Task<RepoIndexProgress> StartAsync(RepoIndexJobRequest request);

    /// <summary>
    /// Ensures the repository is (re-)indexed using its last persisted request,
    /// without any client call. Used by the always-on self-heal sweep to re-drive a
    /// bootstrap pass over a repository whose structural walk completed but whose
    /// embeddings were left incomplete, so the idempotent back-fill can close the
    /// gap. Does nothing and returns <see langword="false"/> when the repository was
    /// never bootstrapped (no persisted request) or a run is already in flight;
    /// otherwise it starts a run from the persisted request and returns
    /// <see langword="true"/>.
    /// </summary>
    /// <returns><see langword="true"/> if a run was triggered; otherwise <see langword="false"/>.</returns>
    Task<bool> EnsureIndexedAsync();

    /// <summary>Returns the current progress snapshot for the repository's job.</summary>
    /// <returns>The point-in-time progress, or an empty snapshot when no job has ever run.</returns>
    Task<RepoIndexProgress> GetProgressAsync();

    /// <summary>Merges a partial progress delta from the background runner into the durable state.</summary>
    /// <param name="update">The fields that changed.</param>
    Task ReportProgressAsync(RepoIndexProgressUpdate update);

    /// <summary>Marks the job completed, records the final counters and elapsed time, and clears the reminder.</summary>
    /// <param name="finalCounts">The final reconciliation counters.</param>
    /// <param name="elapsedMilliseconds">The run's wall-clock duration.</param>
    Task CompleteAsync(RepoIndexProgressUpdate finalCounts, long elapsedMilliseconds);

    /// <summary>Marks the job failed with a reason and clears the reminder so it does not retry on its own.</summary>
    /// <param name="error">The failure reason recorded on the snapshot.</param>
    Task FailAsync(string error);

    /// <summary>
    /// Cancels any in-flight run, unregisters the reminder, and clears the durable
    /// state. Called when a repository is removed so no orphaned reminder keeps
    /// firing for a repository that no longer exists.
    /// </summary>
    Task CancelAndClearAsync();
}
