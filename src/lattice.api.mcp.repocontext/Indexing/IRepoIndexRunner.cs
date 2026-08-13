namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Runs repository indexing off the request thread. The job grain hands a request
/// to the runner, which executes the bootstrap pass on a background task bound to
/// the host's lifetime (not to any client request), reports progress back into the
/// grain, and settles the grain in a terminal state. A client disconnect therefore
/// never aborts an index; only a host shutdown does, and the grain's reminder
/// resumes it on the next start.
/// </summary>
internal interface IRepoIndexRunner
{
    /// <summary>
    /// Starts (or resumes) the indexing run for a repository. Single-flight per
    /// repository id: a call while a run for the same id is already in flight is a
    /// no-op, so a duplicate start or an overlapping reminder tick cannot launch a
    /// second concurrent run.
    /// </summary>
    /// <param name="request">The durable job inputs.</param>
    void Enqueue(RepoIndexJobRequest request);

    /// <summary>
    /// Starts (or re-attaches to) the indexing job for a repository and returns its
    /// current progress. This is the request-thread entry point: it resolves the
    /// job grain through the runner's root-scoped grain factory - safe to call from
    /// any thread - so an MCP tool never touches an activation-scoped factory.
    /// </summary>
    /// <param name="request">The durable job inputs.</param>
    /// <returns>The job's progress snapshot after the start is recorded.</returns>
    Task<RepoIndexProgress> StartIndexAsync(RepoIndexJobRequest request);

    /// <summary>
    /// Reads the current progress of a repository's indexing job through the
    /// runner's root-scoped grain factory. Safe to call from any thread.
    /// </summary>
    /// <param name="repoId">The repository whose job progress to read.</param>
    /// <returns>The job's current progress snapshot.</returns>
    Task<RepoIndexProgress> GetProgressAsync(string repoId);

    /// <summary>
    /// Cancels an in-flight run for a repository, if any. The cancelled run stops
    /// promptly and does not settle the grain (the caller owns the terminal state).
    /// </summary>
    /// <param name="repoId">The repository whose run to cancel.</param>
    /// <returns><see langword="true"/> when a run was in flight and was signalled to cancel.</returns>
    bool Cancel(string repoId);

    /// <summary>
    /// Cancels an in-flight run for a repository and awaits its full termination,
    /// so that when the returned task completes no structural write from the run is
    /// still in flight. Callers that are about to delete a repository's records use
    /// this to drain the indexer to a halt first, so a range-delete never races a
    /// concurrent index write (which would surface as an Orleans state version
    /// conflict on a shared leaf). Returns immediately when no run is in flight.
    /// </summary>
    /// <param name="repoId">The repository whose run to cancel and drain.</param>
    Task CancelAndWaitAsync(string repoId);
}
