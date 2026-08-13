namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The lifecycle state of a repository indexing job. A job is created in
/// <see cref="Running"/> the moment onboarding is requested, runs asynchronously
/// off the request thread, and settles in exactly one terminal state
/// (<see cref="Completed"/> or <see cref="Failed"/>). The state is durable: it
/// survives a host restart so an interrupted run can be observed and resumed.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexStatus)]
public enum RepoIndexStatus
{
    /// <summary>No indexing job has ever been started for this repository.</summary>
    None = 0,

    /// <summary>
    /// The job is accepted and indexing is in progress (or is waiting to be
    /// resumed after a restart). Progress counters advance as the run proceeds.
    /// </summary>
    Running = 1,

    /// <summary>The most recent run finished and reconciled the whole tree.</summary>
    Completed = 2,

    /// <summary>
    /// The most recent run stopped on an error before completing. The
    /// <see cref="RepoIndexProgress.Error"/> field carries the reason and the
    /// durable structural writes that already landed are preserved, so re-running
    /// resumes from the first uncommitted chunk.
    /// </summary>
    Failed = 3,
}
