namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The phase a repository indexing run is currently executing. Phases advance
/// monotonically within a single run and let an observer see where a long
/// onboarding pass has reached without waiting for it to finish.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexPhase)]
public enum RepoIndexPhase
{
    /// <summary>The job has not begun executing (freshly queued or never run).</summary>
    Pending = 0,

    /// <summary>Walking the working tree and hashing file content.</summary>
    Walking = 1,

    /// <summary>Reading the already-stored digests to reconcile the scan against them.</summary>
    Reconciling = 2,

    /// <summary>Committing the structural difference in atomic chunks.</summary>
    Applying = 3,

    /// <summary>Embedding the changed files and storing their vectors.</summary>
    Vectorising = 4,

    /// <summary>The run finished; no phase is executing.</summary>
    Done = 5,
}
