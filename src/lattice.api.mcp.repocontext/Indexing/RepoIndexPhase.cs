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

    /// <summary>
    /// Tearing the code index down rather than building it up: a
    /// <c>repocontext_reset_index</c> sweep is dropping the repository's
    /// structural, symbol, content, cross-reference, session, and vector trees.
    /// This phase is what lets one status verb answer "is this repository being
    /// built up or torn down right now" - a job in <see cref="Resetting"/> is a
    /// reset in flight, distinct from every build phase above, so a caller that
    /// loses the reset's response can still poll <c>index_status</c> and see the
    /// teardown running (rather than the pre-2642 silence, where a reset left no
    /// pollable signal at all and a still-working reset was indistinguishable
    /// from one that had died).
    /// </summary>
    Resetting = 6,
}
