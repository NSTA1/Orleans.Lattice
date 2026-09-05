namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The inputs to one staged fetch-and-checkout attempt for a git-sourced
/// repository.
/// </summary>
internal sealed record RepoContextGitFetchRequest
{
    /// <summary>The repository's git source configuration. Never carries a secret.</summary>
    public required RepoContextGitSourceOptions Source { get; init; }

    /// <summary>The resolved credential the transport presents to the remote.</summary>
    public required RepoContextGitCredential Credential { get; init; }

    /// <summary>
    /// The staging work tree to fetch into and check out. It is created if absent
    /// and re-used across refreshes, so an incremental fetch only transfers the new
    /// objects.
    /// </summary>
    public required string WorkTreePath { get; init; }

    /// <summary>
    /// The commit SHA of the last generation that indexed successfully, or
    /// <see langword="null"/> when the repository has never completed one. Used to
    /// short-circuit an unchanged ref and to compute the between-commit change
    /// summary when the previous commit is still in the object database.
    /// </summary>
    public string? LastIndexedCommitSha { get; init; }
}
