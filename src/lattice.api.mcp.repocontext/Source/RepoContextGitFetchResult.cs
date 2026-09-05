namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What one staged fetch resolved: the commit the configured ref now points at,
/// whether the work tree was checked out, and - when the previous commit was still
/// resolvable in the object database - a between-commit change summary for
/// diagnostics.
/// <para>
/// The change summary is diagnostic only. The authoritative add / modify / delete
/// changeset is computed downstream by diffing the commit tree against the stored
/// per-file digests, which stays correct even when the previous commit was pruned
/// by a shallow fetch.
/// </para>
/// </summary>
internal sealed record RepoContextGitFetchResult
{
    /// <summary>The commit SHA the configured ref resolved to.</summary>
    public required string CommitSha { get; init; }

    /// <summary>
    /// The commit SHA the staging tree held before this fetch, or
    /// <see langword="null"/> when the tree was cold.
    /// </summary>
    public string? PreviousCommitSha { get; init; }

    /// <summary>
    /// Whether the work tree was checked out to <see cref="CommitSha"/>. False when
    /// the resolved commit already equalled the last-indexed commit, in which case
    /// the tree is left untouched.
    /// </summary>
    public bool CheckedOut { get; init; }

    /// <summary>
    /// Whether <see cref="Added"/>, <see cref="Modified"/>, and
    /// <see cref="Deleted"/> were computed. False when the previous commit was not
    /// present in the object database (the usual case after a shallow fetch).
    /// </summary>
    public bool DiffAvailable { get; init; }

    /// <summary>Paths added between the previous and resolved commit.</summary>
    public int Added { get; init; }

    /// <summary>Paths modified between the previous and resolved commit.</summary>
    public int Modified { get; init; }

    /// <summary>Paths deleted between the previous and resolved commit.</summary>
    public int Deleted { get; init; }
}
