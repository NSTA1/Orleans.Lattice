namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A single registered repository as reported by <c>repocontext_list_repos</c>:
/// its identity, the filesystem root it was indexed from, the last time it was
/// ingested, and the number of files recorded for it. An agent uses the list to
/// discover which repositories under the mounted workspace are queryable before it
/// searches, recalls, or removes one - and to confirm that a given id is about the
/// tree it expects, which the id alone does not establish.
/// </summary>
/// <remarks>
/// An MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRepoSummary
{
    /// <summary>The repository identity records are filed under.</summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// The resolved filesystem root the repository's records were actually walked from,
    /// or <see langword="null"/> when it was never indexed (or its index was reset, which
    /// clears the durable request this is read from). It is read from the same persisted
    /// index request the drift report walks, so it is the index's own account of what it
    /// is about rather than a restatement of the id.
    /// <para>
    /// It is reported because <see cref="RepoId"/> does not imply it and cannot.
    /// A repository's id defaults to the final segment of the indexed path, so a git
    /// worktree registered from the wrong workspace root indexes under the BASE
    /// repository's id while every record in it describes the worktree - a state in which
    /// the ingest marker, the file count, and search ranking are all genuinely healthy and
    /// none of them is about the tree the caller means. Reporting the root beside the id
    /// makes that self-evident on the first call of a session: the id reads
    /// <c>lattice</c> and the root reads <c>/workspace/bucket4-merge</c> on the same row.
    /// </para>
    /// </summary>
    public string? IndexedRoot { get; init; }

    /// <summary>
    /// The last-ingested marker recorded on the repository root, or
    /// <see langword="null"/> when none was written. It is re-stamped by every
    /// completed indexing pass, including one that walked the tree and found nothing
    /// to change, so it reads as the time the index was last verified current rather
    /// than the time a file last changed.
    /// </summary>
    public string? LastIngested { get; init; }

    /// <summary>
    /// The number of files recorded for the repository as of its last ingestion,
    /// or <see langword="null"/> when the count was not recorded (a repository
    /// ingested before the count was tracked).
    /// </summary>
    public long? FileCount { get; init; }

    /// <summary>
    /// The number of sources with a live embedding currently in the store - the size
    /// of the durable add-wins vector-membership set, read from the store of record
    /// (never the last run's in-flight progress). A source is a file (embedded as
    /// several overlapping window passages), a captured symbol (embedded as a
    /// single passage), or an embedded memory entry, so this counts distinct
    /// embedded files plus embedded symbols and memory entries, and therefore
    /// exceeds <see cref="FileCount"/> once symbols are embedded. It is
    /// <c>0</c> for a repository whose embeddings have not yet landed and rises as
    /// vectorising completes. Because it is read from durable state rather than run
    /// progress, it survives a host restart, so an operator can confirm the vectors
    /// persisted by re-reading it after a restart.
    /// <para>
    /// <see langword="null"/> means <em>not yet measured</em>, which is not the same
    /// answer as <c>0</c>: computing the count exactly costs a walk of the whole
    /// membership tree, so it is served from the last completed walk and refreshed out
    /// of band rather than blocking this call (issue 1992). Read it together with
    /// <see cref="EmbeddedVectorCountPending"/>.
    /// </para>
    /// </summary>
    public long? EmbeddedVectorCount { get; init; }

    /// <summary>
    /// Whether a refresh of <see cref="EmbeddedVectorCount"/> is outstanding, so the
    /// reported count is from an earlier membership generation (or absent entirely)
    /// rather than the current one. <see langword="false"/> means the count is exact as
    /// of this call. Expect <see langword="true"/> throughout an active ingest, which
    /// advances the membership set continuously; poll again once it settles for an
    /// exact figure.
    /// </summary>
    public bool EmbeddedVectorCountPending { get; init; }

    /// <summary>
    /// The commit SHA the repository's index generation was built from, or
    /// <see langword="null"/> when the repository is indexed from a mounted
    /// workspace (which has no commit anchor). For a git-ref-sourced repository this
    /// is the verifiable answer to "which revision is this index serving", and it is
    /// the same value a spoke reports for its replicated copy.
    /// </summary>
    public string? IndexedCommit { get; init; }
}
