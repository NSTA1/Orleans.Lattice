namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The durable inputs to a repository indexing job: the resolved absolute working
/// tree to walk, the repository identity records are filed under, and the optional
/// include / exclude filters that scope the walk. Unlike the transient
/// <see cref="RepoContextBootstrapRequest"/>, this record crosses the grain
/// boundary and is persisted in <see cref="RepoIndexJobState"/> so a job can be
/// resumed after a host restart without the original client call - it therefore
/// carries Orleans serialization metadata.
/// <para>
/// The <see cref="RepoRoot"/> is the already-resolved, workspace-bounds-checked
/// absolute path: the fail-closed workspace guard is applied at the tool seam
/// before the job is started, so a persisted, later-resumed job never re-derives
/// a path that could escape the workspace.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoIndexJobRequest)]
[Immutable]
public sealed record RepoIndexJobRequest
{
    /// <summary>The resolved absolute path to the repository working tree to walk.</summary>
    [Id(0)]
    public required string RepoRoot { get; init; }

    /// <summary>The repository identity records are filed under.</summary>
    [Id(1)]
    public required string RepoId { get; init; }

    /// <summary>
    /// Optional include globs; when non-empty a file is walked only if it matches
    /// at least one.
    /// </summary>
    [Id(2)]
    public IReadOnlyList<string>? IncludeGlobs { get; init; }

    /// <summary>
    /// Optional exclude globs; a match removes a file from the walk even when it
    /// also matched an include.
    /// </summary>
    [Id(3)]
    public IReadOnlyList<string>? ExcludeGlobs { get; init; }

    /// <summary>
    /// When <see langword="true"/> (the default), the tree's <c>.gitignore</c>
    /// files are honoured hierarchically so ignored files and directories are not
    /// walked. A persisted job that predates this field deserialises it as
    /// <see langword="false"/>, preserving the exact walk a pre-upgrade job began.
    /// </summary>
    [Id(4)]
    public bool RespectGitignore { get; init; }

    /// <summary>
    /// When <see langword="true"/> (the default at the tool seam), a file whose
    /// leading bytes look non-text (a NUL byte is present) is dropped from the walk
    /// so binary blobs never enter the index. A persisted job that predates this
    /// field deserialises it as <see langword="false"/>, preserving the exact walk a
    /// pre-upgrade job began.
    /// </summary>
    [Id(5)]
    public bool ExcludeBinary { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the background reconcile that re-drives this job
    /// may use directory-modification-time pruning to skip the per-file stat of
    /// unchanged directories. It is set only by the self-index continuous-reconcile
    /// path, whose periodic full-sweep backstop catches the in-place content edits
    /// pruning cannot see; an explicit onboarding or re-bootstrap leaves it
    /// <see langword="false"/> so the walk is full and exact. A persisted job that
    /// predates this field deserialises it as <see langword="false"/>, so a resumed
    /// pre-upgrade job runs the same exact walk it began.
    /// </summary>
    [Id(6)]
    public bool AllowPrune { get; init; }
}
