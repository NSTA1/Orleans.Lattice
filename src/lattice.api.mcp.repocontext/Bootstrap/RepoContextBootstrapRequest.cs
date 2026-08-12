namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The inputs to a single <c>repocontext_bootstrap</c> ingestion run: which tree
/// to walk, the repository identity to file records under, and the optional
/// include / exclude filters that scope the walk. This is an in-memory request
/// object assembled from the tool's parameters server-side; it never crosses an
/// Orleans wire, so it carries no serialization attributes.
/// </summary>
internal sealed class RepoContextBootstrapRequest
{
    /// <summary>
    /// The absolute path to the repository working tree the server should walk.
    /// </summary>
    public required string RepoRoot { get; init; }

    /// <summary>
    /// The repository identity records are filed under (the <c>{repoId}</c> in the
    /// key grammar). A stable alias chosen by the caller so re-ingesting the same
    /// codebase updates the same records.
    /// </summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// Optional include globs; when non-empty a file is walked only if it matches
    /// at least one. When null or empty every file (outside <c>.git</c>) is a
    /// candidate.
    /// </summary>
    public IReadOnlyList<string>? IncludeGlobs { get; init; }

    /// <summary>
    /// Optional exclude globs; a match removes a file from the walk even when it
    /// also matched an include.
    /// </summary>
    public IReadOnlyList<string>? ExcludeGlobs { get; init; }

    /// <summary>
    /// When <see langword="true"/> (the default), the tree's <c>.gitignore</c>
    /// files are honoured hierarchically so ignored files and directories are not
    /// walked. Set to <see langword="false"/> to walk every file regardless of
    /// ignore rules.
    /// </summary>
    public bool RespectGitignore { get; init; } = true;

    /// <summary>
    /// When <see langword="true"/> (the default), a file whose leading bytes look
    /// non-text (a NUL byte is present) is dropped from the walk, so compiled
    /// artefacts, images, and other binary blobs never enter the index. Set to
    /// <see langword="false"/> to ingest binary files too.
    /// </summary>
    public bool ExcludeBinary { get; init; } = true;
}
