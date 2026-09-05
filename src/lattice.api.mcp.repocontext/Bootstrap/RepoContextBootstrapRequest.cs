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

    /// <summary>
    /// When <see langword="true"/>, the walk may skip the per-file stat of a
    /// directory whose modification time is unchanged since the previous walk
    /// (directory-modification-time pruning), trading exactness for a much cheaper
    /// sweep. Pruning is blind to an in-place content edit that leaves a
    /// directory's modification time untouched, so it is only ever enabled for the
    /// continuous background reconcile, whose periodic full-sweep backstop closes
    /// that gap within a bounded interval. Defaults to <see langword="false"/>, so
    /// an explicit onboarding or re-bootstrap always runs a full, exact walk.
    /// </summary>
    public bool AllowPrune { get; init; }

    /// <summary>
    /// When <see langword="true"/>, the vectorisation arm re-probes the whole
    /// content-unchanged set for embedding gaps regardless of the periodic gap-scan
    /// cadence (<see cref="RepoContextIndexingOptions.PassesPerEmbeddingGapScan"/>).
    /// It is set by the re-drive the self-index grain's out-of-band paged gap sweep
    /// triggers, which knows a vector is genuinely missing, so the healing pass runs
    /// immediately instead of waiting for the cadence. Defaults to
    /// <see langword="false"/>; a run with <see cref="AllowPrune"/> false (an explicit
    /// onboarding or re-bootstrap) always scans anyway.
    /// </summary>
    public bool ForceEmbeddingGapScan { get; init; }

    /// <summary>
    /// The resolved commit SHA this run indexes, when the repository is git-sourced.
    /// It switches the run's scan set from a filesystem walk to the commit's tree
    /// (see <see cref="IRepoContextSourceScanner"/>) and is stamped onto the
    /// repository node so the generation is reproducible. <see langword="null"/> for
    /// a mounted-workspace run, which walks the tree exactly as before.
    /// </summary>
    public string? CommitSha { get; init; }
}
