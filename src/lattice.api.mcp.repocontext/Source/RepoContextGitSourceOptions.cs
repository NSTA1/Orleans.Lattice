namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The opt-in configuration that binds one repository identity to a git remote and
/// ref, replacing the mounted-workspace walk for that repository. Configuration is
/// the declared truth: the remote URL plus the ref say what the hub serves, and the
/// commit SHA the fetch resolves anchors every index generation built from it.
/// <para>
/// This record deliberately carries no secret. Credentials are resolved separately
/// through <see cref="IRepoContextGitCredentialProvider"/>, so a token can never
/// leak through a log line that formats these options.
/// </para>
/// </summary>
internal sealed record RepoContextGitSourceOptions
{
    /// <summary>The default refresh cadence when none is configured.</summary>
    internal static readonly TimeSpan DefaultRefreshInterval = TimeSpan.FromMinutes(5);

    /// <summary>The default bound on a single fetch attempt.</summary>
    internal static readonly TimeSpan DefaultFetchTimeout = TimeSpan.FromMinutes(5);

    /// <summary>The repository identity records are filed under.</summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// The remote to fetch from. Empty when the repository was declared git-sourced
    /// but its URL is missing, which is a configuration error the source reports as
    /// a fail-closed failure rather than silently falling back to a mount.
    /// </summary>
    public required string RemoteUrl { get; init; }

    /// <summary>
    /// The ref to track, as a fully-qualified ref (<c>refs/heads/main</c>) or a bare
    /// branch name (<c>main</c>), which is normalised to a branch ref.
    /// </summary>
    public string Reference { get; init; } = RepoContextGitReference.DefaultReference;

    /// <summary>
    /// The shallow-fetch depth in commits. <c>1</c> (the default) keeps the hub's
    /// working copy small; <c>0</c> fetches the full history. The local transport
    /// ignores depth, so a fetch may legitimately return more history than asked.
    /// </summary>
    public int Depth { get; init; } = 1;

    /// <summary>How often the reminder-driven refresh re-fetches the ref.</summary>
    public TimeSpan RefreshInterval { get; init; } = DefaultRefreshInterval;

    /// <summary>
    /// The bound on a single fetch-and-checkout attempt. The transport is
    /// synchronous, so a hung remote would otherwise wedge the per-repository
    /// singleton grain until the process restarts.
    /// </summary>
    public TimeSpan FetchTimeout { get; init; } = DefaultFetchTimeout;

    /// <summary>How the fetch authenticates. See <see cref="RepoContextGitAuthMode"/>.</summary>
    public RepoContextGitAuthMode AuthMode { get; init; } = RepoContextGitAuthMode.Token;

    /// <summary>
    /// Optional include globs; when non-empty a file in the commit tree is indexed
    /// only if it matches at least one.
    /// </summary>
    public IReadOnlyList<string>? IncludeGlobs { get; init; }

    /// <summary>
    /// Optional exclude globs; a match removes a file from the indexed set even when
    /// it also matched an include.
    /// </summary>
    public IReadOnlyList<string>? ExcludeGlobs { get; init; }

    /// <summary>
    /// When <see langword="true"/> (the default), a blob whose leading bytes look
    /// non-text is dropped so binary blobs never enter the index.
    /// </summary>
    public bool ExcludeBinary { get; init; } = true;
}
