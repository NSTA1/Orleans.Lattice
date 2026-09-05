namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The single seam that decides which repositories are git-sourced, and the only
/// place the mount-versus-git mutual exclusion is enforced. A repository listed here
/// is never walked from a mounted path and is refused by <c>repocontext_add_repo</c>;
/// a repository absent from here always uses the mounted-workspace default.
/// <para>
/// The registry is empty unless the feature is explicitly configured, so the
/// mounted-workspace path stays the default and this whole subsystem stays inert
/// until an operator opts in.
/// </para>
/// </summary>
internal sealed class RepoContextGitSourceRegistry
{
    /// <summary>The environment variable listing the git-sourced repository ids.</summary>
    internal const string ReposVariable = "LATTICE_REPOCONTEXT_GIT_REPOS";

    /// <summary>The environment variable overriding the staging root.</summary>
    internal const string StagingRootVariable = "LATTICE_REPOCONTEXT_GIT_STAGING_ROOT";

    /// <summary>The prefix every per-repository git setting shares.</summary>
    internal const string SettingPrefix = "LATTICE_REPOCONTEXT_GIT_";

    private static readonly char[] ListSeparators = [';', ','];

    private readonly Dictionary<string, RepoContextGitSourceOptions> _byRepoId;

    /// <summary>
    /// Creates a registry over an explicit set of git sources. Used by the host when
    /// it configures sources in code rather than through the environment.
    /// </summary>
    /// <param name="sources">The configured git sources. Must not be
    /// <see langword="null"/>. A duplicate repository id keeps the first entry, so a
    /// later duplicate cannot silently redirect an already-declared repository at a
    /// different remote.</param>
    /// <param name="stagingRoot">The directory staging work trees are created under.
    /// Must not be <see langword="null"/> or blank.</param>
    public RepoContextGitSourceRegistry(IEnumerable<RepoContextGitSourceOptions> sources, string stagingRoot)
    {
        ArgumentNullException.ThrowIfNull(sources);
        ArgumentException.ThrowIfNullOrWhiteSpace(stagingRoot);

        _byRepoId = new Dictionary<string, RepoContextGitSourceOptions>(StringComparer.Ordinal);
        foreach (var source in sources)
        {
            ArgumentNullException.ThrowIfNull(source);
            _byRepoId.TryAdd(source.RepoId, source);
        }

        StagingRoot = Path.GetFullPath(stagingRoot);
    }

    /// <summary>An empty registry: no repository is git-sourced.</summary>
    internal static RepoContextGitSourceRegistry Empty { get; } =
        new([], DefaultStagingRoot());

    /// <summary>The directory staging work trees are created under.</summary>
    public string StagingRoot { get; }

    /// <summary>Whether no repository is git-sourced, so the feature is inert.</summary>
    public bool IsEmpty => _byRepoId.Count == 0;

    /// <summary>Every configured git source, in registration order.</summary>
    public IReadOnlyCollection<RepoContextGitSourceOptions> Sources => _byRepoId.Values;

    /// <summary>
    /// Whether <paramref name="repoId"/> is declared git-sourced. A declared
    /// repository is git-sourced even when its configuration is incomplete, so a
    /// misconfiguration fails closed instead of degrading to a mounted walk.
    /// </summary>
    /// <param name="repoId">The repository identity to test. Must not be
    /// <see langword="null"/>.</param>
    /// <returns><see langword="true"/> when the repository is git-sourced.</returns>
    public bool IsGitSourced(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _byRepoId.ContainsKey(repoId);
    }

    /// <summary>
    /// The git source configured for <paramref name="repoId"/>, or
    /// <see langword="null"/> when the repository is not git-sourced.
    /// </summary>
    /// <param name="repoId">The repository identity to look up. Must not be
    /// <see langword="null"/>.</param>
    /// <returns>The configured options, or <see langword="null"/>.</returns>
    public RepoContextGitSourceOptions? Find(string repoId)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return _byRepoId.GetValueOrDefault(repoId);
    }

    /// <summary>
    /// Reads the git-source configuration from the process environment. Absent or
    /// blank <c>LATTICE_REPOCONTEXT_GIT_REPOS</c> yields an empty registry, which is
    /// what keeps the mounted workspace the default. Every malformed numeric or
    /// enumeration value falls back to its safe default rather than failing host
    /// startup.
    /// </summary>
    /// <returns>The registry described by the environment; never <see langword="null"/>.</returns>
    public static RepoContextGitSourceRegistry FromEnvironment()
    {
        var repoIds = SplitList(Environment.GetEnvironmentVariable(ReposVariable));
        if (repoIds.Count == 0)
        {
            return Empty;
        }

        var stagingRoot = Environment.GetEnvironmentVariable(StagingRootVariable);
        var sources = new List<RepoContextGitSourceOptions>(repoIds.Count);
        foreach (var repoId in repoIds)
        {
            sources.Add(ReadSource(repoId));
        }

        return new RepoContextGitSourceRegistry(
            sources,
            string.IsNullOrWhiteSpace(stagingRoot) ? DefaultStagingRoot() : stagingRoot);
    }

    /// <summary>
    /// The environment-variable name carrying <paramref name="setting"/> for
    /// <paramref name="repoId"/>. The repository id is folded to an upper-case
    /// identifier so a repository named <c>my-repo</c> reads
    /// <c>LATTICE_REPOCONTEXT_GIT_MY_REPO_URL</c>.
    /// </summary>
    /// <param name="repoId">The repository identity. Must not be <see langword="null"/>.</param>
    /// <param name="setting">The trailing setting name, such as <c>URL</c>.</param>
    /// <returns>The full environment-variable name.</returns>
    internal static string VariableName(string repoId, string setting)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(setting);

        var buffer = new char[repoId.Length];
        for (var i = 0; i < repoId.Length; i++)
        {
            var c = repoId[i];
            buffer[i] = char.IsAsciiLetterOrDigit(c) ? char.ToUpperInvariant(c) : '_';
        }

        return SettingPrefix + new string(buffer) + "_" + setting;
    }

    /// <summary>The staging root used when none is configured.</summary>
    private static string DefaultStagingRoot() =>
        Path.Combine(Path.GetTempPath(), "lattice-repocontext-git");

    private static RepoContextGitSourceOptions ReadSource(string repoId)
    {
        var url = Environment.GetEnvironmentVariable(VariableName(repoId, "URL"));
        var reference = Environment.GetEnvironmentVariable(VariableName(repoId, "REF"));
        var auth = Environment.GetEnvironmentVariable(VariableName(repoId, "AUTH"));

        return new RepoContextGitSourceOptions
        {
            RepoId = repoId,
            RemoteUrl = url?.Trim() ?? string.Empty,
            Reference = string.IsNullOrWhiteSpace(reference)
                ? RepoContextGitReference.DefaultReference
                : reference.Trim(),
            Depth = ReadInt(VariableName(repoId, "DEPTH"), fallback: 1, min: 0, max: 100_000),
            RefreshInterval = TimeSpan.FromSeconds(ReadInt(
                VariableName(repoId, "REFRESH_SECONDS"),
                fallback: (int)RepoContextGitSourceOptions.DefaultRefreshInterval.TotalSeconds,
                min: 30,
                max: 86_400)),
            FetchTimeout = TimeSpan.FromSeconds(ReadInt(
                VariableName(repoId, "FETCH_TIMEOUT_SECONDS"),
                fallback: (int)RepoContextGitSourceOptions.DefaultFetchTimeout.TotalSeconds,
                min: 10,
                max: 3_600)),
            AuthMode = string.Equals(auth?.Trim(), "anonymous", StringComparison.OrdinalIgnoreCase)
                ? RepoContextGitAuthMode.Anonymous
                : RepoContextGitAuthMode.Token,
            IncludeGlobs = NullIfEmpty(SplitList(Environment.GetEnvironmentVariable(VariableName(repoId, "INCLUDE")))),
            ExcludeGlobs = NullIfEmpty(SplitList(Environment.GetEnvironmentVariable(VariableName(repoId, "EXCLUDE")))),
            ExcludeBinary = !string.Equals(
                Environment.GetEnvironmentVariable(VariableName(repoId, "EXCLUDE_BINARY"))?.Trim(),
                "false",
                StringComparison.OrdinalIgnoreCase),
        };
    }

    private static IReadOnlyList<string>? NullIfEmpty(IReadOnlyList<string> values) =>
        values.Count == 0 ? null : values;

    private static List<string> SplitList(string? raw)
    {
        var values = new List<string>();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return values;
        }

        foreach (var part in raw.Split(ListSeparators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!values.Contains(part, StringComparer.Ordinal))
            {
                values.Add(part);
            }
        }

        return values;
    }

    private static int ReadInt(string variable, int fallback, int min, int max)
    {
        var raw = Environment.GetEnvironmentVariable(variable);
        if (!int.TryParse(
                raw,
                System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture,
                out var parsed))
        {
            return fallback;
        }

        return Math.Clamp(parsed, min, max);
    }
}
