namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The background-indexing cadence knobs for the repository-context surface: how often
/// the per-repository self-index grain ticks, how the periodic content reconcile is
/// spaced, and how often the walk's directory-modification-time pruning is forced to fall
/// back to a full sweep. The defaults reproduce the original behaviour (a 15-minute
/// reconcile with a 15-second tick), so an in-process host is unchanged unless it opts in.
/// <para>
/// The container host makes the reconcile near-continuous by setting a short
/// <see cref="ReconcileInterval"/> (and a short <see cref="TickInterval"/> so completion is
/// noticed promptly): because the reconcile is single-flight in the job grain and each
/// tick is a fresh grain turn, re-driving on completion polls for the previous run to
/// finish rather than recursing, so it can never overflow the stack or re-enter itself.
/// </para>
/// <para>
/// This is a plain in-memory options object, resolved once from the environment in
/// <c>AddRepoContextTools</c> and injected as a singleton. A host or test harness that
/// registers its own instance first wins (the registration uses <c>TryAdd</c>).
/// </para>
/// </summary>
internal sealed class RepoContextIndexingOptions
{
    /// <summary>Environment variable overriding <see cref="TickInterval"/> (in seconds).</summary>
    public const string TickIntervalSecondsKey = "LATTICE_SELFINDEX_TICK_SECONDS";

    /// <summary>Environment variable overriding <see cref="ReconcileInterval"/> (in seconds).</summary>
    public const string ReconcileIntervalSecondsKey = "LATTICE_RECONCILE_INTERVAL_SECONDS";

    /// <summary>Environment variable overriding <see cref="ReconcileIntervalJitter"/> (in seconds).</summary>
    public const string ReconcileJitterSecondsKey = "LATTICE_RECONCILE_JITTER_SECONDS";

    /// <summary>Environment variable overriding <see cref="FullWalkInterval"/> (in seconds).</summary>
    public const string FullWalkIntervalSecondsKey = "LATTICE_FULL_WALK_INTERVAL_SECONDS";

    /// <summary>Environment variable overriding <see cref="VectorCacheTtl"/> (in seconds).</summary>
    public const string VectorCacheTtlSecondsKey = "LATTICE_VECTOR_CACHE_TTL_SECONDS";

    /// <summary>Environment variable overriding <see cref="TokenizerProfile"/>.</summary>
    public const string TokenizerProfileKey = "LATTICE_REPOCONTEXT_TOKENIZER";

    /// <summary>Environment variable overriding <see cref="Role"/>.</summary>
    public const string IndexingRoleKey = "LATTICE_REPOCONTEXT_INDEXING_ROLE";

    /// <summary>Environment variable overriding <see cref="SemanticRetrieval"/>.</summary>
    public const string SemanticRetrievalKey = "LATTICE_REPOCONTEXT_SEMANTIC_RETRIEVAL";

    /// <summary>The <see cref="SemanticRetrieval"/> value selecting the persisted approximate index (the default).</summary>
    public const string SemanticRetrievalApproximate = "approximate";

    /// <summary>The <see cref="SemanticRetrieval"/> value selecting the brute-force exact scan.</summary>
    public const string SemanticRetrievalExact = "exact";

    /// <summary>The <see cref="TokenizerProfile"/> value selecting the OpenAI o200k_base BPE encoding (the default).</summary>
    public const string TokenizerProfileO200k = "o200k";

    /// <summary>The <see cref="TokenizerProfile"/> value selecting the OpenAI cl100k_base BPE encoding.</summary>
    public const string TokenizerProfileCl100k = "cl100k";

    /// <summary>The self-index grain tick cadence; each tick does at most one unit of work.</summary>
    public TimeSpan TickInterval { get; init; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// The base interval between periodic content reconciles. A short value (with a small
    /// or zero <see cref="ReconcileIntervalJitter"/>) makes the reconcile effectively
    /// continuous, bounded by <see cref="TickInterval"/>.
    /// </summary>
    public TimeSpan ReconcileInterval { get; init; } = TimeSpan.FromMinutes(15);

    /// <summary>The maximum extra random interval added on top of <see cref="ReconcileInterval"/> to desync repositories.</summary>
    public TimeSpan ReconcileIntervalJitter { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How often the reconcile walk is forced to ignore the directory-modification-time
    /// prune cache and stat every file, so an in-place content edit (which does not bump a
    /// directory's modification time and is therefore invisible to pruning) is picked up
    /// within this bound. The first walk after a process start is always a full one.
    /// </summary>
    public TimeSpan FullWalkInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How long a warm decoded-vector candidate set is trusted in the
    /// <see cref="RepoContextVectorCache"/> before it is re-gathered from the store.
    /// Local writes invalidate the cache precisely and immediately, so this bound
    /// only backstops a change that bypasses the local writer - a vector landing via
    /// cross-cluster replication - which the invalidation cannot observe. A short
    /// default keeps such a change visible quickly while still absorbing repeated
    /// queries between writes. A value of zero (or negative) disables the cache: every
    /// query re-gathers, exactly as the uncached path did.
    /// </summary>
    public TimeSpan VectorCacheTtl { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The BPE tokenizer profile the token counter uses to measure per-file token
    /// counts: <see cref="TokenizerProfileO200k"/> (the default, OpenAI o200k_base) or
    /// <see cref="TokenizerProfileCl100k"/> (OpenAI cl100k_base). Resolved from
    /// <see cref="TokenizerProfileKey"/>; an absent or unrecognised value falls back
    /// to the default profile (fail-closed).
    /// </summary>
    public string TokenizerProfile { get; init; } = TokenizerProfileO200k;

    /// <summary>
    /// The indexing role this cluster plays. <see cref="RepoContextIndexingRole.Hub"/>
    /// (the default, preserving single-cluster behaviour) is the authoritative
    /// indexer; <see cref="RepoContextIndexingRole.Spoke"/> is a read-only replica
    /// whose self-index grain never walks, reconciles, prunes, or re-embeds.
    /// Resolved from <see cref="IndexingRoleKey"/>; an absent or unrecognised value
    /// falls back to <see cref="RepoContextIndexingRole.Hub"/> (fail-closed to the
    /// original single-cluster behaviour).
    /// </summary>
    public RepoContextIndexingRole Role { get; init; } = RepoContextIndexingRole.Hub;

    /// <summary>
    /// Whether this cluster's self-index grain may mutate source-derived index
    /// state (walk, reconcile, prune, re-embed). True only for a
    /// <see cref="RepoContextIndexingRole.Hub"/>; a
    /// <see cref="RepoContextIndexingRole.Spoke"/> serves replicated reads but its
    /// index pass is inert.
    /// </summary>
    public bool IndexingEnabled => Role == RepoContextIndexingRole.Hub;

    /// <summary>
    /// Which semantic retrieval path is bound:
    /// <see cref="RepoContextSemanticRetrievalMode.Approximate"/> (the default) routes
    /// semantic search through the persisted approximate nearest-neighbour index, and
    /// <see cref="RepoContextSemanticRetrievalMode.Exact"/> routes it through the
    /// brute-force exact scan instead. Resolved from
    /// <see cref="SemanticRetrievalKey"/>; an absent or unrecognised value falls back
    /// to the default (fail-closed to the configured default rather than to a path
    /// nobody chose). The answer reports which guarantee it carries through
    /// <see cref="RepoContextSearchResult.RetrievalPath"/> either way.
    /// </summary>
    public RepoContextSemanticRetrievalMode SemanticRetrieval { get; init; } =
        RepoContextSemanticRetrievalMode.Approximate;

    /// <summary>
    /// Resolves the options from environment variables, falling back to the defaults (the
    /// original behaviour) for any variable that is absent or malformed.
    /// </summary>
    /// <returns>The resolved options.</returns>
    public static RepoContextIndexingOptions FromEnvironment()
    {
        var defaults = new RepoContextIndexingOptions();
        return new RepoContextIndexingOptions
        {
            TickInterval = ReadSeconds(TickIntervalSecondsKey, defaults.TickInterval),
            ReconcileInterval = ReadSeconds(ReconcileIntervalSecondsKey, defaults.ReconcileInterval),
            ReconcileIntervalJitter = ReadSeconds(ReconcileJitterSecondsKey, defaults.ReconcileIntervalJitter),
            FullWalkInterval = ReadSeconds(FullWalkIntervalSecondsKey, defaults.FullWalkInterval),
            VectorCacheTtl = ReadSeconds(VectorCacheTtlSecondsKey, defaults.VectorCacheTtl),
            TokenizerProfile = ReadTokenizerProfile(TokenizerProfileKey, defaults.TokenizerProfile),
            Role = ReadIndexingRole(IndexingRoleKey, defaults.Role),
            SemanticRetrieval = ReadSemanticRetrieval(SemanticRetrievalKey, defaults.SemanticRetrieval),
        };
    }

    private static RepoContextSemanticRetrievalMode ReadSemanticRetrieval(
        string key, RepoContextSemanticRetrievalMode fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        // Fail closed on any unrecognised value: only the two supported modes are
        // honoured and anything else falls back to the default, so a typo can never
        // silently leave the box on a retrieval path nobody chose.
        return raw.Trim().ToLowerInvariant() switch
        {
            SemanticRetrievalApproximate => RepoContextSemanticRetrievalMode.Approximate,
            SemanticRetrievalExact => RepoContextSemanticRetrievalMode.Exact,
            _ => fallback,
        };
    }

    private static RepoContextIndexingRole ReadIndexingRole(string key, RepoContextIndexingRole fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        // Fail closed on any unrecognised value: only the two supported roles are
        // honoured; anything else falls back to the default (Hub), so a typo can
        // never silently turn a cluster into an inert spoke that indexes nothing.
        return raw.Trim().ToLowerInvariant() switch
        {
            "hub" => RepoContextIndexingRole.Hub,
            "spoke" => RepoContextIndexingRole.Spoke,
            _ => fallback,
        };
    }

    private static string ReadTokenizerProfile(string key, string fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        // Fail closed on any unrecognised value: only the two supported profiles are
        // honoured; anything else falls back to the default.
        return raw.Trim().ToLowerInvariant() switch
        {
            TokenizerProfileO200k => TokenizerProfileO200k,
            TokenizerProfileCl100k => TokenizerProfileCl100k,
            _ => fallback,
        };
    }

    private static TimeSpan ReadSeconds(string key, TimeSpan fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (!string.IsNullOrWhiteSpace(raw)
            && double.TryParse(raw, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var seconds)
            && seconds >= 0)
        {
            return TimeSpan.FromSeconds(seconds);
        }

        return fallback;
    }
}
