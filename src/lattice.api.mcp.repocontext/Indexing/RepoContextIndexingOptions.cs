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
