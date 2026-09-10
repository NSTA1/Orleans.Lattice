using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What the memory archive is permitted to restore at startup.
/// </summary>
internal enum RepoContextMemoryArchiveRestoreMode
{
    /// <summary>Never restore. The archive is written but never read back automatically.</summary>
    Off,

    /// <summary>
    /// Restore only when the memory tree holds no records at all - the state a store
    /// is left in by a volume wipe. A store with any memory in it is left alone, so an
    /// ordinary restart never re-imports.
    /// </summary>
    Auto,

    /// <summary>
    /// Restore on every start, merging the archive into whatever the store already
    /// holds. Safe because the import is a CRDT join over the memory record's
    /// multi-value register rather than an overwrite, so an older archive meeting a
    /// newer live entry converges on the newer value instead of regressing it.
    /// </summary>
    Always,
}

/// <summary>
/// The configuration of the durable-memory archive: where the snapshot is written,
/// how often, and whether it may be restored automatically.
/// <para>
/// <b>Why this exists.</b> A repository-context deployment holds two kinds of state
/// in one store. The code index is derived and rebuilds by re-indexing; agent memory
/// is authored and rebuilds from nothing. A volume-level wipe cannot tell them apart,
/// so it destroys the irreplaceable half along with the cheap half. The archive
/// carries the irreplaceable half to a location the wipe does not reach.
/// </para>
/// <para>
/// <b>What this is not.</b> It is not a backup product and it does not make the
/// container self-healing beyond one narrow case. It protects memory only as far as
/// the last successful export, it covers only the memory tree, and it restores only
/// under <see cref="RepoContextMemoryArchiveRestoreMode.Auto"/> into an empty store
/// or on an explicit <see cref="RepoContextMemoryArchiveRestoreMode.Always"/>. A
/// scheduled whole-store backup with manifests, retention, and an operator-driven
/// restore is a separate concern served by the backup package.
/// </para>
/// <para>
/// The feature is inert unless <see cref="Directory"/> is set, so a host that
/// configures nothing gains no background work and no new failure mode.
/// </para>
/// </summary>
internal sealed class RepoContextMemoryArchiveOptions
{
    /// <summary>Environment variable supplying <see cref="Directory"/>. Absent or blank disables the archive.</summary>
    internal const string DirectoryKey = "LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR";

    /// <summary>Environment variable overriding <see cref="Interval"/> (in seconds).</summary>
    internal const string IntervalSecondsKey = "LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_INTERVAL_SECONDS";

    /// <summary>Environment variable overriding <see cref="RestoreMode"/> (<c>off</c>, <c>auto</c>, or <c>always</c>).</summary>
    internal const string RestoreKey = "LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_RESTORE";

    /// <summary>Environment variable overriding <see cref="StopTimeout"/> (in seconds).</summary>
    internal const string StopTimeoutSecondsKey = "LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_STOP_TIMEOUT_SECONDS";

    /// <summary>
    /// The floor the export cadence is clamped to. A cadence below this would spend
    /// more of the store's read budget on exporting memory than on serving it, for a
    /// residual-window saving measured in seconds.
    /// </summary>
    internal static readonly TimeSpan MinimumInterval = TimeSpan.FromSeconds(30);

    /// <summary>The ceiling the stop-time export budget is clamped to.</summary>
    internal static readonly TimeSpan MaximumStopTimeout = TimeSpan.FromSeconds(60);

    /// <summary>The floor the stop-time export budget is clamped to.</summary>
    internal static readonly TimeSpan MinimumStopTimeout = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The directory the snapshot is written to, or <see langword="null"/> when no
    /// archive is configured. For the container this must be a path that a volume
    /// wipe does not reach - a host bind mount - because a directory inside the
    /// container's data volume is destroyed by the same gesture the archive exists to
    /// survive.
    /// </summary>
    public string? Directory { get; init; }

    /// <summary>
    /// How often memory is exported while the host runs. The default is deliberately
    /// short: the interval is the width of the window in which authored memory exists
    /// only in the volume, and that window is the residual risk this feature does not
    /// remove.
    /// </summary>
    public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Whether the archive may be imported at startup, and under what condition.</summary>
    public RepoContextMemoryArchiveRestoreMode RestoreMode { get; init; }
        = RepoContextMemoryArchiveRestoreMode.Auto;

    /// <summary>
    /// The budget the final export on graceful shutdown is allowed. Bounded explicitly
    /// rather than allowed to run to completion because the shutdown grace period is
    /// shared with the store's own drain: an export that overruns it is killed
    /// mid-write, and the process that would have protected memory instead delays the
    /// drain that protects everything else.
    /// </summary>
    public TimeSpan StopTimeout { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>Whether an archive directory was configured at all.</summary>
    public bool IsEnabled => !string.IsNullOrWhiteSpace(Directory);

    /// <summary>The export cadence actually in force, with <see cref="MinimumInterval"/> applied.</summary>
    public TimeSpan EffectiveInterval => Interval < MinimumInterval ? MinimumInterval : Interval;

    /// <summary>The stop-time export budget actually in force, clamped to the supported range.</summary>
    public TimeSpan EffectiveStopTimeout
    {
        get
        {
            if (StopTimeout < MinimumStopTimeout)
            {
                return MinimumStopTimeout;
            }

            return StopTimeout > MaximumStopTimeout ? MaximumStopTimeout : StopTimeout;
        }
    }

    /// <summary>
    /// Resolves the archive configuration from the process environment. A malformed or
    /// unrecognised value falls back to the default rather than failing the host, which
    /// is what every other option class in this package does; the effective value is
    /// reported at startup, so a value that did not take is visible rather than silent.
    /// </summary>
    /// <returns>The resolved options.</returns>
    public static RepoContextMemoryArchiveOptions FromEnvironment()
    {
        var defaults = new RepoContextMemoryArchiveOptions();
        var directory = Environment.GetEnvironmentVariable(DirectoryKey);

        return new RepoContextMemoryArchiveOptions
        {
            Directory = string.IsNullOrWhiteSpace(directory) ? null : directory.Trim(),
            Interval = ReadSeconds(IntervalSecondsKey, defaults.Interval),
            RestoreMode = ReadRestoreMode(defaults.RestoreMode),
            StopTimeout = ReadSeconds(StopTimeoutSecondsKey, defaults.StopTimeout),
        };
    }

    private static TimeSpan ReadSeconds(string key, TimeSpan fallback)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        return double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds)
            && seconds > 0
            && !double.IsInfinity(seconds)
                ? TimeSpan.FromSeconds(seconds)
                : fallback;
    }

    private static RepoContextMemoryArchiveRestoreMode ReadRestoreMode(
        RepoContextMemoryArchiveRestoreMode fallback)
    {
        var raw = Environment.GetEnvironmentVariable(RestoreKey);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        return raw.Trim().ToLowerInvariant() switch
        {
            "off" or "none" or "false" => RepoContextMemoryArchiveRestoreMode.Off,
            "auto" or "on-empty" => RepoContextMemoryArchiveRestoreMode.Auto,
            "always" => RepoContextMemoryArchiveRestoreMode.Always,
            _ => fallback,
        };
    }
}
