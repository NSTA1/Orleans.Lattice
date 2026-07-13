namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to register a recurring backup schedule for a scope. The
/// <see cref="Scope"/> selects the region to capture, <see cref="Incremental"/>
/// chooses whether each scheduled cycle captures a full or an incremental
/// backup, and <see cref="Interval"/> is the cadence between captures (clamped up
/// to the scheduler minimum when smaller). A runtime schedule registered this way
/// overrides the configured <see cref="LatticeBackupScheduleOptions"/> cadence for
/// the chosen kind.
/// </summary>
public sealed record LatticeBackupScheduleRequest
{
    /// <summary>Initializes a new <see cref="LatticeBackupScheduleRequest"/>.</summary>
    /// <param name="scope">The region of the tree to capture on each cycle. Must not be <c>null</c>.</param>
    /// <param name="incremental"><c>true</c> to schedule incremental captures, <c>false</c> for full captures.</param>
    /// <param name="interval">The cadence between scheduled captures. Must be strictly positive.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="interval"/> is not strictly positive.</exception>
    public LatticeBackupScheduleRequest(BackupScopeSelector scope, bool incremental, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(interval.Ticks);
        Scope = scope;
        Incremental = incremental;
        Interval = interval;
    }

    /// <summary>The region of the tree each scheduled cycle captures.</summary>
    public BackupScopeSelector Scope { get; init; }

    /// <summary>Whether each scheduled cycle captures an incremental backup rather than a full one.</summary>
    public bool Incremental { get; init; }

    /// <summary>The cadence between scheduled captures.</summary>
    public TimeSpan Interval { get; init; }
}
