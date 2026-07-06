namespace Orleans.Lattice.Backup;

/// <summary>
/// Per-scope configuration for scheduled backup triggering and backup-chain
/// retention. Register a named instance - keyed by the scope key returned by
/// <see cref="BackupScopeKey.For(BackupScopeSelector)"/> - to override settings
/// for a specific scope; the unnamed (default) instance applies to every scope
/// that does not have a named override. The scheduler grain resolves the
/// per-scope instance via
/// <c>IOptionsMonitor&lt;LatticeBackupScheduleOptions&gt;.Get(scopeKey)</c>,
/// mirroring the per-tree <see cref="LatticeOptions"/> pattern.
/// <para>
/// Every knob defaults to disabled: scheduling and retention are strictly
/// operator opt-in, so registering the backup package never starts capturing or
/// pruning anything on its own.
/// </para>
/// </summary>
public sealed class LatticeBackupScheduleOptions
{
    /// <summary>
    /// The smallest cadence a schedule reminder honours (the Orleans reminder
    /// minimum). A configured interval smaller than this is clamped up to it
    /// rather than rejected.
    /// </summary>
    public static readonly TimeSpan MinimumInterval = TimeSpan.FromMinutes(1);

    /// <summary>Default value for <see cref="FullBackupInterval"/> (one day).</summary>
    public static readonly TimeSpan DefaultFullBackupInterval = TimeSpan.FromDays(1);

    /// <summary>Default value for <see cref="IncrementalBackupInterval"/> (one hour).</summary>
    public static readonly TimeSpan DefaultIncrementalBackupInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// Whether a recurring full-backup schedule is enabled for the scope.
    /// Default <c>false</c> (operator opt-in).
    /// </summary>
    public bool FullBackupScheduleEnabled { get; set; }

    /// <summary>
    /// Cadence between scheduled full backups. Clamped up to
    /// <see cref="MinimumInterval"/> when the schedule reminder is registered.
    /// Default <see cref="DefaultFullBackupInterval"/>.
    /// </summary>
    public TimeSpan FullBackupInterval { get; set; } = DefaultFullBackupInterval;

    /// <summary>
    /// Whether a recurring incremental-backup schedule is enabled for the scope.
    /// Default <c>false</c> (operator opt-in).
    /// </summary>
    public bool IncrementalBackupScheduleEnabled { get; set; }

    /// <summary>
    /// Cadence between scheduled incremental backups. Clamped up to
    /// <see cref="MinimumInterval"/> when the schedule reminder is registered.
    /// Default <see cref="DefaultIncrementalBackupInterval"/>.
    /// </summary>
    public TimeSpan IncrementalBackupInterval { get; set; } = DefaultIncrementalBackupInterval;

    /// <summary>
    /// Whether backup-chain retention is enabled for the scope. Default
    /// <c>false</c>: no backup is ever pruned unless the operator opts in. When
    /// enabled, retention runs after every scheduled capture and can be invoked
    /// on demand.
    /// </summary>
    public bool RetentionEnabled { get; set; }

    /// <summary>
    /// Keep at most this many of the most recent backups for the scope, or
    /// <c>null</c> to not bound by count. Must be at least 1 when supplied. A
    /// backup is retained if it satisfies this rule <i>or</i>
    /// <see cref="RetentionMaxAge"/>; only a backup failing every enabled rule is
    /// eligible for pruning. The base chain of a retained increment is always
    /// preserved regardless of the count.
    /// </summary>
    public int? RetentionKeepLast { get; set; }

    /// <summary>
    /// Retain backups captured within this window, or <c>null</c> to not bound by
    /// age. Must be strictly positive when supplied. A backup is retained if it
    /// satisfies this rule <i>or</i> <see cref="RetentionKeepLast"/>; the base
    /// chain of a retained increment is always preserved regardless of age.
    /// </summary>
    public TimeSpan? RetentionMaxAge { get; set; }
}
