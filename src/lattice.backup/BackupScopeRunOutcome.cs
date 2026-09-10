namespace Orleans.Lattice.Backup;

/// <summary>
/// The terminal outcome of the most recent capture cycle a per-scope backup
/// scheduler ran. Persisted on the scheduler state and surfaced through the admin
/// status surface so an operator can see at a glance whether a scope's last
/// backup succeeded or failed without scraping metrics.
/// </summary>
public enum BackupScopeRunOutcome
{
    /// <summary>No capture cycle has run for the scope yet.</summary>
    None = 0,

    /// <summary>The most recent capture cycle completed successfully.</summary>
    Success = 1,

    /// <summary>The most recent capture cycle faulted.</summary>
    Failure = 2,

    /// <summary>
    /// The most recent capture cycle was refused by the access gate. Recorded
    /// distinctly from <see cref="Failure"/> so a denial on a gated host reads as
    /// a denial rather than as a generic fault or, worse, as "nothing to back up"
    /// (issue #2608).
    /// </summary>
    Denied = 3,
}
