namespace Orleans.Lattice.Backup;

/// <summary>
/// The declared conflict-resolution shape of a captured key, sourced from the
/// durable log record so a restore can re-apply the value faithfully. A backup is
/// mode-faithful: a last-writer-wins value and a CRDT value are re-applied through
/// different paths on restore.
/// </summary>
public enum BackupKeyMergeMode
{
    /// <summary>An opaque last-writer-wins register value.</summary>
    LastWriterWins = 0,

    /// <summary>A convergent replicated data type whose merge is algebraic.</summary>
    Crdt = 1,
}
