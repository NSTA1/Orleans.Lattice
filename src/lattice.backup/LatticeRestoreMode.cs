namespace Orleans.Lattice.Backup;

/// <summary>
/// Selects how a backup is installed into its target tree.
/// </summary>
public enum LatticeRestoreMode
{
    /// <summary>
    /// Replays the backup into the target tree in place. An empty target takes the
    /// bottom-up bulk-load fast path; a target that already holds data takes the
    /// last-writer-wins merge path so the restored entries converge with the live
    /// data by hybrid-logical-clock order. Every entry's HLC, version vector, origin
    /// cluster id, expiry, and tombstone flag are installed verbatim, so no causal
    /// history is rewritten.
    /// </summary>
    InPlace = 0,

    /// <summary>
    /// Builds a fresh shadow tree from the backup and atomically swaps the registry
    /// alias so the logical tree id points at the restored shadow. The previous
    /// physical tree is retained (not deleted) so the restore is revertible via
    /// <see cref="ILatticeBackupRestoreService.RevertRestoreAsync"/>. This is the
    /// clean point-in-time-recovery path because it does not fight last-writer-wins
    /// convergence against live data.
    /// </summary>
    ShadowCutover = 1,
}
