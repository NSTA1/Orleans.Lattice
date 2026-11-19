namespace Orleans.Lattice.Backup;

/// <summary>
/// Which physical copies the shadow-cutover restores of one logical tree have left
/// behind, which of them a revert can still reach, and which are retired because
/// none can (issue #6103). Returned by
/// <see cref="ILatticeBackupRestoreService.AuditRestoreCopiesAsync"/> and
/// <see cref="ILatticeBackupRestoreService.RepairRestoreCopiesAsync"/>.
/// </summary>
public sealed record LatticeRestoreCopyAudit
{
    /// <summary>The logical tree audited.</summary>
    public required string TreeId { get; init; }

    /// <summary>The physical tree the logical tree currently resolves to. Never retired.</summary>
    public required string AliasTargetPhysicalTreeId { get; init; }

    /// <summary>
    /// The copy the current restore's revert returns to, which is kept; <see langword="null"/>
    /// when the current alias target has no revertible restore, or the copy is unknown
    /// (<see cref="LatticeRestoreCopyHold.UnknownRevertTarget"/>).
    /// </summary>
    public string? RevertTargetPhysicalTreeId { get; init; }

    /// <summary>
    /// When the restore that can still be reverted committed, if Lattice recorded it;
    /// its revert window, when one is configured, runs from here.
    /// </summary>
    public DateTimeOffset? RevertCommittedAtUtc { get; init; }

    /// <summary>
    /// Every live restore copy of the tree - each registered restore shadow, and the
    /// tree's own original shards once a restore replaced them - that no revert can
    /// reach and that has not yet been retired. Empty when <see cref="Hold"/> is not
    /// <see cref="LatticeRestoreCopyHold.None"/>.
    /// </summary>
    public IReadOnlyList<string> RetirablePhysicalTreeIds { get; init; } = [];

    /// <summary>
    /// Every registered restore shadow of the tree, live or already retired and
    /// awaiting its purge, in ordinal order.
    /// </summary>
    public IReadOnlyList<string> RestoreShadowPhysicalTreeIds { get; init; } = [];

    /// <summary>Why nothing is retired, or <see cref="LatticeRestoreCopyHold.None"/>.</summary>
    public LatticeRestoreCopyHold Hold { get; init; }
}
