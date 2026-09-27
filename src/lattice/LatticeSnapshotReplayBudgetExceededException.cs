namespace Orleans.Lattice;

/// <summary>
/// Thrown by <see cref="ILattice.OpenSnapshotKeyCursorAsync"/> /
/// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/> when the frozen
/// baseline captured for the deepest shard the cursor would touch - the rows
/// that shard's snapshot leaf would serve - holds more rows than
/// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/>. The open captures
/// every touched shard's baseline first and applies the gate to the largest
/// row count before the cursor is opened, so operators can cap snapshot open
/// cost without waiting for the first <c>Next*Async</c> call to surface the
/// same problem mid-page. The backup package's capture engine raises it too,
/// before it opens a snapshot, when a backup scope's live entry count exceeds
/// the same budget.
/// <para>
/// The exception aborts the open: no cursor is opened or registered, and the
/// captured baselines, seeded only in memory into transient per-shard snapshot
/// leaves, are never persisted. Callers either raise
/// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/>, narrow the range,
/// trigger a leaf-projection rebuild, or fall back to a registry-snapshot
/// point-in-time cursor.
/// </para>
/// </summary>
public sealed class LatticeSnapshotReplayBudgetExceededException : InvalidOperationException, ILatticeDomainFault
{
    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LatticeSnapshotReplayBudgetExceededException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LatticeSnapshotReplayBudgetExceededException(string message, Exception innerException) : base(message, innerException) { }
}
