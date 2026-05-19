namespace Orleans.Lattice;

/// <summary>
/// Thrown by <see cref="ILattice.OpenSnapshotKeyCursorAsync"/> /
/// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/> when the
/// projected WAL-replay cost of materialising the snapshot's leaves
/// would exceed <see cref="LatticeOptions.MaxSnapshotReplayEntries"/>
/// on any of the shards the cursor would touch. The budget is sized
/// upfront using <see cref="ILattice.GetMaterialiserLagAsync"/> as
/// the per-shard lag signal, so operators can cap snapshot open
/// cost without waiting for the first <c>Next*Async</c> call to
/// surface the same problem mid-page.
/// <para>
/// The exception aborts the open: no snapshot leaves are
/// materialised and no WAL retention pin is taken. Callers either
/// raise <see cref="LatticeOptions.MaxSnapshotReplayEntries"/>, fall
/// back to a registry-snapshot point-in-time cursor, or wait for
/// the per-shard materialiser to catch up before retrying.
/// </para>
/// </summary>
public sealed class LatticeSnapshotReplayBudgetExceededException : InvalidOperationException
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
