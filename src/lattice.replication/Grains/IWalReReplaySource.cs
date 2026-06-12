namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A read-only seam over the retained local write-ahead-log entries that a
/// targeted leaf re-replay repair pass selects from. Abstracting the read
/// behind this interface keeps the <see cref="LeafReReplayer"/> engine
/// unit-testable with an in-memory stub; the production implementation is
/// <see cref="WalGrainReReplaySource"/>.
/// </summary>
internal interface IWalReReplaySource
{
    /// <summary>
    /// Reads the retained write-ahead-log entries available for re-replay,
    /// reporting whether the tail was trimmed and (if so) the oldest retained
    /// clock so the engine can detect a garbage-collected-past-divergence gap.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    ValueTask<WalReReplayReadResult> ReadAsync(CancellationToken cancellationToken);
}
