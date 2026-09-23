namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Per-shard coordination state an
/// <see cref="AzureTableWalStorageProvider"/> keeps so reconciliation
/// never runs against the shard's own in-motion writes (#3348), and so
/// an append cannot overlap a batch written at a different start offset.
/// </summary>
internal sealed class WalShardActivity
{
    /// <summary>
    /// Append calls (phase 0, phase 1, and the phase-2 hand-off) in
    /// motion against the shard.
    /// </summary>
    internal WalShardWriteTracker Writes { get; } = new();

    /// <summary>
    /// Serialises <see cref="AzureTableWalStorageProvider.ReconcileAsync"/>
    /// passes against the shard, so two failure-path resyncs cannot
    /// plan against each other's half-applied commits.
    /// </summary>
    internal SemaphoreSlim ReconcileGate { get; } = new(1, 1);

    /// <summary>
    /// Rejects appends that overlap a batch starting at a different
    /// offset, without a storage round-trip on the in-order path.
    /// </summary>
    internal WalShardOverlapGuard Overlap { get; } = new();
}