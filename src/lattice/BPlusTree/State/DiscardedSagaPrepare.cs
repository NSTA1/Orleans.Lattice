namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Durable per-leaf marker for a receiver-side saga prepare that was discarded
/// while settling a poisoned saga during re-seed.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.DiscardedSagaPrepare)]
internal sealed class DiscardedSagaPrepare
{
    /// <summary>The saga transaction id whose prepared rows are discarded.</summary>
    [Id(0)] public Guid TransactionId { get; set; }

    /// <summary>
    /// Best-effort WAL offsets, keyed by partition, that replay has seen for
    /// this discarded prepare. Empty means no offset has been observed yet, so
    /// the marker is retained.
    /// </summary>
    [Id(1)] public Dictionary<int, long> PrepareOffsetsByPartition { get; set; } = new();
}
