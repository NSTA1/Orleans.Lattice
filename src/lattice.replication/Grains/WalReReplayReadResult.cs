using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The result of an <see cref="IWalReReplaySource.ReadAsync(System.Threading.CancellationToken)"/>
/// read: the retained entries plus the trim signal the
/// <see cref="LeafReReplayer"/> engine needs to decide whether the repair can
/// proceed from the local write-ahead-log or must fall back to the
/// operator-only trimmed alert.
/// </summary>
internal readonly record struct WalReReplayReadResult
{
    /// <summary>
    /// The retained entries read across the shard's WAL partitions, in no
    /// guaranteed order. Never <see langword="null"/> on a value produced by a
    /// source; may be empty.
    /// </summary>
    public IReadOnlyList<WalRecord> Entries { get; init; }

    /// <summary>
    /// <see langword="true"/> when at least one partition's oldest retained
    /// entry sat at a sequence greater than zero, i.e. the WAL tail was
    /// garbage-collected.
    /// </summary>
    public bool WasTrimmed { get; init; }

    /// <summary>
    /// The oldest retained clock across the trimmed partitions, or
    /// <see cref="HybridLogicalClock.Zero"/> when nothing was trimmed. When this
    /// is strictly greater than the diverged peer's cursor there is a gap the
    /// WAL can no longer fill, and the repair falls back to the operator-only
    /// trimmed alert.
    /// </summary>
    public HybridLogicalClock OldestRetainedHlc { get; init; }
}
