namespace Orleans.Lattice.Replication;

/// <summary>
/// The receiver's pure point-write decisions: the receiver-side cycle-break, the
/// snapshot-pinned drop floor, and the monotone high-water-mark advance.
/// <para>
/// Extracted from <see cref="ReplicationApplier"/> (its per-entry and batched paths) and
/// <c>ReplicationHighWaterMarkGrain</c> so the decisions the replication TLA+ module
/// (<c>spec/replication/Replication.tla</c>, action <c>Deliver</c>) and the replication Coyote
/// models check are the ones the receiver runs. Every member is allocation-free and runs once
/// per applied entry.
/// </para>
/// </summary>
internal static class ReplicationReceiveDedup
{
    /// <summary>
    /// Whether an inbound entry was authored by the receiving cluster itself. Such an entry is a
    /// dedup no-op: this is the receiver's only enforcement that a write is never applied back
    /// onto its author, independent of what the sending cluster's build filters.
    /// </summary>
    /// <param name="entryOriginClusterId">The entry's origin cluster id.</param>
    /// <param name="localClusterId">The receiving cluster's own id.</param>
    /// <returns><see langword="true"/> when the entry must not be applied.</returns>
    public static bool IsOwnOrigin(string? entryOriginClusterId, string? localClusterId) =>
        string.Equals(entryOriginClusterId, localClusterId, StringComparison.Ordinal);

    /// <summary>
    /// Whether a point write is dropped by the snapshot-pinned floor: its source HLC is at or
    /// below the floor pinned for its origin, outside a bootstrap drain and outside a saga
    /// prepare phase. The incremental high-water mark is deliberately not an input: per-leaf
    /// clocks are unordered, so a write below it is routinely new (#1060).
    /// </summary>
    /// <param name="timestamp">The entry's source HLC.</param>
    /// <param name="pinnedFloor">The floor pinned for the entry's origin.</param>
    /// <param name="isBootstrapDrain">Whether the entry is applied by a bootstrap drain.</param>
    /// <param name="isPreparedAtomicBatch">Whether the entry is a saga prepare-phase entry.</param>
    /// <returns><see langword="true"/> when the entry is dropped as already held.</returns>
    public static bool IsCoveredByPinnedFloor(
        HybridLogicalClock timestamp,
        HybridLogicalClock pinnedFloor,
        bool isBootstrapDrain,
        bool isPreparedAtomicBatch) =>
        !isBootstrapDrain
        && !isPreparedAtomicBatch
        && timestamp.CompareTo(pinnedFloor) <= 0;

    /// <summary>
    /// Whether a candidate advances a per-origin high-water mark: the mark only ever takes the
    /// maximum, so a re-delivered or out-of-order entry never lowers it.
    /// </summary>
    /// <param name="current">The current mark.</param>
    /// <param name="candidate">The applied entry's source HLC.</param>
    /// <returns><see langword="true"/> when the mark moves to <paramref name="candidate"/>.</returns>
    public static bool AdvancesHighWaterMark(HybridLogicalClock current, HybridLogicalClock candidate) =>
        candidate.CompareTo(current) > 0;
}
