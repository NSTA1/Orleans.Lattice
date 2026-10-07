namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The pure decisions behind a WAL partition's clock floor (issue #4586).
/// A replication shipper's low watermark for a partition is only
/// downward-closed if no write of this cluster stamped below a published floor
/// can be appended after the floor was published. The partition enforces that
/// by refusing such a write; these rules say which writes the floor governs,
/// whether one is admitted, and when the floor advances.
/// </summary>
internal static class WalClockFloorCore
{
    /// <summary>
    /// Whether the floor governs <paramref name="record"/>: a write whose stamp
    /// was freshly minted on this cluster. A carried stamp is exempt, because the
    /// identity it names was first appended fresh, at a lower offset of the same
    /// key's partition (or of another tree's, which the low watermark also
    /// covers): a replicated write of another origin, a merge or backstop copy, a
    /// migrated row (including a saga prepare carried at its original stamp from
    /// another shard), a record stamped under a carried HLC override, and a
    /// tombstone-reap envelope. A zero stamp names no write a dependency can refer
    /// to and is exempt too. A prepare the leaf minted its own original stamp for
    /// (<see cref="WalRecord.PrepareStampOriginal"/> without
    /// <see cref="WalRecord.IsMigrated"/>) is fresh and governed.
    /// </summary>
    /// <param name="record">The record about to be appended.</param>
    /// <param name="localClusterId">This cluster's id for the record's tree, or <see langword="null"/> when none is configured.</param>
    /// <returns><see langword="true"/> when the record must be stamped at or above the floor.</returns>
    public static bool IsSubjectToFloor(in WalRecord record, string? localClusterId)
    {
        if (record.Op == MutationKind.Tombstone
            || record.Timestamp == HybridLogicalClock.Zero
            || record.IsCarriedStamp
            || record.IsMigrated
            || record.IsMerge
            || record.IsBackstop)
        {
            return false;
        }

        var origin = record.OriginClusterId;
        return string.IsNullOrEmpty(origin)
            || string.IsNullOrEmpty(localClusterId)
            || string.Equals(origin, localClusterId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether a partition whose floor is <paramref name="floor"/> admits
    /// <paramref name="record"/>. A zero floor admits everything: the partition
    /// has never published one.
    /// </summary>
    /// <param name="record">The record about to be appended.</param>
    /// <param name="floor">The partition's current floor.</param>
    /// <param name="localClusterId">This cluster's id for the record's tree.</param>
    /// <returns><see langword="true"/> when the record may be assigned an offset.</returns>
    public static bool IsAdmitted(in WalRecord record, HybridLogicalClock floor, string? localClusterId) =>
        floor == HybridLogicalClock.Zero
        || record.Timestamp >= floor
        || !IsSubjectToFloor(in record, localClusterId);

    /// <summary>
    /// The floor a partition aims for at wall-clock time <paramref name="nowUtcTicks"/>:
    /// <paramref name="lag"/> behind it, with a zero logical counter.
    /// </summary>
    /// <param name="nowUtcTicks">The partition's current UTC wall-clock ticks.</param>
    /// <param name="lag">The configured floor lag.</param>
    /// <returns>The target floor.</returns>
    public static HybridLogicalClock Target(long nowUtcTicks, TimeSpan lag) =>
        new() { WallClockTicks = Math.Max(0, nowUtcTicks - lag.Ticks), Counter = 0 };

    /// <summary>
    /// Whether the partition should persist and publish <paramref name="target"/>
    /// in place of <paramref name="current"/>. The floor moves only once it has
    /// fallen half a lag behind its target, which bounds the storage writes to
    /// two per lag per partition while it is being shipped and keeps the floor
    /// between one and one and a half lags behind the wall clock.
    /// </summary>
    /// <param name="current">The floor in force.</param>
    /// <param name="target">The floor <see cref="Target"/> computed now.</param>
    /// <param name="lag">The configured floor lag.</param>
    /// <returns><see langword="true"/> when the floor should advance to <paramref name="target"/>.</returns>
    public static bool ShouldAdvance(HybridLogicalClock current, HybridLogicalClock target, TimeSpan lag) =>
        target > current && target.WallClockTicks - current.WallClockTicks >= lag.Ticks / 2;
}
