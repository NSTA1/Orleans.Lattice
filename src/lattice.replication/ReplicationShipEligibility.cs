using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The shipper's pure eligibility decisions: which WAL entries a cluster may ship to a peer
/// (the local-origin cycle-break) and when the scalar HLC cursor may still skip an entry.
/// <para>
/// Extracted from <c>ReplicationShipperGrain</c> so the decision the replication TLA+ module
/// (<c>spec/replication/Replication.tla</c>, actions <c>ShipSkip</c> and <c>Deliver</c>) and the
/// replication Coyote models check is the one the shipper runs. Every member is allocation-free
/// and run once per drained WAL entry.
/// </para>
/// </summary>
internal static class ReplicationShipEligibility
{
    /// <summary>
    /// Whether an entry clears the shipper's origin and operation clauses: it carries a non-empty
    /// origin, it is not a tombstone-reap maintenance record, and it was authored by
    /// <paramref name="localClusterId"/>. Only local-origin entries ship, so an entry this cluster
    /// applied from a peer is never re-shipped - neither back to its author (reflection) nor on to
    /// a third cluster (relay). Key filters are applied by the caller afterwards.
    /// </summary>
    /// <param name="originClusterId">The entry's <see cref="WalRecord.OriginClusterId"/>.</param>
    /// <param name="op">The entry's <see cref="WalRecord.Op"/>.</param>
    /// <param name="localClusterId">The shipping cluster's own id.</param>
    /// <returns><see langword="true"/> when the entry may ship.</returns>
    public static bool IsShipEligible(string? originClusterId, MutationKind op, string? localClusterId)
    {
        if (string.IsNullOrEmpty(originClusterId))
        {
            return false;
        }

        if (op == MutationKind.Tombstone)
        {
            return false;
        }

        return string.Equals(originClusterId, localClusterId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether this drain tick is the one-time legacy-migration tick: a state persisted by a
    /// pre-partition-cursor build carries a non-zero scalar cursor but no partition cursors, so
    /// every partition would resume from sequence 0 and re-ship the already-shipped prefix. Only
    /// on this tick may <see cref="IsBelowLegacyScalarCursor"/> drop on the scalar cursor.
    /// </summary>
    /// <param name="scalarCursor">The shipper's persisted scalar HLC cursor.</param>
    /// <param name="partitionCursorCount">How many partition cursors the shipper has saved.</param>
    /// <returns><see langword="true"/> on the legacy-migration tick.</returns>
    public static bool IsLegacyMigrationTick(HybridLogicalClock scalarCursor, int partitionCursorCount) =>
        scalarCursor != HybridLogicalClock.Zero && partitionCursorCount == 0;

    /// <summary>
    /// Whether the scalar HLC cursor drops an entry the partition merge has already consumed.
    /// Only the one-time legacy-migration tick (a state persisted with a non-zero scalar cursor
    /// and no partition cursors) may drop on the scalar cursor; from the first saved partition
    /// cursor onward an entry below the scalar cursor is routinely a genuinely new write on
    /// another leaf's clock and must ship (#1060). Range deletes (zero HLC) and saga prepare-phase
    /// entries are never dropped, because their HLCs are not monotonic either.
    /// </summary>
    /// <param name="legacyMigrationPending">Whether this tick is the legacy-migration tick.</param>
    /// <param name="isPreparedAtomicBatch">Whether the entry is a saga prepare-phase entry.</param>
    /// <param name="timestamp">The entry's source HLC.</param>
    /// <param name="scalarCursor">The shipper's scalar HLC cursor.</param>
    /// <returns><see langword="true"/> when the entry is dropped as already shipped.</returns>
    public static bool IsBelowLegacyScalarCursor(
        bool legacyMigrationPending,
        bool isPreparedAtomicBatch,
        HybridLogicalClock timestamp,
        HybridLogicalClock scalarCursor) =>
        legacyMigrationPending
        && !isPreparedAtomicBatch
        && timestamp != HybridLogicalClock.Zero
        && timestamp.CompareTo(scalarCursor) <= 0;
}
