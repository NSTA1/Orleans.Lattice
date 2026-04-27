using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Point-in-time snapshot of a single consumer''s reported cursor,
/// returned by <see cref="ILatticeReplicationCursorRegistry.SnapshotAsync"/>.
/// </summary>
/// <param name="ConsumerId">Stable identifier for the reporting consumer.</param>
/// <param name="Cursor">Highest <see cref="HybridLogicalClock"/> the consumer has fully consumed.</param>
/// <param name="LastReportedAtTicks">UTC tick count (<see cref="DateTime.Ticks"/>) of the most recent report for this consumer. Diagnostic only.</param>
public readonly record struct ReplicationCursorSnapshot(
    string ConsumerId,
    HybridLogicalClock Cursor,
    long LastReportedAtTicks);
