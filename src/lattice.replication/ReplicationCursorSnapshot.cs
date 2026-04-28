using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Point-in-time snapshot of a single consumer's reported cursor,
/// returned by <see cref="ILatticeReplicationCursorRegistry.SnapshotAsync"/>.
/// </summary>
/// <param name="ConsumerId">Stable identifier for the reporting consumer.</param>
/// <param name="Cursor">Highest <see cref="HybridLogicalClock"/> the consumer has fully consumed.</param>
/// <param name="LastReportedAtTicks">UTC tick count (<see cref="DateTime.Ticks"/>) of the most recent report for this consumer. Diagnostic only.</param>
/// <param name="Vector">Optional causal-plus vector-clock frontier the consumer has fully consumed, captured by the VC-shaped <see cref="ILatticeReplicationCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, VersionVector, CancellationToken)"/> overload. <see langword="null"/> when the consumer reports HLC-only via the legacy overload; such consumers contribute to the HLC <c>min(cursor)</c> half of the GC predicate but are skipped when computing the causal-stable frontier so the GC degrades cleanly to legacy HLC-only behaviour for them.</param>
public readonly record struct ReplicationCursorSnapshot(
    string ConsumerId,
    HybridLogicalClock Cursor,
    long LastReportedAtTicks,
    VersionVector? Vector = null);
