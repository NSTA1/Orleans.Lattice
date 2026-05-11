using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Point-in-time snapshot of a single consumer's reported cursor,
/// returned by <see cref="IWalCursorRegistry.SnapshotAsync"/>.
/// </summary>
/// <param name="ConsumerId">Stable identifier for the reporting consumer.</param>
/// <param name="Cursor">Highest <see cref="HybridLogicalClock"/> the consumer has fully consumed. <see cref="HybridLogicalClock.Zero"/> indicates the consumer has never reported a cursor (e.g. a buffer-only consumer that registered through the blocked-floor overload of <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, HybridLogicalClock?, CancellationToken)"/> with <see cref="HybridLogicalClock.Zero"/> for cursor); such consumers are excluded from the GC's HLC <c>min(cursor)</c> half but still contribute to the blocked-floor meet.</param>
/// <param name="LastReportedAtTicks">UTC tick count (<see cref="DateTime.Ticks"/>) of the most recent report for this consumer. Diagnostic only.</param>
/// <param name="Vector">Optional causal-plus vector-clock frontier the consumer has fully consumed, captured by the VC-shaped overload of <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, VersionVector, CancellationToken)"/>. <see langword="null"/> when the consumer reports HLC-only via the legacy overload; such consumers contribute to the HLC <c>min(cursor)</c> half of the GC predicate but are skipped when computing the causal-stable frontier so the GC degrades cleanly to legacy HLC-only behaviour for them.</param>
/// <param name="BlockedAtHlc">Optional lowest <see cref="HybridLogicalClock"/> of any partially-buffered atomic batch the consumer is currently holding. <see langword="null"/> when the consumer has no buffer pin (most consumers — leaf materialisers, peer ship loops — never set this). When at least one consumer reports a non-<see langword="null"/> <see cref="BlockedAtHlc"/>, the GC AND-s a strict-less <c>entry.Timestamp &lt; blockedFloor</c> clause into its trim predicate so the producer cannot trim past an entry the receiver still needs to recover from buffer state. The consumer is the authority on its own pin: each report replaces the previous value (including transitioning back to <see langword="null"/> when the buffer drains).</param>
public readonly record struct WalCursorSnapshot(
    string ConsumerId,
    HybridLogicalClock Cursor,
    long LastReportedAtTicks,
    VersionVector? Vector = null,
    HybridLogicalClock? BlockedAtHlc = null);
