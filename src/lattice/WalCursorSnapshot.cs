using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Point-in-time snapshot of a single consumer's reported cursor,
/// returned by <see cref="IWalCursorRegistry.SnapshotAsync"/>.
/// </summary>
/// <param name="ConsumerId">Stable identifier for the reporting consumer.</param>
/// <param name="Cursor">Highest <see cref="HybridLogicalClock"/> the consumer has fully consumed. <see cref="HybridLogicalClock.Zero"/> indicates the consumer has never reported a cursor (e.g. a buffer-only consumer that registered through the blocked-floor overload of <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, HybridLogicalClock?, CancellationToken)"/> with <see cref="HybridLogicalClock.Zero"/> for cursor); such consumers are excluded from the GC's HLC <c>min(cursor)</c> half but still contribute to the blocked-floor meet.</param>
/// <param name="LastReportedAtTicks">UTC tick count (<see cref="DateTime.Ticks"/>) of the most recent report for this consumer. The WAL GC trim floor ignores this value and remains conservative over every registered consumer; the saturation classifier's drain-lag input uses it only to exclude cold consumers from the lag-plane pacing signal.</param>
/// <param name="Vector">Optional causal-plus vector-clock frontier the consumer has fully consumed, captured by the VC-shaped overload of <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, VersionVector, CancellationToken)"/>. <see langword="null"/> when the consumer reports HLC-only via the legacy overload; such consumers contribute to the HLC <c>min(cursor)</c> half of the GC predicate but are skipped when computing the causal-stable frontier so the GC degrades cleanly to legacy HLC-only behaviour for them.</param>
/// <param name="BlockedAtHlc">Optional lowest <see cref="HybridLogicalClock"/> of any partially-buffered atomic batch the consumer is currently holding. <see langword="null"/> when the consumer has no buffer pin (most consumers - leaf materialisers, peer ship loops - never set this). When at least one consumer reports a non-<see langword="null"/> <see cref="BlockedAtHlc"/>, the GC AND-s a strict-less <c>entry.Timestamp &lt; blockedFloor</c> clause into its trim predicate so the producer cannot trim past an entry the receiver still needs to recover from buffer state. The consumer is the authority on its own pin: each report replaces the previous value (including transitioning back to <see langword="null"/> when the buffer drains).</param>
public readonly record struct WalCursorSnapshot(
    string ConsumerId,
    HybridLogicalClock Cursor,
    long LastReportedAtTicks,
    VersionVector? Vector = null,
    HybridLogicalClock? BlockedAtHlc = null)
{
    /// <summary>
    /// UTC tick count (<see cref="DateTime.Ticks"/>) of the most recent report that
    /// strictly <b>advanced</b> <see cref="Cursor"/>, as opposed to
    /// <see cref="LastReportedAtTicks"/>, which moves on every report including a
    /// re-report of an unchanged position. <c>0</c> when the registry has not seen the
    /// position move since the consumer registered (the registering report only asserts
    /// a position, it does not show the consumer draining). <see langword="null"/> when
    /// the registry that produced the snapshot does not track position age, in which
    /// case the saturation classifier falls back to <see cref="LastReportedAtTicks"/>
    /// alone.
    /// <para>
    /// The saturation classifier's drain-lag input reads this for leaf-materialiser
    /// consumers only (issue #3131). A leaf's cursor is the highest HLC applied to its
    /// own key range, so a leaf whose range has received no write keeps an old cursor
    /// while fully caught up, and re-reports that persisted position with a fresh
    /// <see cref="LastReportedAtTicks"/> on every activation. For such a consumer the
    /// distance to the tree-wide WAL head is not undrained work. The WAL GC trim floor
    /// ignores this value.
    /// </para>
    /// </summary>
    public long? CursorAdvancedAtTicks { get; init; }
}
