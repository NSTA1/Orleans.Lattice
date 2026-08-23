namespace Orleans.Lattice;

/// <summary>
/// Pure, allocation-free decision core for the write-ahead-log garbage
/// collector's <b>blocked-floor</b> meet - the lowest buffer-pin
/// <see cref="HybridLogicalClock"/> across every consumer that is currently
/// holding a partially-buffered atomic batch. Extracted from
/// <see cref="InMemoryWalCursorRegistry"/> so the exact production fold can be
/// driven under systematic (Coyote) interleaving without a silo: a violation the
/// model finds is a violation of the real blocked-floor path.
/// </summary>
/// <remarks>
/// <para>
/// A buffering receiver reports the HLC of the oldest entry it has staged but not
/// yet committed (its <c>BlockedAtHlc</c> pin); the GC must not trim any entry at
/// or after the <em>minimum</em> such pin across consumers, so the receiver can
/// still recover from buffer state (<see cref="WalGcTrimCore.IsEntryEligible"/>'s
/// blocked-floor clause). Each consumer owns its own pin and moves it through a
/// lifecycle - taking it (null to a value), raising it as the buffer drains
/// (value to a higher value), and clearing it when the buffer empties (value back
/// to null) - all racing concurrent GC floor reads.
/// </para>
/// <para>
/// The load-bearing property is that the floor is the <b>meet (minimum)</b> of
/// the live pins, never the join (maximum): a floor computed as the maximum would
/// sit above a slower consumer's live pin and let the GC trim an entry that
/// consumer still needs to drain its buffer. <see cref="Meet"/> is that single
/// fold step, applied over every consumer's current pin exactly as the registry
/// applies it under its gate.
/// </para>
/// </remarks>
internal static class WalBlockedFloorCore
{
    /// <summary>
    /// Folds one consumer's buffer pin into the running blocked-floor meet: a
    /// <see langword="null"/> pin (the consumer is not buffering) leaves the meet
    /// unchanged, and a non-<see langword="null"/> pin lowers the meet whenever it
    /// is strictly below the running value (or seeds it when none is set yet). The
    /// registry seeds the fold with <see langword="null"/> and applies this to
    /// every consumer snapshot, so the result is the minimum pin across the
    /// buffering consumers, or <see langword="null"/> when none is buffering.
    /// </summary>
    /// <param name="runningFloor">
    /// The meet accumulated so far, or <see langword="null"/> before any buffering
    /// consumer has been folded in.
    /// </param>
    /// <param name="consumerPin">
    /// The consumer's current buffer pin, or <see langword="null"/> when it is not
    /// holding a partially-buffered batch.
    /// </param>
    /// <returns>The updated blocked-floor meet.</returns>
    public static HybridLogicalClock? Meet(HybridLogicalClock? runningFloor, HybridLogicalClock? consumerPin)
    {
        if (consumerPin is not { } pin)
        {
            return runningFloor;
        }

        if (runningFloor is not { } floor || pin < floor)
        {
            return pin;
        }

        return floor;
    }
}
