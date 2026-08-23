namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free rule that decides which WAL offsets a
/// cursor-advancing reader (the replication shipper, the materialised-view
/// maintainer, leaf replay) may observe while flushes are still in flight.
/// <para>
/// A single WAL shard can have several concurrent in-flight flushes
/// (<see cref="LatticeOptions.WalMaxPendingBatches"/> &gt; 1). Each flush
/// persists its own contiguous, non-overlapping offset window to the underlying
/// provider <b>independently and out of completion order</b>, so a higher-offset
/// window can become durable while a lower-offset window is still in flight -
/// leaving a transient prefix <em>hole</em> in the persisted log. A reader that
/// was handed an offset above such a hole would advance its durable per-partition
/// cursor past the hole and strand the still-in-flight lower entries forever once
/// they finally land, a silent permanent replication / projection gap (the
/// sender-side residual of #1076, reproduced by the cold-partition first-batch
/// data-loss regression).
/// </para>
/// <para>
/// The <b>durable-contiguous tail</b> is the exclusive upper bound of the durable,
/// gap-free offset prefix: every offset strictly below it is durable and forms an
/// unbroken run from the log head. Because offsets are assigned in strictly
/// increasing order and each in-flight flush owns a contiguous window (the
/// in-flight list is ordered oldest-first), the first in-flight window's start
/// offset is precisely the first not-yet-guaranteed-durable offset; with no flush
/// in flight the tail is the next offset to be assigned. Pending (un-flushed)
/// entries always carry higher offsets than any in-flight window, so they cannot
/// introduce a hole below this bound.
/// </para>
/// <para>
/// Extracted from <c>WalShardGrain</c> so the shipping-read seam and the Coyote
/// concurrency model that exhaustively interleaves out-of-order flush completions
/// against a polling reader share one rule with no possibility of drift: the
/// no-hole property the model proves is a property of the code Orleans runs.
/// </para>
/// </summary>
internal static class WalShippingWatermark
{
    /// <summary>
    /// Computes the durable-contiguous tail offset. When
    /// <paramref name="hasInFlight"/> is <see langword="false"/> every assigned
    /// offset has persisted contiguously (or been resynced away by a flush
    /// failure), so the tail is <paramref name="nextOffset"/>. Otherwise the tail
    /// is <paramref name="firstInFlightStartOffset"/> - the start offset of the
    /// oldest in-flight flush, which is the first offset not yet guaranteed to be
    /// durable and contiguous with everything below it.
    /// </summary>
    /// <param name="hasInFlight">Whether any flush is currently in flight.</param>
    /// <param name="firstInFlightStartOffset">
    /// The start offset of the oldest in-flight flush (the first entry of the
    /// oldest-first in-flight window list). Ignored when
    /// <paramref name="hasInFlight"/> is <see langword="false"/>.
    /// </param>
    /// <param name="nextOffset">The next offset that will be assigned on append.</param>
    /// <returns>The exclusive upper bound of the durable, gap-free offset prefix.</returns>
    public static long DurableContiguousTail(bool hasInFlight, long firstInFlightStartOffset, long nextOffset)
        => hasInFlight ? firstInFlightStartOffset : nextOffset;

    /// <summary>
    /// Decides whether <paramref name="offset"/> may be surfaced to a
    /// cursor-advancing reader: only offsets strictly below the
    /// <paramref name="durableContiguousTail"/> are durable-and-contiguous and
    /// therefore safe to expose. An offset at or above the tail may sit above a
    /// transient prefix hole and must be deferred until the hole fills.
    /// </summary>
    /// <param name="offset">The candidate offset.</param>
    /// <param name="durableContiguousTail">
    /// The durable-contiguous tail from <see cref="DurableContiguousTail"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the offset is safe to expose to a
    /// cursor-advancing reader; otherwise <see langword="false"/>.
    /// </returns>
    public static bool IsOffsetExposable(long offset, long durableContiguousTail)
        => offset < durableContiguousTail;
}
