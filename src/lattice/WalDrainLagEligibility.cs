using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// The single predicate deciding whether a registered consumer contributes to a
/// tree's materialiser drain-lag lag plane. Shared by
/// <see cref="InMemoryWalCursorRegistry.GetMinCursorForDrainLagAsync"/>, which
/// produces the aggregate, and the saturation sampler's lagging-consumer count,
/// which decomposes it, so the two can never disagree about who is counted.
/// Lag plane only: the WAL GC trim floor reads every registered consumer.
/// </summary>
/// <remarks>
/// <para>
/// Three exclusions, each a consumer whose distance to the WAL head is not
/// evidence of undrained work:
/// </para>
/// <list type="number">
/// <item><description>A consumer that has never reported a real cursor
/// (<see cref="HybridLogicalClock.Zero"/>), matching the GC cursor meet.</description></item>
/// <item><description>A cold consumer, whose last report is older than the
/// freshness floor (issue #2446).</description></item>
/// <item><description>A leaf-materialiser consumer whose <b>position</b> is stale:
/// it has not advanced since the freshness floor and the position itself is older
/// than the floor (issue #3131). A leaf's cursor is the highest HLC applied to its
/// own key range, while the head is tree-wide, so a leaf whose range has seen no
/// write is caught up however far its cursor trails the head. It also re-reports
/// that persisted position with a fresh report time on every activation, which is
/// why the report-age test alone cannot see it.</description></item>
/// </list>
/// <para>
/// The third exclusion is deliberately confined to leaf materialisers
/// (<see cref="ILeafCursorReporter.MaterialiserConsumerIdPrefix"/>). A tree-wide
/// tailer - a view maintainer, a WAL subscriber, a replication shipper - reports a
/// cursor on the same scope as the head, so when it stops advancing while the head
/// moves on, the gap IS undrained work, and hiding it would silence the only
/// signal that reports a genuinely stuck consumer. A leaf that is actually
/// draining advances on every checkpoint flush and stays counted however far
/// behind it is.
/// </para>
/// <para>
/// A <see langword="long.MinValue"/> floor (freshness disabled) passes every
/// positive cursor, restoring the historical all-consumers input. A snapshot whose
/// <see cref="WalCursorSnapshot.CursorAdvancedAtTicks"/> is <see langword="null"/>
/// comes from a registry that does not track position age and is judged on report
/// age alone.
/// </para>
/// </remarks>
internal static class WalDrainLagEligibility
{
    /// <summary>
    /// Returns whether <paramref name="snapshot"/> contributes to the drain-lag lag
    /// plane under the given freshness floor.
    /// </summary>
    /// <param name="snapshot">The consumer's registry entry.</param>
    /// <param name="reportedAtOrAfterTicks">UTC <see cref="DateTime.Ticks"/> freshness floor; <see cref="long.MinValue"/> disables the freshness exclusions.</param>
    /// <returns><see langword="true"/> when the consumer is counted.</returns>
    internal static bool IsEligible(in WalCursorSnapshot snapshot, long reportedAtOrAfterTicks)
    {
        if (snapshot.Cursor <= HybridLogicalClock.Zero)
        {
            return false;
        }

        if (snapshot.LastReportedAtTicks < reportedAtOrAfterTicks)
        {
            return false;
        }

        if (snapshot.CursorAdvancedAtTicks is not { } advancedAtTicks
            || !IsRangeScoped(snapshot.ConsumerId))
        {
            return true;
        }

        return advancedAtTicks >= reportedAtOrAfterTicks
            || snapshot.Cursor.WallClockTicks >= reportedAtOrAfterTicks;
    }

    /// <summary>
    /// Returns whether <paramref name="consumerId"/> names a leaf-materialiser
    /// cursor, which covers one leaf's key range rather than the whole tree.
    /// </summary>
    /// <param name="consumerId">The registered consumer id.</param>
    /// <returns><see langword="true"/> for a leaf-materialiser consumer id.</returns>
    internal static bool IsRangeScoped(string consumerId)
        => consumerId is not null
            && consumerId.StartsWith(ILeafCursorReporter.MaterialiserConsumerIdPrefix, StringComparison.Ordinal);
}
