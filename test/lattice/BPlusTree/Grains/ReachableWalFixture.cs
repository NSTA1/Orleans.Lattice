using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Refuses to build a stubbed WAL partition that production cannot produce
/// (issue #2680).
/// </summary>
/// <remarks>
/// <para>
/// A partition head is EXCLUSIVE: <see cref="ILeafReplayCoordinatorGrain.GetHeadOffsetAsync"/>
/// resolves to <c>WalShardGrain._nextOffset</c>, the sequence the NEXT append
/// will be assigned, which is rebuilt on activation as <c>highest + 1</c>. Every
/// entry persisted when the head is read therefore satisfies
/// <c>0 &lt;= entry.Offset &lt; head</c>, and offsets are unique and strictly
/// ascending because each append takes the next sequence.
/// </para>
/// <para>
/// A fixture that stubs <c>head: 1</c> over an entry at offset 1 is not merely
/// unusual, it is unreachable as a snapshot of the log, and a guard exercised only by such
/// a fixture passes without ever running against the reachable neighbour it
/// claims to cover. #2668 was exactly that. The shared coordinator-stub builders
/// in the <see cref="BPlusLeafGrainTests"/> fixtures route their inputs through
/// <see cref="EnsureReachable"/>, so the unreachable shape is unconstructible
/// rather than merely absent.
/// </para>
/// <para>
/// An entry at or beyond a head IS reachable in one way: appended after the head
/// was read. A fixture that means that must say so by passing those entries as
/// <c>appendedAfterHeadRead</c>, which are held to that shape instead (the first
/// sits exactly at the head, since the next append is assigned the head itself),
/// so the case is explicit rather than an accident of a miscounted head.
/// </para>
/// </remarks>
internal static class ReachableWalFixture
{
    /// <summary>
    /// Throws <see cref="InvalidOperationException"/> unless
    /// <paramref name="entries"/> could be the persisted contents of a WAL
    /// partition whose exclusive head read <paramref name="head"/>, optionally
    /// followed by <paramref name="appendedAfterHeadRead"/>.
    /// </summary>
    /// <param name="head">The stubbed exclusive partition head.</param>
    /// <param name="entries">The entries persisted when the head was read, in the order the stub serves them.</param>
    /// <param name="appendedAfterHeadRead">Entries appended after the head was read, or <see langword="null"/> for none.</param>
    public static void EnsureReachable(
        long head,
        IReadOnlyList<CommitLogSliceEntry> entries,
        IReadOnlyList<CommitLogSliceEntry>? appendedAfterHeadRead = null)
    {
        ArgumentNullException.ThrowIfNull(entries);

        if (head < 0)
        {
            throw new InvalidOperationException(
                $"Unreachable WAL fixture: head {head} is negative. A partition head is the next sequence to be "
                + "assigned and is 0 on an empty partition (issue #2680).");
        }

        var newest = EnsureAscending(entries, previous: -1, "entry");
        if (entries.Count > 0 && newest >= head)
        {
            throw new InvalidOperationException(
                $"Unreachable WAL fixture: the newest entry sits at offset {newest}, at or beyond the EXCLUSIVE "
                + $"partition head {head}. A persisted entry always satisfies offset < head, so this stub needs a "
                + $"head of at least {newest + 1}. An entry appended AFTER the head was read is reachable, but must "
                + "be declared as such (issue #2680).");
        }

        if (appendedAfterHeadRead is { Count: > 0 })
        {
            if (appendedAfterHeadRead[0].Offset != head)
            {
                throw new InvalidOperationException(
                    $"Unreachable WAL fixture: an entry appended after the head was read sits at offset "
                    + $"{appendedAfterHeadRead[0].Offset}, but the next append after a head read of {head} is "
                    + "assigned that head itself (issue #2680).");
            }

            EnsureAscending(appendedAfterHeadRead, newest, "late entry");
        }
    }

    private static long EnsureAscending(IReadOnlyList<CommitLogSliceEntry> entries, long previous, string label)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            var offset = entries[i].Offset;
            if (offset <= previous)
            {
                throw new InvalidOperationException(
                    $"Unreachable WAL fixture: {label} [{i}] at offset {offset} does not follow the previous offset "
                    + $"{previous}. WAL offsets are unique, non-negative and strictly ascending (issue #2680).");
            }

            previous = offset;
        }

        return previous;
    }
}
