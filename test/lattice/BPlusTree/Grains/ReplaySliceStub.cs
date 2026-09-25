using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The one slice-serving stub for <see cref="ILeafReplayCoordinatorGrain"/> in
/// the leaf replay fixtures. It serves both slice overloads from one list of
/// entries, so a leaf that pushes its ownership down with the filtered overload
/// (issue #3565) reads exactly the window a leaf reading unfiltered would.
/// <para>
/// The filtered overload is answered by an independent reference
/// implementation of the rule - examine at most the budget, keep what the
/// filter does not exclude, deliver the last examined entry routing-only when it
/// is excluded - written from the filter's public bounds and bitmap rather than
/// through the production <c>WalFilteredRead</c> or <see cref="WalKeyFilter.Owns"/>.
/// A defect in the production rule therefore cannot hide inside the fixtures
/// that depend on it.
/// </para>
/// </summary>
internal static class ReplaySliceStub
{
    /// <summary>
    /// Configures both <c>ReadSliceAsync</c> overloads of
    /// <paramref name="coordinator"/> to serve <paramref name="served"/>, which
    /// must be ascending by offset.
    /// </summary>
    public static void ServeBothOverloads(ILeafReplayCoordinatorGrain coordinator, IReadOnlyList<CommitLogSliceEntry> served)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(served);

        coordinator.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Unfiltered(served, call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2))));

        coordinator.ReadSliceAsync(
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<WalKeyFilter>(),
                Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Filtered(
                served, call.ArgAt<long>(0), call.ArgAt<long>(1), call.ArgAt<int>(2), call.ArgAt<WalKeyFilter>(3))));
    }

    /// <summary>
    /// The unfiltered slice: entries in <c>(fromExclusive, toInclusive]</c>, at
    /// most <paramref name="budget"/> of them.
    /// </summary>
    public static IReadOnlyList<CommitLogSliceEntry> Unfiltered(
        IReadOnlyList<CommitLogSliceEntry> served,
        long fromExclusive,
        long toInclusive,
        int budget)
    {
        var slice = new List<CommitLogSliceEntry>();
        foreach (var entry in served)
        {
            if (entry.Offset <= fromExclusive)
                continue;
            if (entry.Offset > toInclusive || slice.Count >= budget)
                break;
            slice.Add(entry);
        }

        return slice;
    }

    /// <summary>
    /// The filtered slice, by the reference rule: examine at most
    /// <paramref name="budget"/> entries of the window, keep the ones
    /// <paramref name="filter"/> does not exclude, and deliver the last examined
    /// entry routing-only when it is excluded.
    /// </summary>
    public static IReadOnlyList<CommitLogSliceEntry> Filtered(
        IReadOnlyList<CommitLogSliceEntry> served,
        long fromExclusive,
        long toInclusive,
        int budget,
        WalKeyFilter filter)
    {
        var examined = Unfiltered(served, fromExclusive, toInclusive, budget);
        var slice = new List<CommitLogSliceEntry>();
        for (var i = 0; i < examined.Count; i++)
        {
            var entry = examined[i];
            if (!IsExcluded(entry.Mutation, filter))
            {
                slice.Add(entry);
            }
            else if (i == examined.Count - 1)
            {
                slice.Add(entry with
                {
                    Mutation = new LatticeMutation
                    {
                        TreeId = entry.Mutation.TreeId,
                        Kind = entry.Mutation.Kind,
                        Key = entry.Mutation.Key,
                    },
                });
            }
        }

        return slice;
    }

    private static bool IsExcluded(LatticeMutation mutation, WalKeyFilter filter)
    {
        if (mutation.Kind is not (MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone) || mutation.Key is null)
            return false;

        var key = mutation.Key;
        var inRange = (filter.LowKeyInclusive is null || string.CompareOrdinal(key, filter.LowKeyInclusive) >= 0)
            && (filter.HighKeyExclusive is null || string.CompareOrdinal(key, filter.HighKeyExclusive) < 0);
        if (!inRange)
            return true;

        if (!filter.HasShardConstraint)
            return false;

        var slot = ShardMap.GetVirtualSlot(key, filter.VirtualShardCount);
        return (filter.OwnedSlots[slot / 64] & (1UL << (slot % 64))) == 0;
    }
}
