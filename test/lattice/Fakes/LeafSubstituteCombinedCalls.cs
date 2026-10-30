using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Wires a substitute leaf's combined leaf-walk calls to answer from its
/// separately stubbed calls, so a fixture that stubs
/// <see cref="ILeafSeparateCalls.GetKeyRangeAsync"/> and
/// <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> keeps describing the same
/// chain when production reads both through
/// <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/>. The separate
/// calls are still invoked, so <c>Received</c> counts and <c>When/Do</c> hooks
/// on them keep firing once per leaf visit. Facets that are not grain calls
/// live on <see cref="ILeafSeparateCalls"/>, which a substitute from
/// <see cref="CreateComposed"/> also implements.
/// </summary>
internal static class LeafSubstituteCombinedCalls
{
    /// <summary>
    /// Creates a leaf substitute that also implements <see cref="ILeafSeparateCalls"/>,
    /// with its combined calls composed by <see cref="ComposeChainLink"/>.
    /// </summary>
    public static IBPlusLeafGrain CreateComposed() =>
        Substitute.For<IBPlusLeafGrain, ILeafSeparateCalls>().ComposeChainLink();

    /// <summary>
    /// The separate, non-grain-call facets of a leaf substitute created by
    /// <see cref="CreateComposed"/>, for stubbing and <c>Received</c> checks.
    /// </summary>
    public static ILeafSeparateCalls Separate(this IBPlusLeafGrain leaf) =>
        leaf as ILeafSeparateCalls
        ?? throw new InvalidOperationException(
            "This leaf substitute has no separate facets; create it with LeafSubstituteCombinedCalls.CreateComposed().");

    /// <summary>Preserves existing cache arrangements; call-budget tests override this composition.</summary>
    internal static IBPlusLeafGrain ComposeCacheRefresh(this IBPlusLeafGrain leaf)
    {
        leaf.GetCacheRefreshAsync(Arg.Any<LeafDeliveryCursor>()).Returns(async call =>
        {
            var pending = await leaf.GetPendingKeysAsync();
            return (pending, await leaf.GetDeltaSinceCursorAsync(call.Arg<LeafDeliveryCursor>()));
        });
        return leaf;
    }

    /// <summary>
    /// Composes <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/>
    /// from the substitute's own <see cref="IBPlusLeafGrain.GetKeyRangeAsync"/>
    /// and <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> stubs, in the order
    /// and with the tail short-circuit the walk used before the calls were
    /// combined (sibling first; the key range only when there is a successor).
    /// Also composes the payload-plus-link calls
    /// (<see cref="IBPlusLeafGrain.GetDeltaSinceAndNextSiblingAsync"/>,
    /// <see cref="IBPlusLeafGrain.GetDeltaSinceForSlotsAndNextSiblingAsync"/>,
    /// <see cref="IBPlusLeafGrain.CompactTombstonesAndNextSiblingAsync"/>,
    /// <see cref="IBPlusLeafGrain.GetLiveRawEntriesAndNextSiblingAsync"/>) from the
    /// payload stub plus that link, and
    /// <see cref="IBPlusLeafGrain.GetKeyRangeAndPrevSiblingAsync"/> from the key-range
    /// and previous-sibling stubs. Finally composes
    /// <see cref="IBPlusLeafGrain.DeleteRangeAndNextSiblingAsync"/>,
    /// <see cref="IBPlusLeafGrain.GetClockAndNextSiblingAsync"/> and
    /// <see cref="IBPlusLeafGrain.FreezeProjectionAndNextSiblingAsync"/> from their
    /// payload stubs plus the separately stubbed bounds and sibling pointer.
    /// </summary>
    public static IBPlusLeafGrain ComposeChainLink(this IBPlusLeafGrain leaf)
    {
        leaf.ClearGrainStateAndGetNextSiblingAsync(Arg.Any<bool>()).Returns(async call =>
        {
            var next = await leaf.GetNextSiblingAsync();
            if (call.Arg<bool>())
                await leaf.ClearGrainStateForPurgeAsync();
            else
                await leaf.ClearGrainStateAsync();
            return next;
        });
        leaf.GetKeyRangeAndNextSiblingAsync().Returns(async _ =>
        {
            var next = await leaf.GetNextSiblingAsync();
            return new LeafChainLink
            {
                KeyRange = next is null ? default : await leaf.Separate().GetKeyRangeAsync(),
                NextSibling = next,
            };
        });
        leaf.GetDeltaSinceAndNextSiblingAsync(Arg.Any<VersionVector>()).Returns(async call =>
        {
            var delta = await leaf.GetDeltaSinceAsync(call.Arg<VersionVector>());
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            return new LeafDeltaPage { Delta = delta, NextSibling = link.NextSibling, KeyRange = link.KeyRange };
        });
        leaf.GetDeltaSinceForSlotsAndNextSiblingAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>()).Returns(async call =>
        {
            var delta = await leaf.Separate().GetDeltaSinceForSlotsAsync(call.ArgAt<VersionVector>(0), call.ArgAt<int[]>(1), call.ArgAt<int>(2));
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            return new LeafDeltaPage { Delta = delta, NextSibling = link.NextSibling, KeyRange = link.KeyRange };
        });
        leaf.CompactTombstonesAndNextSiblingAsync(Arg.Any<TimeSpan>(), Arg.Any<HybridLogicalClock?>()).Returns(async call =>
        {
            var gracePeriod = call.ArgAt<TimeSpan>(0);
            var result = call.ArgAt<HybridLogicalClock?>(1) is { } ceiling
                ? await leaf.CompactTombstonesBelowAsync(gracePeriod, ceiling)
                : await leaf.CompactTombstonesAsync(gracePeriod);
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            return new LeafCompactionPage { Result = result, NextSibling = link.NextSibling, KeyRange = link.KeyRange };
        });
        leaf.GetLiveRawEntriesAndNextSiblingAsync().Returns(async _ =>
        {
            var entries = await leaf.GetLiveRawEntriesAsync();
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            return new LeafLiveRawEntriesPage { Entries = entries, NextSibling = link.NextSibling, KeyRange = link.KeyRange };
        });
        leaf.GetKeyRangeAndPrevSiblingAsync().Returns(async _ => new LeafPrevChainLink
        {
            KeyRange = await leaf.Separate().GetKeyRangeAsync(),
            PrevSibling = await leaf.GetPrevSiblingAsync(),
        });
        leaf.DeleteRangeAndNextSiblingAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<LatticePredicateNode?>()).Returns(async call =>
        {
            var result = await leaf.DeleteRangeAsync(call.ArgAt<string>(0), call.ArgAt<string>(1), call.ArgAt<LatticePredicateNode?>(2));
            return new LeafRangeDeletePage
            {
                Result = result,
                KeyRange = await leaf.Separate().GetKeyRangeAsync(),
                NextSibling = await leaf.GetNextSiblingAsync(),
            };
        });
        leaf.GetClockAndNextSiblingAsync().Returns(async _ =>
        {
            var clock = await leaf.GetClockAsync();
            return new LeafClockPage
            {
                Clock = clock,
                KeyRange = await leaf.Separate().GetKeyRangeAsync(),
                NextSibling = await leaf.GetNextSiblingAsync(),
            };
        });
        leaf.GetProjectionCheckpointAndNextSiblingAsync().Returns(async _ =>
        {
            var checkpoint = await leaf.Separate().GetProjectionCheckpointOffsetAsync();
            return new LeafCheckpointPage
            {
                CheckpointOffset = checkpoint,
                Link = await leaf.GetKeyRangeAndNextSiblingAsync(),
            };
        });
        leaf.CountAndNextSiblingAsync(Arg.Any<string?>(), Arg.Any<string?>()).Returns(async call =>
        {
            var count = await leaf.CountAsync(call.ArgAt<string?>(0), call.ArgAt<string?>(1));
            return new LeafCountPage { Count = count, Link = await leaf.GetKeyRangeAndNextSiblingAsync() };
        });
        leaf.GetKeysAndNextSiblingAsync(Arg.Any<string?>(), Arg.Any<string?>()).Returns(async call =>
        {
            var keys = await leaf.GetKeysAsync(call.ArgAt<string?>(0), call.ArgAt<string?>(1));
            return (keys, await leaf.GetKeyRangeAndNextSiblingAsync());
        });
        leaf.RebuildProjectionAndNextSiblingAsync().Returns(async _ =>
        {
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            await leaf.Separate().RebuildProjectionFromWalAsync();
            return link;
        });
        leaf.GetStatsAndNextSiblingAsync().Returns(async _ =>
        {
            var stats = await leaf.GetStatsAsync();
            return new LeafStatsPage { Stats = stats, Link = await leaf.GetKeyRangeAndNextSiblingAsync() };
        });
        leaf.FreezeProjectionAndNextSiblingAsync(Arg.Any<CancellationToken>()).Returns(async call =>
        {
            var freeze = await leaf.Separate().FreezeProjectionAsync(call.Arg<CancellationToken>());
            return new LeafBaselineFreezePage { Freeze = freeze, NextSibling = await leaf.GetNextSiblingAsync() };
        });
        leaf.GetPendingMutationsForSlotsAndNextSiblingAsync(Arg.Any<int[]>(), Arg.Any<int>()).Returns(async call =>
        {
            var mutations = await leaf.GetPendingMutationsForSlotsAsync(call.ArgAt<int[]>(0), call.ArgAt<int>(1));
            var next = await leaf.GetNextSiblingAsync();
            return new LeafPendingMutationsPage { Mutations = mutations, NextSibling = next };
        });
        return leaf;
    }
}
