using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Wires a substitute leaf's combined leaf-walk calls to answer from its
/// separately stubbed calls, so a fixture that stubs
/// <see cref="IBPlusLeafGrain.GetKeyRangeAsync"/> and
/// <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> keeps describing the same
/// chain when production reads both through
/// <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/>. The separate
/// calls are still invoked, so <c>Received</c> counts and <c>When/Do</c> hooks
/// on them keep firing once per leaf visit.
/// </summary>
internal static class LeafSubstituteCombinedCalls
{
    /// <summary>
    /// Composes <see cref="IBPlusLeafGrain.GetKeyRangeAndNextSiblingAsync"/>
    /// from the substitute's own <see cref="IBPlusLeafGrain.GetKeyRangeAsync"/>
    /// and <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> stubs, in the order
    /// and with the tail short-circuit the walk used before the calls were
    /// combined (sibling first; the key range only when there is a successor).
    /// Also composes the payload-plus-link calls
    /// (<see cref="IBPlusLeafGrain.GetDeltaSinceAndNextSiblingAsync"/>,
    /// <see cref="IBPlusLeafGrain.GetLiveRawEntriesAndNextSiblingAsync"/>) from the
    /// payload stub plus that link, and
    /// <see cref="IBPlusLeafGrain.GetKeyRangeAndPrevSiblingAsync"/> from the key-range
    /// and previous-sibling stubs.
    /// </summary>
    public static IBPlusLeafGrain ComposeChainLink(this IBPlusLeafGrain leaf)
    {
        leaf.GetKeyRangeAndNextSiblingAsync().Returns(async _ =>
        {
            var next = await leaf.GetNextSiblingAsync();
            return new LeafChainLink
            {
                KeyRange = next is null ? default : await leaf.GetKeyRangeAsync(),
                NextSibling = next,
            };
        });
        leaf.GetDeltaSinceAndNextSiblingAsync(Arg.Any<VersionVector>()).Returns(async call =>
        {
            var delta = await leaf.GetDeltaSinceAsync(call.Arg<VersionVector>());
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            return new LeafDeltaPage { Delta = delta, NextSibling = link.NextSibling, KeyRange = link.KeyRange };
        });
        leaf.GetLiveRawEntriesAndNextSiblingAsync().Returns(async _ =>
        {
            var entries = await leaf.GetLiveRawEntriesAsync();
            var link = await leaf.GetKeyRangeAndNextSiblingAsync();
            return new LeafLiveRawEntriesPage { Entries = entries, NextSibling = link.NextSibling, KeyRange = link.KeyRange };
        });
        leaf.GetKeyRangeAndPrevSiblingAsync().Returns(async _ => new LeafPrevChainLink
        {
            KeyRange = await leaf.GetKeyRangeAsync(),
            PrevSibling = await leaf.GetPrevSiblingAsync(),
        });
        return leaf;
    }
}
