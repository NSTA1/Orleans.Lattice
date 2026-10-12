using NSubstitute;
using Orleans.Lattice.BPlusTree;

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
        return leaf;
    }
}
