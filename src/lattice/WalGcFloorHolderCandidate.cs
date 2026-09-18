using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// One durable leaf-materialiser pin offered to the WAL GC's bounded sample of
/// the pins holding a tree's retention floor, carrying both axes the floor is
/// computed on plus the leaf identity the sample is deduplicated by.
/// </summary>
/// <remarks>
/// <para>
/// <b>Both axes travel together because the floor has two of them, and they are
/// held by different pins.</b> <c>LatticeWalGc.ApplyDurableMaterialiserFloorAsync</c>
/// folds the <see cref="Frontier"/> half into the HLC cursor floor, while
/// <c>LatticeWalGc.ComputeMaterialiserOffsetFloorAsync</c> minimises the
/// <see cref="Offset"/> half - skipping every <c>-1</c> - into an independent
/// offset floor. A pin holding one is not in general the pin holding the other,
/// so a sample ranked on a single axis describes the wrong holder whenever the
/// other axis is what the pass is stopping at (issue #3178).
/// </para>
/// <para>
/// <b><see cref="LeafGrainId"/> is what makes a budget mean what it says.</b>
/// A leaf publishes one pin per WAL partition and
/// <c>BPlusLeafGrain.FlushDurableMaterialiserFrontierAsync</c> reads its clock
/// once for the whole batch, so all of a leaf's partition pins carry a
/// byte-identical frontier - structurally, not probabilistically. Keyed by
/// consumer id, a sample of eight on an eight-partition tree is therefore one
/// leaf, and the reactivation touches it licenses all resolve to that same leaf
/// grain. Deduplicating on the leaf is what turns a budget of N into N leaves
/// rather than N/partitions.
/// </para>
/// <para>
/// <see cref="LeafResolved"/> is carried rather than inferred from
/// <see cref="LeafGrainId"/> because an unparseable consumer id has no leaf to
/// be compared on, and must fall back to being its own identity rather than
/// collapsing onto every other unparseable pin.
/// </para>
/// </remarks>
/// <param name="ConsumerId">The durable pin's consumer id, as the pin store keys it.</param>
/// <param name="LeafGrainId">The leaf grain the consumer id resolves to; meaningless unless <paramref name="LeafResolved"/>.</param>
/// <param name="LeafResolved">Whether <paramref name="LeafGrainId"/> was parsed from <paramref name="ConsumerId"/>.</param>
/// <param name="Offset">The pin's durable checkpoint offset, or <c>-1</c> when it constrains no offset floor.</param>
/// <param name="Frontier">The pin's durable frontier clock.</param>
internal readonly record struct WalGcFloorHolderCandidate(
    string ConsumerId,
    GrainId LeafGrainId,
    bool LeafResolved,
    long Offset,
    HybridLogicalClock Frontier)
{
    /// <summary>
    /// Whether <paramref name="other"/> names the same leaf as this candidate,
    /// falling back to consumer-id identity when either side could not be
    /// resolved to a leaf.
    /// </summary>
    /// <param name="other">The candidate to compare leaf identity with.</param>
    /// <returns><see langword="true"/> when both describe the same leaf.</returns>
    internal bool SameLeafAs(in WalGcFloorHolderCandidate other) =>
        LeafResolved && other.LeafResolved
            ? LeafGrainId.Equals(other.LeafGrainId)
            : string.Equals(ConsumerId, other.ConsumerId, StringComparison.Ordinal);

    /// <summary>
    /// Whether this candidate sorts strictly before <paramref name="other"/> in
    /// the sample's ascending order: lowest offset first (the axis
    /// <c>ComputeMaterialiserOffsetFloorAsync</c> minimises), then lowest
    /// frontier, then ordinal consumer id so the sample is stable across sweeps.
    /// </summary>
    /// <param name="other">The candidate to order against.</param>
    /// <returns><see langword="true"/> when this candidate sorts first.</returns>
    internal bool Precedes(in WalGcFloorHolderCandidate other)
    {
        if (Offset != other.Offset)
        {
            return Offset < other.Offset;
        }

        var byFrontier = Frontier.CompareTo(other.Frontier);
        return byFrontier != 0
            ? byFrontier < 0
            : string.CompareOrdinal(ConsumerId, other.ConsumerId) < 0;
    }
}
