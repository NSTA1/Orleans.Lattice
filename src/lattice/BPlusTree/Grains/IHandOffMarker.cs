using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The shape <see cref="HandOffMarkerCore"/> works on (epic #5605, member 2):
/// implemented by both <see cref="LeafHandOffState"/> and <see cref="ShardHandOffState"/>.
/// </summary>
internal interface IHandOffMarker
{
    /// <summary>The open hand-offs, by token.</summary>
    Dictionary<string, HandOffMarkerRange> Open { get; }

    /// <summary>The retained closes, oldest first.</summary>
    List<HandOffClose> Closes { get; }

    /// <summary>The per-partition maximum of every evicted close's heads.</summary>
    long[] CompactedHeads { get; set; }

    /// <summary>Raised on every open and close.</summary>
    long Sequence { get; set; }
}
