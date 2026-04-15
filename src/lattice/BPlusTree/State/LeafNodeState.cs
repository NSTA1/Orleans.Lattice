using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for a leaf grain. Keys are stored in a sorted dictionary
/// wrapped in <see cref="LwwValue{T}"/> for monotonic merge semantics.
/// </summary>
[GenerateSerializer]
public sealed class LeafNodeState
{
    /// <summary>Sorted entries: key → LWW-wrapped value.</summary>
    [Id(0)] public SortedDictionary<string, LwwValue<byte[]>> Entries { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Grain identity of the right sibling leaf (for range scans), or <c>null</c>.</summary>
    [Id(1)] public GrainId? NextSibling { get; set; }

    /// <summary>Monotonic split lifecycle state.</summary>
    [Id(2)] public SplitState SplitState { get; set; }

    /// <summary>If split has occurred, the key at which this node was split.</summary>
    [Id(3)] public string? SplitKey { get; set; }

    /// <summary>If split has occurred, the grain identity of the new right sibling created by the split.</summary>
    [Id(4)] public GrainId? SplitSiblingId { get; set; }

    /// <summary>The current logical clock for this grain.</summary>
    [Id(5)] public HybridLogicalClock Clock { get; set; }

    /// <summary>
    /// Version vector tracking causal history. Each write ticks the local
    /// replica entry, enabling delta extraction for replication.
    /// </summary>
    [Id(6)] public VersionVector Version { get; set; } = new();

    /// <summary>Returns the number of live (non-tombstoned) entries.</summary>
    public int LiveCount => Entries.Count(e => !e.Value.IsTombstone);
}
