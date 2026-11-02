namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One open hand-off on a marker (epic #5605, member 2): the token that names the
/// hand-off, and the key range it covers. A leaf whose range is disjoint from it
/// ignores it, by the same overlap test the replay judge uses for a range delete.
/// </summary>
/// <remarks>
/// Part of <see cref="ShardHandOffState"/> and <see cref="LeafHandOffState"/>, so it
/// is binary persisted with them; every member is a public property of a concrete
/// type so the JSON grain-storage serializer round trips it too (#5579).
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.HandOffMarkerRange)]
internal sealed class HandOffMarkerRange
{
    /// <summary>The hand-off's token, unique among the marker's open hand-offs.</summary>
    [Id(0)]
    public string Token { get; set; } = string.Empty;

    /// <summary>The inclusive low bound of the covered range; <see langword="null"/> for unbounded.</summary>
    [Id(1)]
    public string? LowInclusive { get; set; }

    /// <summary>The exclusive high bound of the covered range; <see langword="null"/> for unbounded.</summary>
    [Id(2)]
    public string? HighExclusive { get; set; }
}
