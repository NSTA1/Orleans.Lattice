namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One closed hand-off on a marker (epic #5605, member 2): the range it covered and
/// the per-partition WAL heads its close fence returned. An owner that the range
/// overlaps re-arms only after replaying to those heads; every write admitted before
/// the close lies below them, and every later one stamped under the old epoch or
/// generation is rejected.
/// </summary>
/// <remarks>
/// Binary persisted with its marker; every member is a public property of a concrete
/// type so the JSON grain-storage serializer round trips it too (#5579).
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.HandOffClose)]
internal sealed class HandOffClose
{
    /// <summary>The token of the hand-off that closed.</summary>
    [Id(0)]
    public string Token { get; set; } = string.Empty;

    /// <summary>The inclusive low bound of the covered range; <see langword="null"/> for unbounded.</summary>
    [Id(1)]
    public string? LowInclusive { get; set; }

    /// <summary>The exclusive high bound of the covered range; <see langword="null"/> for unbounded.</summary>
    [Id(2)]
    public string? HighExclusive { get; set; }

    /// <summary>The close fence's heads, indexed by WAL partition.</summary>
    [Id(3)]
    public long[] Heads { get; set; } = [];

    /// <summary>The marker's <see cref="ShardHandOffState.Sequence"/> when the close was recorded.</summary>
    [Id(4)]
    public long Sequence { get; set; }
}
