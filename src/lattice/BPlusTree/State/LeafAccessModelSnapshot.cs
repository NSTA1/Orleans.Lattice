using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Compact, bounded persisted form of a shard root's leaf-access histogram.
/// Rides inside <see cref="ShardRootState"/> so the model survives the shard
/// root's deactivation and the silo restart that follows, which is the whole
/// point of pre-warming: a model rebuilt from scratch on every activation would
/// have nothing to say at the exact moment it is needed.
/// </summary>
/// <remarks>
/// <para>
/// <b>Shape.</b> Two parallel lists: <see cref="Leaves"/> holds leaf grain
/// identities in their round-trippable string form, and <see cref="Visits"/>
/// holds each leaf's observed read count. Entries are written most-visited
/// first, so a truncated read still carries the hottest leaves.
/// </para>
/// <para>
/// <b>Bound.</b> The producer caps the snapshot at
/// <c>LeafAccessFrequencyModel.MaxPersistedLeaves</c> (64) leaves. The
/// worst-case payload is therefore 64 grain identities plus 64 <c>long</c>
/// counts - on the order of 3 KB serialised, and independent of the tree's key
/// space.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LeafAccessModelSnapshot)]
[Immutable]
internal sealed record LeafAccessModelSnapshot
{
    /// <summary>
    /// Leaf grain identities, in <see cref="GrainId.ToString"/> form, ordered
    /// most-visited first. Parallel to <see cref="Visits"/>.
    /// </summary>
    [Id(0)] public required List<string> Leaves { get; init; }

    /// <summary>
    /// Number of reads observed to land on each leaf, parallel to
    /// <see cref="Leaves"/>. A snapshot whose two lists disagree in length is
    /// read up to the shorter of the two rather than discarded.
    /// </summary>
    [Id(1)] public required List<long> Visits { get; init; }

    /// <summary>The canonical empty snapshot - no leaves observed yet.</summary>
    public static LeafAccessModelSnapshot Empty { get; } = new() { Leaves = [], Visits = [] };
}
