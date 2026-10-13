using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Reads a fresh topology entry and derives its options without a second
/// registry round trip when its structural pins are complete.
/// </summary>
internal static class RegistryEntryOptions
{
    /// <summary>
    /// Preserves lazy structural seeding for legacy or absent rows, then reads
    /// topology afresh rather than using a coalesced options read for routing.
    /// </summary>
    internal static async Task<(TreeRegistryEntry? Entry, ResolvedLatticeOptions Options)> ReadAsync(
        ILatticeRegistry registry, LatticeOptionsResolver resolver, string treeId)
    {
        var entry = await registry.GetEntryAsync(treeId);
        if (!treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            && (entry?.MaxLeafKeys is null || entry.MaxInternalChildren is null || entry.ShardCount is null))
        {
            await resolver.ResolveAsync(treeId);
            entry = await registry.GetEntryAsync(treeId);
        }

        return (entry, resolver.Resolve(treeId, entry));
    }
}
