namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task<LeafCountPage> CountAndNextSiblingAsync(string? startInclusive, string? endExclusive)
    {
        var count = await CountAsync(startInclusive, endExclusive);
        return new LeafCountPage
        {
            Count = count,
            Link = new LeafChainLink { KeyRange = CurrentKeyRange(), NextSibling = state.State.NextSibling },
        };
    }

    /// <inheritdoc />
    public async Task<LeafStatsPage> GetStatsAndNextSiblingAsync()
    {
        var stats = await GetStatsAsync();
        return new LeafStatsPage
        {
            Stats = stats,
            Link = new LeafChainLink { KeyRange = CurrentKeyRange(), NextSibling = state.State.NextSibling },
        };
    }
}
