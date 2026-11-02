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
    public async Task<(int LiveCount, string? HighestLiveKey)> GetAppendTailAsync()
    {
        await AwaitReplayBarrierAsync();

        // The live keys come back ascending, so the count and the right edge are
        // read from one list in one turn: a merge cannot land between them.
        var keys = await GetKeysAsync();
        return (keys.Count, keys.Count == 0 ? null : keys[^1]);
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
