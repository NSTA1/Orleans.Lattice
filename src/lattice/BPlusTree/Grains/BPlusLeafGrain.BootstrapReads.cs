using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task<(StateDelta Delta, LeafLiveRawEntriesPage Live)> GetBootstrapCaptureAsync(VersionVector sinceVersion)
    {
        var delta = await GetDeltaSinceAsync(sinceVersion);
        var live = await GetLiveRawEntriesAndNextSiblingAsync();
        return (delta, live);
    }

    /// <inheritdoc />
    public async Task<(List<LwwEntry> Entries, LeafPendingMutationsPage Pending)> GetLiveRawEntriesAndPendingAsync(
        int[] sortedSlots, int virtualShardCount)
    {
        var entries = await GetLiveRawEntriesAsync();
        var pending = await GetPendingMutationsForSlotsAndNextSiblingAsync(sortedSlots, virtualShardCount);
        return (entries, pending);
    }
}
