using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>Default <see cref="ICrossTreePurgeFrontierGrain"/>.</summary>
internal sealed class CrossTreePurgeFrontierGrain(
    IGrainFactory grainFactory,
    ILogger<CrossTreePurgeFrontierGrain> logger,
    [PersistentState("cross-tree-purge-frontier", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreePurgeFrontierState> state) : Grain, ICrossTreePurgeFrontierGrain
{
    /// <inheritdoc />
    public async Task AdvanceAsync(IReadOnlyDictionary<string, long> frontiers)
    {
        ArgumentNullException.ThrowIfNull(frontiers);
        var advanced = new List<string>();
        var previous = new Dictionary<string, long>(state.State.Frontiers, StringComparer.Ordinal);
        foreach (var (tree, frontier) in frontiers)
        {
            if (string.IsNullOrEmpty(tree) || frontier < 0)
            {
                continue;
            }

            if (!state.State.Frontiers.TryGetValue(tree, out var current) || frontier > current)
            {
                state.State.Frontiers[tree] = frontier;
                advanced.Add(tree);
            }
        }

        if (advanced.Count == 0)
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Frontiers = previous;
            throw;
        }

        // Read after the frontier is durable: a tombstone listed before this
        // read is settled here, and one listed after reads the frontier itself.
        var snapshot = new Dictionary<string, long>(state.State.Frontiers, StringComparer.Ordinal);
        foreach (var tree in advanced)
        {
            var index = grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(tree);
            foreach (var key in await index.GetTombstonesAsync())
            {
                try
                {
                    if (!await grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key).SettleTombstoneAsync(snapshot))
                    {
                        await index.RemoveTombstoneAsync(key);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "Settling cross-tree tombstone {Key} listed under tree '{Tree}' failed; retried on the next advance.", key, tree);
                }
            }
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyDictionary<string, long>> GetAsync() =>
        Task.FromResult<IReadOnlyDictionary<string, long>>(new Dictionary<string, long>(state.State.Frontiers, StringComparer.Ordinal));
}
