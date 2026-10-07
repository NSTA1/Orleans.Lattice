using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="ICrossTreePurgeFrontierSourceGrain"/>.</summary>
internal sealed class CrossTreePurgeFrontierSourceGrain(
    IGrainFactory grainFactory,
    IReplicatedTreeMembership membership,
    ILogger<CrossTreePurgeFrontierSourceGrain> logger,
    [PersistentState("replication-cross-tree-purge-frontier", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreePurgeFrontierSourceState> state) : Grain, ICrossTreePurgeFrontierSourceGrain
{
    /// <summary>The least interval between two recomputations.</summary>
    internal static TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(5);

    private ImmutableDictionary<string, long> _frontiers = ImmutableDictionary.Create<string, long>(StringComparer.Ordinal);
    private long _refreshedAt = long.MinValue;

    /// <inheritdoc />
    public async Task RegisterTreesAsync(IReadOnlyCollection<string> trees)
    {
        ArgumentNullException.ThrowIfNull(trees);
        var added = trees.Where(t => !string.IsNullOrEmpty(t) && state.State.Trees.Add(t)).ToList();
        if (added.Count == 0)
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Trees.ExceptWith(added);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<ImmutableDictionary<string, long>> GetAsync()
    {
        if (_refreshedAt != long.MinValue
            && Environment.TickCount64 - _refreshedAt < (long)RefreshInterval.TotalMilliseconds)
        {
            return _frontiers;
        }

        var builder = _frontiers.ToBuilder();
        foreach (var tree in state.State.Trees.Union(membership.ReplicatedTrees, StringComparer.Ordinal))
        {
            try
            {
                // The counter first: a sequence confirmed after it is above it.
                var sequence = await grainFactory.GetGrain<ICrossTreeDecisionSequenceGrain>(tree).GetAsync();
                var floor = await TxRegistryFanOut.CrossTreeSequenceFloorAsync(grainFactory, tree);
                var frontier = sequence.Counter;
                if (sequence.MinPending is { } pending) frontier = Math.Min(frontier, pending - 1);
                if (floor is { } stored) frontier = Math.Min(frontier, stored - 1);

                // A purged decision stays purged, so the frontier never moves back.
                if (frontier >= 0 && (!builder.TryGetValue(tree, out var previous) || frontier > previous))
                {
                    builder[tree] = frontier;
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Computing the cross-tree purge frontier of tree '{Tree}' failed; it keeps its last value.", tree);
            }
        }

        _frontiers = builder.ToImmutable();
        _refreshedAt = Environment.TickCount64;
        return _frontiers;
    }
}
