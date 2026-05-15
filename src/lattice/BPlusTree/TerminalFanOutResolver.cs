namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Pre-resolves the transitive closure of split-forward destinations
/// for a seed set of shard indices, so callers can fan saga terminal
/// marks out flat (in parallel) instead of relying on the receiving
/// shard root to recursively forward the terminal to its own split
/// destinations. The pre-resolution moves the BFS into the saga (or
/// the cross-cluster replication-apply path) where the cancellation
/// budget is owned by the broadcast loop, bounding wall-clock under
/// cascading mid-saga splits - a recursive per-shard forward depth
/// of N collapses to a single-hop parallel fan-out at the caller, so
/// deep multi-hop reshard chains no longer compound per-shard RPC
/// time into Orleans' default response timeout.
/// </summary>
/// <remarks>
/// The expansion is breadth-first: every shard in the current
/// frontier is queried in parallel for its
/// <see cref="IShardRootGrain.GetSplitForwardTargetsAsync"/>, the
/// results are unioned against the visited set, and any newly-seen
/// destinations form the next frontier. The visited set itself acts
/// as the cycle guard - a target that's already been expanded is
/// never re-queued, even if it appears as a destination of multiple
/// upstream shards. The returned list is sorted ascending so the
/// caller's fan-out iteration is order-deterministic.
/// </remarks>
internal static class TerminalFanOutResolver
{
    /// <summary>
    /// Returns the transitive closure of <paramref name="seed"/>
    /// expanded by each shard's
    /// <see cref="IShardRootGrain.GetSplitForwardTargetsAsync"/>. The
    /// returned list is sorted ascending and includes every seed
    /// shard plus every shard reachable through any chain of
    /// <see cref="State.ShardRootState.SplitInProgress"/> /
    /// <see cref="State.ShardRootState.MovedAwaySlots"/> records.
    /// </summary>
    /// <param name="grainFactory">Grain factory for resolving each
    /// shard root by its <c>{treeId}/{shardIndex}</c> grain key.</param>
    /// <param name="physicalTreeId">The physical tree id segment of
    /// the shard-root grain key - i.e. the tree the saga is acting
    /// against right now, post any online-resize alias swap.</param>
    /// <param name="seed">The seed shard indices to expand. Typically
    /// the saga's <c>TouchedShards</c> after the routing-drift /
    /// registry-participant union, or a single shard index from the
    /// cross-cluster replication apply path.</param>
    /// <param name="cancellationToken">Cooperative cancellation for
    /// the BFS expansion. Each wavefront's parallel
    /// <see cref="IShardRootGrain.GetSplitForwardTargetsAsync"/>
    /// fan-out checks the token before issuing the RPCs.</param>
    public static async Task<List<int>> ResolveTransitiveAsync(
        IGrainFactory grainFactory,
        string physicalTreeId,
        IEnumerable<int> seed,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(physicalTreeId);
        ArgumentNullException.ThrowIfNull(seed);

        var visited = new HashSet<int>();
        var frontier = new List<int>();
        foreach (var s in seed)
        {
            if (visited.Add(s))
                frontier.Add(s);
        }

        while (frontier.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var tasks = new Task<List<int>>[frontier.Count];
            for (var i = 0; i < frontier.Count; i++)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>(
                    $"{physicalTreeId}/{frontier[i]}");
                tasks[i] = shard.GetSplitForwardTargetsAsync();
            }
            var results = await Task.WhenAll(tasks);

            var next = new List<int>();
            foreach (var children in results)
            {
                if (children is null) continue;
                foreach (var child in children)
                {
                    if (visited.Add(child))
                        next.Add(child);
                }
            }
            frontier = next;
        }

        var sorted = new List<int>(visited);
        sorted.Sort();
        return sorted;
    }
}
