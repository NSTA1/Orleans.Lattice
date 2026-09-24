using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The one shared chain-tiling check for integration suites (issue #2125):
/// walks a shard's leaf chain and reports every place the chain stops tiling
/// the keyspace, or routing stops agreeing with it.
/// <para>
/// Two properties are checked:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Adjacent spans meet.</b> For each consecutive pair,
/// <c>here.HighKeyExclusive == next.LowKeyInclusive</c>. A <c>null</c> bound
/// means unbounded, not missing, so it is compliant: a bulk-loaded leaf
/// declares no span at all and owns every key, and an unbounded side can only
/// ever overlap a neighbour, never leave a gap. Only two bounded sides that
/// disagree are a break.
/// </description></item>
/// <item><description>
/// <b>Routing agrees with the chain.</b> Each leaf's own low bound is descended
/// through the shard root, and the leaf that descent reaches must declare the
/// key. A gap in <i>routing</i> - a range sent to a leaf whose declared span
/// excludes it - is invisible to the span walk above, because both leaves
/// still sit in the chain with matching bounds. It is the state the pre-#2099
/// fold ordering (retire routing before widening the predecessor) leaves behind
/// when the fold then declines, and it is the one that loses writes: the
/// predecessor accepts them and its replay filter drops them.
/// </description></item>
/// </list>
/// </summary>
internal static class LeafChainTiling
{
    private const int MaxChainLength = 10_000;

    /// <summary>
    /// Asserts that <paramref name="shard"/>'s chain tiles the keyspace and that
    /// routing agrees with it.
    /// </summary>
    public static async Task AssertTilesAsync(IGrainFactory grainFactory, IShardRootGrain shard, string because)
    {
        var breaks = await FindBreaksAsync(grainFactory, shard);
        Assert.That(breaks, Is.Empty,
            $"{because}: the leaf chain no longer tiles the keyspace: {string.Join("; ", breaks)}");
    }

    /// <summary>
    /// Asserts the tiling of every live physical shard of the logical tree
    /// <paramref name="treeId"/>, resolving the physical tree id and the shard
    /// set through the registry so a consolidated, resized or aliased tree is
    /// walked as it actually routes.
    /// </summary>
    public static async Task AssertTreeTilesAsync(IGrainFactory grainFactory, string treeId, string because)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(treeId);
        var map = await registry.GetShardMapAsync(treeId);
        IEnumerable<int> shards = map is not null
            ? map.GetPhysicalShardIndices()
            : Enumerable.Range(0, (await registry.GetEntryAsync(treeId))?.ShardCount ?? LatticeConstants.DefaultShardCount);

        var walked = 0;
        foreach (var i in shards)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{i}");

            // A shard no key ever hashed to has never materialised a root, so it
            // has no chain to tile. That is vacuously compliant at tree scope;
            // the single-shard AssertTilesAsync still treats a missing chain as
            // a break, because its callers have written to that shard.
            if (await shard.GetLeftmostLeafIdAsync() is null)
            {
                continue;
            }

            await AssertTilesAsync(grainFactory, shard, $"{because} (shard {i})");
            walked++;
        }

        Assert.That(walked, Is.GreaterThan(0),
            $"{because}: no shard of tree '{treeId}' has a leaf chain, so the tiling check walked nothing");
    }

    /// <summary>
    /// Returns every tiling and routing break in <paramref name="shard"/>'s
    /// chain without asserting, for fixtures that gather all diagnostics before
    /// asserting any of them.
    /// </summary>
    public static async Task<List<string>> FindBreaksAsync(IGrainFactory grainFactory, IShardRootGrain shard)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(shard);

        var breaks = new List<string>();
        var chain = new List<(GrainId Id, LeafKeyRange Range)>();
        var visited = new HashSet<GrainId>();
        var cursor = await shard.GetLeftmostLeafIdAsync();
        while (cursor is { } id)
        {
            if (!visited.Add(id) || chain.Count >= MaxChainLength)
            {
                breaks.Add($"the sibling chain revisits leaf {id} or exceeds {MaxChainLength} leaves");
                break;
            }

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(id);
            chain.Add((id, await leaf.GetKeyRangeAsync()));
            cursor = await leaf.GetNextSiblingAsync();
        }

        if (chain.Count == 0)
        {
            breaks.Add("the shard has no leftmost leaf");
            return breaks;
        }

        for (var i = 0; i < chain.Count - 1; i++)
        {
            var high = chain[i].Range.HighKeyExclusive;
            var low = chain[i + 1].Range.LowKeyInclusive;
            if (high is not null && low is not null && !string.Equals(high, low, StringComparison.Ordinal))
            {
                breaks.Add(
                    $"leaf {i} ends at '{high}' but leaf {i + 1} begins at '{low}', so that span is owned by "
                    + (string.CompareOrdinal(high, low) < 0 ? "nobody" : "both"));
            }
        }

        for (var i = 0; i < chain.Count; i++)
        {
            var low = chain[i].Range.LowKeyInclusive;
            if (low is null) continue;

            var routed = await shard.GetLeafIdForKeyAsync(low);
            if (routed is not { } routedId)
            {
                breaks.Add($"descent on leaf {i}'s low bound '{low}' resolved to no leaf");
                continue;
            }

            var routedRange = routedId == chain[i].Id
                ? chain[i].Range
                : await grainFactory.GetGrain<IBPlusLeafGrain>(routedId).GetKeyRangeAsync();
            if (!SplitBoundary.Owns(low, routedRange.LowKeyInclusive, routedRange.HighKeyExclusive))
            {
                breaks.Add(
                    $"descent on leaf {i}'s low bound '{low}' reaches leaf {routedId}, whose declared span "
                    + $"['{routedRange.LowKeyInclusive ?? "-inf"}', '{routedRange.HighKeyExclusive ?? "+inf"}') excludes it, "
                    + "so routing sends that range to a leaf that does not declare it");
            }
        }

        return breaks;
    }
}
