namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Resolves the physical shard indices a whole-tree copy (snapshot, and the
/// online resize built on it) must visit.
/// <para>
/// The pinned <c>ShardCount</c> alone is not enough. An adaptive shard split
/// allocates its target index above the pin and routes the moved slots there
/// without changing the pin, so a walk over <c>0</c> to <c>ShardCount - 1</c>
/// silently skips every key a split moved (issue 3880). The result is the
/// sorted union of that contiguous range and every index the routing map names:
/// the contiguous range is kept so a shard the map no longer routes to is still
/// quiesced, forwarded, and rejected alongside the rest, and a copy filters out
/// whatever such a shard holds by routing it through the map.
/// </para>
/// </summary>
internal static class RoutedShardIndices
{
    /// <summary>
    /// Returns the sorted, distinct union of <c>0</c> to
    /// <paramref name="shardCount"/><c> - 1</c> and the physical indices
    /// <paramref name="map"/> routes to. A <see langword="null"/> map (a tree
    /// still on its default routing) contributes nothing beyond the pinned range.
    /// </summary>
    public static int[] Resolve(int shardCount, ShardMap? map)
    {
        if (shardCount < 0)
            throw new ArgumentOutOfRangeException(nameof(shardCount), "Must not be negative.");

        var highest = shardCount - 1;
        var routed = map?.GetPhysicalShardIndices() ?? Array.Empty<int>();
        foreach (var index in routed)
        {
            if (index > highest) highest = index;
        }

        var present = new bool[highest + 1];
        var count = 0;
        for (var i = 0; i < shardCount; i++)
        {
            present[i] = true;
            count++;
        }
        foreach (var index in routed)
        {
            if (index < 0 || present[index]) continue;
            present[index] = true;
            count++;
        }

        var result = new int[count];
        var next = 0;
        for (var i = 0; i < present.Length; i++)
        {
            if (present[i]) result[next++] = i;
        }
        return result;
    }

    /// <summary>
    /// Returns <paramref name="persisted"/> when a coordinator recorded its
    /// index set, otherwise <c>0</c> to <paramref name="shardCount"/><c> - 1</c>:
    /// the set a coordinator persisted before the index set existed walked.
    /// </summary>
    public static int[] OrContiguous(int[]? persisted, int shardCount)
    {
        if (persisted is not null) return persisted;
        var result = new int[Math.Max(0, shardCount)];
        for (var i = 0; i < result.Length; i++) result[i] = i;
        return result;
    }
}
