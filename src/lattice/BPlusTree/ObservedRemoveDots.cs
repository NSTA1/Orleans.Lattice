namespace Orleans.Lattice;

/// <summary>
/// Dot bookkeeping shared by the observed-remove accessors
/// (<see cref="OrFlagAccessor"/>, <see cref="RwFlagAccessor"/>,
/// <see cref="OrSetAccessor"/>, <see cref="RwSetAccessor"/>) and the tag index's
/// atomic write path, so every writer mints and observes dots identically.
/// </summary>
internal static class ObservedRemoveDots
{
    /// <summary>
    /// The highest counter <paramref name="replicaId"/> has used across the
    /// flag's enable and tombstone dots, plus one.
    /// </summary>
    /// <param name="flag">The flag's current state.</param>
    /// <param name="replicaId">The writing replica.</param>
    internal static long NextCounter(OrFlag flag, string replicaId)
    {
        var max = MaxCounter(flag.Enables, replicaId, 0);
        max = MaxCounter(flag.Tombstones, replicaId, max);
        return max + 1;
    }

    /// <summary>
    /// The highest counter <paramref name="replicaId"/> has used across the
    /// flag's enable, disable and tombstone dots, plus one.
    /// </summary>
    /// <param name="flag">The flag's current state.</param>
    /// <param name="replicaId">The writing replica.</param>
    internal static long NextCounter(RwFlag flag, string replicaId)
    {
        var max = MaxCounter(flag.Enables, replicaId, 0);
        max = MaxCounter(flag.Disables, replicaId, max);
        max = MaxCounter(flag.Tombstones, replicaId, max);
        return max + 1;
    }

    /// <summary>
    /// The highest counter <paramref name="replicaId"/> has used across the
    /// set's add and tombstone dots, plus one.
    /// </summary>
    /// <param name="set">The set's current state.</param>
    /// <param name="replicaId">The writing replica.</param>
    internal static long NextCounter(OrSet set, string replicaId)
    {
        var max = MaxCounter(set.Adds, replicaId, 0);
        max = MaxCounter(set.Tombstones, replicaId, max);
        return max + 1;
    }

    /// <summary>
    /// The highest counter <paramref name="replicaId"/> has used across the
    /// set's add, remove and tombstone dots, plus one.
    /// </summary>
    /// <param name="set">The set's current state.</param>
    /// <param name="replicaId">The writing replica.</param>
    internal static long NextCounter(RwSet set, string replicaId)
    {
        var max = MaxCounter(set.Adds, replicaId, 0);
        max = MaxCounter(set.Removes, replicaId, max);
        max = MaxCounter(set.Tombstones, replicaId, max);
        return max + 1;
    }

    /// <summary>
    /// A copy of the disable dots an <see cref="RwFlag"/> enable currently
    /// observes - and therefore cancels (remove-wins bookkeeping). Returns the
    /// shared empty array when there are none.
    /// </summary>
    /// <param name="flag">The flag's current state.</param>
    internal static OrSetDot[] ObservedDisables(RwFlag flag)
    {
        if (flag.Disables.Count == 0) return Array.Empty<OrSetDot>();
        var observed = new OrSetDot[flag.Disables.Count];
        for (var i = 0; i < flag.Disables.Count; i++)
        {
            observed[i] = flag.Disables[i];
        }
        return observed;
    }

    /// <summary>
    /// Flattens a set's per-element dot map (keyed by the base64 element) into
    /// the delta-wire dot list, decoding each element once per key. Returns the
    /// shared empty array when the map holds no dots.
    /// </summary>
    /// <param name="map">The per-element dot map.</param>
    internal static OrSetDeltaDot[] FlattenToDeltaDots(Dictionary<string, List<OrSetDot>> map)
    {
        if (map.Count == 0) return Array.Empty<OrSetDeltaDot>();
        var total = 0;
        foreach (var dots in map.Values) total += dots.Count;
        if (total == 0) return Array.Empty<OrSetDeltaDot>();
        var result = new OrSetDeltaDot[total];
        var i = 0;
        foreach (var (key, dots) in map)
        {
            var element = Convert.FromBase64String(key);
            foreach (var d in dots)
            {
                result[i++] = new OrSetDeltaDot { Element = element, ReplicaId = d.ReplicaId, Counter = d.Counter };
            }
        }
        return result;
    }

    private static long MaxCounter(List<OrSetDot> dots, string replicaId, long max)
    {
        foreach (var d in dots)
        {
            if (d.ReplicaId == replicaId && d.Counter > max) max = d.Counter;
        }
        return max;
    }

    private static long MaxCounter(Dictionary<string, List<OrSetDot>> map, string replicaId, long max)
    {
        foreach (var dots in map.Values)
        {
            max = MaxCounter(dots, replicaId, max);
        }
        return max;
    }
}
