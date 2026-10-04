using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Pure helpers for following a shadow-forward the destination copy refused
/// because the key's slot has moved there (issue #4478).
/// <para>
/// After an online resize the copy it replaced keeps mirroring each mutation it
/// still takes into the resized copy, addressed by shard index. A split of the
/// resized copy can move a slot off that shard, which then refuses the forward
/// with <see cref="StaleShardRoutingException"/> naming the shard that owns the
/// slot now. The refusal is raised by the write gate before anything is applied,
/// so the forward is re-sent to that shard; a batch is first broken into one
/// forward per entry, because its other entries may still belong where it was
/// sent. A forward that is still refused after <see cref="MaxHops"/> re-sends
/// fails, and with it the mirrored write, which is then never acknowledged.
/// </para>
/// </summary>
internal static class ShadowForwardRefusal
{
    /// <summary>
    /// The most re-sends one forward follows across the destination copy's shards
    /// before the refusal surfaces. Each re-send follows one completed or
    /// rejecting split, so this bounds a chain of splits of the same slot made
    /// while the replaced copy still mirrors.
    /// </summary>
    internal const int MaxHops = 8;

    /// <summary>
    /// The shard to re-send a refused forward to, or <see langword="null"/> when
    /// the refusal names no other shard of the destination copy - a shard an
    /// online consolidation retired refuses with no target - so the refusal
    /// surfaces.
    /// </summary>
    /// <param name="refusal">The refusal the destination shard raised.</param>
    /// <param name="refusingShardIndex">The destination shard that raised it.</param>
    /// <param name="hopsTaken">Re-sends this forward has already followed.</param>
    internal static int? NextShard(StaleShardRoutingException refusal, int refusingShardIndex, int hopsTaken)
    {
        ArgumentNullException.ThrowIfNull(refusal);
        if (hopsTaken >= MaxHops) return null;
        var target = refusal.TargetShardIndex;
        return target < 0 || target == refusingShardIndex ? null : target;
    }

    /// <summary>One single-entry batch per entry of a refused <c>SetManyAsync</c> forward.</summary>
    internal static IReadOnlyList<List<KeyValuePair<string, byte[]>>> PerEntry(List<KeyValuePair<string, byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var parts = new List<List<KeyValuePair<string, byte[]>>>(entries.Count);
        foreach (var entry in entries)
            parts.Add([entry]);
        return parts;
    }

    /// <summary>One single-entry batch per entry of a refused conditional <c>SetManyWherePredicateAsync</c> forward, each with the same predicate.</summary>
    internal static IReadOnlyList<(List<KeyValuePair<string, byte[]>> Entries, LatticePredicateNode Predicate)> PerEntry(
        (List<KeyValuePair<string, byte[]>> Entries, LatticePredicateNode Predicate) batch)
    {
        ArgumentNullException.ThrowIfNull(batch.Entries);
        var parts = new List<(List<KeyValuePair<string, byte[]>>, LatticePredicateNode)>(batch.Entries.Count);
        foreach (var entry in batch.Entries)
            parts.Add(([entry], batch.Predicate));
        return parts;
    }

    /// <summary>One single-entry batch per entry of a refused <c>MergeManyAsync</c> forward.</summary>
    internal static IReadOnlyList<Dictionary<string, LwwValue<byte[]>>> PerEntry(Dictionary<string, LwwValue<byte[]>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        var parts = new List<Dictionary<string, LwwValue<byte[]>>>(entries.Count);
        foreach (var (key, value) in entries)
            parts.Add(new Dictionary<string, LwwValue<byte[]>>(1, entries.Comparer) { [key] = value });
        return parts;
    }
}
