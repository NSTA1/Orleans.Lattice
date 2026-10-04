namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Chooses the shards of the resized copy that a saga terminal is redelivered to
/// when the copy it was addressed to has been purged (issue #4475).
/// <para>
/// A saga bound to a resize's old copy addresses each terminal to one of that
/// copy's shards. Once the copy is purged that shard refuses on every retry, but
/// every prepared bucket the saga wrote to it was mirrored into the resized copy
/// at the same shard index, and a migration on the resized copy may since have
/// moved a bucket or a key on to another shard. The terminal is therefore
/// redelivered to the resized copy's shard at the same index and to the shard
/// that now owns each key the purged shard was sent; the caller expands the set
/// by each shard's split-forward targets, so a migration window still open on
/// the resized copy is covered too. Delivering a terminal to a shard that holds
/// no bucket for the transaction is a no-op there.
/// </para>
/// </summary>
internal static class PurgedCopyTerminalTargets
{
    /// <summary>
    /// The resized copy's shard indices to redeliver to, sorted ascending: the
    /// purged shard's own index plus the owner of each of <paramref name="keys"/>
    /// under <paramref name="resizedCopyMap"/>.
    /// </summary>
    /// <param name="shardIndex">The purged copy's shard the terminal was addressed to.</param>
    /// <param name="keys">The saga keys the terminal for that shard covers.</param>
    /// <param name="resizedCopyMap">The resized copy's current shard map.</param>
    internal static List<int> Resolve(int shardIndex, IEnumerable<string> keys, ShardMap resizedCopyMap)
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(resizedCopyMap);

        var targets = new SortedSet<int> { shardIndex };
        foreach (var key in keys)
            targets.Add(resizedCopyMap.Resolve(key));
        return [.. targets];
    }
}
