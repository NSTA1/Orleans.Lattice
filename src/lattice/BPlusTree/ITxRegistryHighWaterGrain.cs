using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Durable, monotone per-tree record of how many saga decision registry shards
/// may hold a decision (issue #3501). Keyed by the physical tree id.
/// <para>
/// A registry shard <c>{treeId}~s{n}</c> raises the mark to at least
/// <c>n + 1</c> and awaits the durable raise <b>before</b> its first state write
/// in each activation, so no shard ever holds a decision the mark does not
/// cover. Tree-wide registry reads (snapshots, the decisions revision, the
/// cross-tree in-flight observation, pin release) read the mark alongside their
/// fan-out and widen to it when it exceeds the key set they used. The set of
/// keys a tree-wide read covers therefore depends only on durable per-tree
/// state, never on the reading silo's configured
/// <see cref="LatticeOptions.TxRegistryShardCount"/>, so lowering the count, or
/// running silos with different counts, can neither hide a shard's decisions
/// nor reroute an existing transaction id.
/// </para>
/// <para>
/// The mark only ever grows. A tree that never mints a sharded transaction id
/// keeps the mark at its initial value (zero: the legacy registry only).
/// </para>
/// <para>
/// This is a dedicated grain type rather than a method on
/// <see cref="ITxRegistryGrain"/> so that, during a rolling upgrade, it is only
/// ever placed on silos that run the sharded registry: a silo without this
/// grain class never hosts it.
/// </para>
/// </summary>
[Alias(TypeAliases.ITxRegistryHighWaterGrain)]
internal interface ITxRegistryHighWaterGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the durable shard high-water mark: one more than the highest
    /// registry shard index that may hold a decision, or zero when no shard has
    /// written anything (the legacy registry, which every read covers, only).
    /// </summary>
    /// <returns>The shard high-water mark.</returns>
    [ReadOnly]
    Task<int> GetShardHighWaterAsync();

    /// <summary>
    /// Raises the shard high-water mark to at least
    /// <paramref name="shardCount"/> and returns once the resulting mark is
    /// durable. A value at or below the current mark is a no-op that writes
    /// nothing.
    /// </summary>
    /// <param name="shardCount">The minimum mark to record, in <c>[1, <see cref="LatticeOptions.MaxTxRegistryShardCount"/>]</c>.</param>
    /// <returns>The durable mark after the call.</returns>
    Task<int> RaiseShardHighWaterAsync(int shardCount);
}
