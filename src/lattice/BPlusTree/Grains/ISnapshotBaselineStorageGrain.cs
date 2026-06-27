using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable per-cursor, per-shard frozen-baseline storage grain. Holds at most
/// one <see cref="SnapshotShardBaseline"/> per (snapshot cursor, physical
/// shard), captured atomically at <c>OpenSnapshot*Async</c> time and served by
/// the per-shard <see cref="ISnapshotLeafGrain"/> for the cursor's lifetime.
/// Sibling to <see cref="ILeafSnapshotStorageGrain"/>: where that grain holds
/// the leaf-state safety-net snapshot, this grain holds the read-snapshot
/// baseline a zero-observable-writes cursor reads instead of replaying the WAL.
/// <para>
/// Grain key format: <c>{treeId}/{shardIndex}/{baselineToken:N}</c>. The
/// baseline token is the per-cursor
/// <see cref="LatticeSnapshotCoordinate.SnapshotBaselineToken"/>, so two
/// callers that reconstruct the same cursor coordinate address the same
/// baseline rows, and a cursor close can deterministically delete every
/// shard's row.
/// </para>
/// </summary>
[Alias(TypeAliases.ISnapshotBaselineStorageGrain)]
internal interface ISnapshotBaselineStorageGrain : IGrainWithStringKey
{
    /// <summary>
    /// Persists <paramref name="baseline"/> as the frozen baseline for this
    /// (cursor, shard), overwriting any previously persisted row. Returns only
    /// after the underlying state provider has durably accepted the write.
    /// </summary>
    /// <param name="baseline">Baseline payload. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the persist.</param>
    Task SaveAsync(SnapshotShardBaseline baseline, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the persisted baseline for this (cursor, shard), or
    /// <see langword="null"/> when none has been captured (or after a
    /// successful <see cref="ClearAsync"/>).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the load.</param>
    Task<SnapshotShardBaseline?> LoadAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Drops the persisted baseline. Idempotent: clearing a (cursor, shard)
    /// that has no baseline is a no-op. Called from the cursor close path and
    /// the idle-TTL expiry cleanup so a baseline never outlives its cursor.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the clear.</param>
    Task ClearAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Slides the leak-guard retention TTL forward without rewriting the
    /// baseline payload. Called (throttled) by the serving snapshot leaf while a
    /// cursor is actively paging, so a long-running scan keeps its durable
    /// baseline alive past the configured
    /// <see cref="LatticeOptions.SnapshotBaselineTtl"/> window. A no-op when no
    /// baseline has been persisted for this (cursor, shard).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the slide.</param>
    Task TouchAsync(CancellationToken cancellationToken);
}
