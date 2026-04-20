using System.ComponentModel;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The root grain for a single shard of a B+ tree.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// The root grain acts as the entry point for traversal; it may be an internal
/// node or (initially) a leaf node.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.IShardRootGrain)]
public interface IShardRootGrain : IGrainWithStringKey
{
    Task<byte[]?> GetAsync(string key);
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Gets the value and its <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/>
    /// version for <paramref name="key"/>. Returns a <see cref="VersionedValue"/>
    /// with <c>null</c> value and zero version when the key is absent or tombstoned.
    /// </summary>
    Task<VersionedValue> GetWithVersionAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>, performing a single
    /// tree traversal per distinct leaf and batching reads at each leaf.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys);

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value);

    /// <summary>
    /// Inserts or updates the value for <paramref name="key"/> with an absolute
    /// expiry. The entry is treated as tombstoned on reads once the
    /// current UTC wall clock passes <paramref name="expiresAtTicks"/>.
    /// Pass <c>0</c> for no expiry (equivalent to <see cref="SetAsync(string, byte[])"/>).
    /// </summary>
    Task SetAsync(string key, byte[] value, long expiresAtTicks);

    /// <summary>
    /// Returns the raw entry for <paramref name="key"/> — wrapped in an
    /// <see cref="LwwEntry"/> so the Orleans type-alias encoder handles a
    /// single aliased shape rather than a nested
    /// <c>Nullable&lt;LwwValue&lt;byte[]&gt;&gt;</c>. Preserves both the
    /// <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/> version
    /// and absolute <c>ExpiresAtTicks</c> ( TTL). Returns <c>null</c>
    /// when the key is absent or tombstoned. Already-expired entries are
    /// returned so callers can introspect expiry metadata; use
    /// <see cref="LwwValue{T}.IsExpired(long)"/> to filter.
    /// </summary>
    Task<LwwEntry?> GetRawEntryAsync(string key);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns the existing value when the key is live,
    /// or <c>null</c> when the write was performed.
    /// </summary>
    Task<byte[]?> GetOrSetAsync(string key, byte[] value);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the entry's
    /// current <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/> matches
    /// <paramref name="expectedVersion"/>. Returns <c>true</c> if the write was applied.
    /// </summary>
    Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion);

    /// <summary>
    /// Inserts or updates multiple key-value pairs in a single traversal batch.
    /// </summary>
    Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// </summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Tombstones all live keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)
    /// by walking the leaf chain. Returns the total number of keys tombstoned.
    /// </summary>
    Task<int> DeleteRangeAsync(string startInclusive, string endExclusive);

    /// <summary>
    /// Returns the total number of live (non-tombstoned) keys in this shard's B+ tree
    /// by walking the leaf chain and summing per-leaf counts.
    /// </summary>
    Task<int> CountAsync();

    /// <summary>
    /// Returns a page of live keys in this shard's B+ tree in sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; keys &gt; the token are returned.
    /// </summary>
    Task<KeysPage> GetSortedKeysBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns a page of live keys in <em>reverse</em> sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; keys &lt; the token are returned.
    /// </summary>
    Task<KeysPage> GetSortedKeysBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns a page of live key-value entries in this shard's B+ tree in sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; entries with keys &gt; the token are returned.
    /// </summary>
    Task<EntriesPage> GetSortedEntriesBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns a page of live key-value entries in <em>reverse</em> sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; entries with keys &lt; the token are returned.
    /// </summary>
    Task<EntriesPage> GetSortedEntriesBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns the <see cref="GrainId"/> of the leftmost leaf in this shard's B+ tree,
    /// or <c>null</c> if the tree has not been initialised yet.
    /// Used by the tombstone compaction grain to walk the leaf chain.
    /// </summary>
    Task<GrainId?> GetLeftmostLeafIdAsync();

    /// <summary>
    /// Bulk-loads pre-sorted key-value pairs into this shard, building leaves and
    /// internal nodes bottom-up. The shard must be empty (no root node).
    /// Entries must already be sorted in ascending key order.
    /// </summary>
    /// <param name="operationId">Unique ID for idempotency. Retries with the same ID are no-ops.</param>
    Task BulkLoadAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries);

    /// <summary>
    /// Bulk-loads pre-stamped <see cref="LwwValue{T}"/> entries into an empty
    /// shard, preserving the original <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/>
    /// version and <c>ExpiresAtTicks</c> on every entry. Used by
    /// snapshot / restore so TTL and source HLC metadata survive
    /// the transfer end-to-end. Entries must already be sorted in ascending
    /// key order.
    /// </summary>
    /// <param name="operationId">Unique ID for idempotency. Retries with the same ID are no-ops.</param>
    Task BulkLoadRawAsync(string operationId, List<LwwEntry> sortedEntries);

    /// <summary>
    /// Appends a sorted batch of key-value pairs to the right edge of this shard's
    /// B+ tree. All keys in <paramref name="sortedEntries"/> must be greater than
    /// every existing key in the shard. Creates new leaves as needed and propagates
    /// separators into internal nodes. Used by the streaming bulk-load extension method.
    /// </summary>
    /// <param name="operationId">Unique ID for idempotency. Retries with the same ID are no-ops.</param>
    Task BulkAppendAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries);

    /// <summary>
    /// Marks this shard as deleted. Subsequent reads and writes will throw
    /// <see cref="InvalidOperationException"/>. Idempotent.
    /// </summary>
    Task MarkDeletedAsync();

    /// <summary>
    /// Clears the deleted flag on this shard, restoring it to normal operation.
    /// Idempotent.
    /// </summary>
    Task UnmarkDeletedAsync();

    /// <summary>
    /// Returns <c>true</c> if this shard has been marked as deleted.
    /// </summary>
    Task<bool> IsDeletedAsync();

    /// <summary>
    /// Permanently purges all grains in this shard (leaves, internal nodes)
    /// by clearing their persistent state and deactivating them, then clears
    /// the shard root's own state. Called by <see cref="ITreeDeletionGrain"/>
    /// after the soft-delete window has elapsed.
    /// </summary>
    Task PurgeAsync();

    /// <summary>
    /// Merges entries into this shard using LWW semantics, preserving original
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> timestamps. Routes
    /// each entry to the correct leaf via tree traversal and handles splits.
    /// Used by the tree merge operation.
    /// </summary>
    Task MergeManyAsync(Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>> entries);

    /// <summary>
    /// Returns volatile in-memory hotness counters for this shard. Counters
    /// track reads and writes since grain activation and reset on deactivation.
    /// Used by split coordinators to detect hot shards without persistence overhead.
    /// </summary>
    Task<ShardHotness> GetHotnessAsync();

    /// <summary>
    /// Marks this shard as the source of an in-progress adaptive split.
    /// While the returned task is incomplete or the split has not been completed,
    /// every write to a key whose virtual slot is in <paramref name="movedSlots"/>
    /// is mirrored to the shard at <paramref name="targetShardIndex"/> via
    /// <see cref="MergeManyAsync"/>, preserving HLC timestamps for CRDT-safe
    /// convergence. Reads continue to be served locally.
    /// <para>
    /// Idempotent: if the shard is already in <see cref="ShardSplitPhase.BeginShadowWrite"/>
    /// or <see cref="ShardSplitPhase.Drain"/> with a matching
    /// <paramref name="targetShardIndex"/> and <paramref name="movedSlots"/>, the call
    /// is a no-op.
    /// </para>
    /// </summary>
    Task BeginSplitAsync(int targetShardIndex, int[] movedSlots, int virtualShardCount);

    /// <summary>
    /// Transitions this shard's in-progress split to the <see cref="ShardSplitPhase.Reject"/>
    /// phase. Subsequent reads and writes to keys in any of the moved virtual slots
    /// throw <see cref="StaleShardRoutingException"/>, which the calling
    /// <c>LatticeGrain</c> catches to refresh its cached <see cref="ShardMap"/> and
    /// retry against the new physical shard. Idempotent.
    /// </summary>
    Task EnterRejectPhaseAsync();

    /// <summary>
    /// Clears the in-progress split state on this shard. Called by the split
    /// coordinator after the post-swap cleanup phase has finished tombstoning
    /// the moved entries. Idempotent.
    /// </summary>
    Task CompleteSplitAsync();

    /// <summary>
    /// Returns <c>true</c> if this shard has a non-null
    /// <see cref="ShardSplitInProgress"/> in its persistent state.
    /// </summary>
    Task<bool> IsSplittingAsync();

    /// <summary>
    /// Returns <c>true</c> if this shard has a pending bulk-load or bulk-append
    /// operation that has not yet been fully grafted into the tree. Used by the
    /// auto-split monitor to suppress splits while bulk operations are mid-flight.
    /// </summary>
    Task<bool> HasPendingBulkOperationAsync();

    /// <summary>
    /// Strongly-consistent variant of <see cref="CountAsync"/> for use by
    /// <c>ILattice.CountAsync</c>. Returns the live key count plus the set of
    /// virtual slots this shard filtered out because they have been (or are
    /// being) moved to another physical shard by an adaptive split.
    /// The orchestrator uses <see cref="ShardCountResult.MovedAwaySlots"/> to
    /// query the new owners for the missing slots and produce a consistent
    /// total even mid-split.
    /// </summary>
    Task<ShardCountResult> CountWithMovedAwayAsync();

    /// <summary>
    /// Returns the number of live (non-tombstoned) keys in this shard whose
    /// virtual slot is in <paramref name="sortedSlots"/>. Used by
    /// <c>ILattice.CountAsync</c> after detecting a topology change to count
    /// only the entries that moved to this shard during a split, without
    /// double-counting the shard's pre-existing data.
    /// </summary>
    /// <param name="sortedSlots">
    /// Virtual slots to count, in ascending order. Used by binary search on
    /// the leaf hot path; pre-sorting is the caller's responsibility.
    /// </param>
    /// <param name="virtualShardCount">
    /// Virtual shard count used to compute the slot for each key; must match
    /// the value used elsewhere in the tree's routing.
    /// </param>
    Task<int> CountForSlotsAsync(int[] sortedSlots, int virtualShardCount);

    /// <summary>
    /// Returns a page of live keys in this shard whose virtual slot is in
    /// <paramref name="sortedSlots"/>, in sorted order, filtered to the
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Used by <c>ILattice.KeysAsync</c> to fetch slot-restricted entries
    /// from a new owner after detecting a topology change mid-scan.
    /// Pagination semantics match <see cref="GetSortedKeysBatchAsync"/>.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="GetSortedKeysBatchAsync"/>, this method does <em>not</em>
    /// apply the shard's <c>MovedAwaySlots</c> filter — the caller has explicitly
    /// asked for these slots and is responsible for routing to the correct owner
    /// based on the latest <see cref="ShardMap"/>. The returned
    /// <see cref="KeysPage.MovedAwaySlots"/> is always <c>null</c>.
    /// </remarks>
    Task<KeysPage> GetSortedKeysBatchForSlotsAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        int[] sortedSlots,
        int virtualShardCount);

    /// <summary>
    /// Returns a page of live key-value entries in this shard whose virtual slot
    /// is in <paramref name="sortedSlots"/>, in sorted key order, filtered to the
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Used by <c>ILattice.EntriesAsync</c> to fetch slot-restricted entries
    /// from a new owner after detecting a topology change mid-scan.
    /// Pagination semantics match <see cref="GetSortedEntriesBatchAsync"/>.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="GetSortedEntriesBatchAsync"/>, this method does <em>not</em>
    /// apply the shard's <c>MovedAwaySlots</c> filter — the caller has explicitly
    /// asked for these slots and is responsible for routing to the correct owner
    /// based on the latest <see cref="ShardMap"/>. The returned
    /// <see cref="EntriesPage.MovedAwaySlots"/> is always <c>null</c>.
    /// </remarks>
    Task<EntriesPage> GetSortedEntriesBatchForSlotsAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken,
        int[] sortedSlots,
        int virtualShardCount);
}
