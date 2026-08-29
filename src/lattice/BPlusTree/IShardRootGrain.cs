using Orleans.Concurrency;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The root grain for a single shard of a B+ tree.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// The root grain acts as the entry point for traversal; it may be an internal
/// node or (initially) a leaf node.
/// </summary>
[Alias(TypeAliases.IShardRootGrain)]
internal interface IShardRootGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the value for <paramref name="key"/>, or <c>null</c> if absent or tombstoned.
    /// <para>
    /// NOT marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>: the original U9h-C
    /// attempt to interleave pure reads against in-flight
    /// <see cref="SetManyAsync"/> turns reintroduced a chaos-reshard mid-saga
    /// invariant violation ("key missing mid-chaos"). The reader does multiple
    /// non-atomic reads of <c>state.State.RootNodeId</c> /
    /// <c>state.State.RootIsLeaf</c> / <c>state.State.MovedAwaySlots</c>
    /// across awaits, and an interleaved promotion or move-away publish can
    /// land between them, surfacing as a null return for a key the workload
    /// invariant guarantees must be present.
    /// </para>
    /// </summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>
    /// Returns <c>true</c> if <paramref name="key"/> exists and is live.
    /// <para>
    /// NOT marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> for the same reason
    /// documented on <see cref="GetAsync"/>: read traversal is composed of
    /// multiple non-atomic shard-root state reads across awaits.
    /// </para>
    /// </summary>
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Gets the value and its <see cref="Orleans.Lattice.HybridLogicalClock"/>
    /// version for <paramref name="key"/>. Returns a <see cref="VersionedValue"/>
    /// with <c>null</c> value and zero version when the key is absent or tombstoned.
    /// </summary>
    Task<VersionedValue> GetWithVersionAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>, performing a single
    /// tree traversal per distinct leaf and batching reads at each leaf.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// <para>
    /// NOT marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> for the same reason
    /// documented on <see cref="GetAsync"/>: batch read traversal performs
    /// multiple non-atomic reads of shard-root routing state across awaits.
    /// </para>
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
    /// Returns the raw entry for <paramref name="key"/> - wrapped in an
    /// <see cref="LwwEntry"/> so the Orleans type-alias encoder handles a
    /// single aliased shape rather than a nested
    /// <c>Nullable&lt;LwwValue&lt;byte[]&gt;&gt;</c>. Preserves both the
    /// <see cref="Orleans.Lattice.HybridLogicalClock"/> version
    /// and absolute <c>ExpiresAtTicks</c> ( TTL). Returns <c>null</c>
    /// when the key is absent or tombstoned. Already-expired entries are
    /// returned so callers can introspect expiry metadata; use
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.IsExpired(long)"/> to filter.
    /// </summary>
    Task<LwwEntry?> GetRawEntryAsync(string key);

    /// <summary>
    /// Batched variant of <see cref="GetRawEntryAsync(string)"/>. Returns
    /// the raw entry for every key in <paramref name="keys"/>, with the
    /// result list aligned by index with the input list (so
    /// <c>result[i]</c> corresponds to <c>keys[i]</c>). Group-by-leaf
    /// inside the shard root collapses the per-key fan-out into one
    /// leaf RPC per distinct target leaf, mirroring the
    /// <see cref="GetManyAsync(List{string})"/> traversal pattern.
    /// Per-element semantics match the single-key variant: <c>null</c>
    /// for absent or tombstoned keys; already-expired entries are
    /// returned for caller-side introspection of expiry metadata.
    /// </summary>
    Task<List<LwwEntry?>> GetRawEntriesAsync(List<string> keys);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns the existing value when the key is live,
    /// or <c>null</c> when the write was performed.
    /// </summary>
    Task<byte[]?> GetOrSetAsync(string key, byte[] value);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the entry's
    /// current <see cref="Orleans.Lattice.HybridLogicalClock"/> matches
    /// <paramref name="expectedVersion"/>. Returns <c>true</c> if the write was applied.
    /// </summary>
    Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion);

    /// <summary>
    /// Routes a typed CRDT delta apply for <paramref name="key"/> to the
    /// owning leaf. Returns the <see cref="HybridLogicalClock"/> stamped
    /// on the committed entry. See
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.ApplyCrdtDeltaAsync"/> for the
    /// full apply contract.
    /// </summary>
    Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes);

    /// <summary>
    /// Expiry-carrying overload of
    /// <see cref="ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[])"/> that
    /// routes an absolute per-entry expiry (UTC
    /// <see cref="System.DateTime.Ticks"/>, already resolved on the handling
    /// silo) to the owning leaf. See
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], long)"/>
    /// for the max-absolute-ticks expiry-join contract. An
    /// <paramref name="expiresAtTicks"/> of <c>0</c> leaves any existing
    /// expiry unchanged.
    /// </summary>
    Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, long expiresAtTicks);

    /// <summary>
    /// Inserts or updates multiple key-value pairs in a single traversal batch.
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> so multiple producer
    /// flush slots aimed at the same per-shard activation can pipeline
    /// concurrent batches instead of serialising behind a single in-flight
    /// turn. Background: at <c>shardCount=16</c> and producer
    /// <c>flushConcurrency&gt;8</c>, every flush slot independently routes
    /// to the same <see cref="Orleans.Lattice.BPlusTree.Grains.ShardRootGrain"/> activation; the
    /// non-reentrant queue then grows to <c>NonReentrancyQueueSize=FC</c>
    /// and the second-from-front call routinely exceeds Orleans' 30 s
    /// response timeout. The bound is per-shard serial-turn pressure, not
    /// upstream <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeGrain"/> work or provider commit p50
    /// (see U9g).
    /// </para>
    /// <para>
    /// Safety relies on three invariants that hold across interleaved
    /// turns on the same activation:
    /// </para>
    /// <list type="bullet">
    ///   <item>The per-activation grain-reference and routing-table caches
    ///   in <see cref="Orleans.Lattice.BPlusTree.Grains.ShardRootGrain"/>'s traversal partial are
    ///   <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}"/>
    ///   instances, so two turns can read/write them concurrently.</item>
    ///   <item>The leaf apply path is LWW-convergent: two interleaved
    ///   batches that touch the same key resolve via HLC at the owning
    ///   leaf and converge regardless of arrival order.</item>
    ///   <item>Every shard-root <c>state.WriteStateAsync()</c> call is
    ///   routed through a single per-activation
    ///   <see cref="System.Threading.SemaphoreSlim"/>
    ///   (<c>WriteShardStateAsync()</c> in the main partial). This
    ///   serialises the storage I/O - including the
    ///   <c>PromoteRootAsync</c> / <c>CompletePromotionAsync</c> root
    ///   rewrite and the hot <c>MarkLeafDirtyAsync</c> write - while
    ///   leaving the surrounding compute interleaved. Closes the etag
    ///   race that surfaced as <c>InconsistentStateException</c> /
    ///   "Etag mismatch during Update" warnings on real Azure Tables when
    ///   <c>[AlwaysInterleave]</c> first shipped (U9g result, U9h-A fix
    ///).</item>
    /// </list>
    /// <para>
    /// The split-bubble loop in <c>SetManyLocalOnlyAsync</c> calls
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusInternalGrain.AcceptSplitAsync"/> against
    /// parent internals (which remain non-reentrant), so the per-shard
    /// split ordering is still serialised at the parent grain even under
    /// interleaved shard-root turns.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Conditional bulk write: routes <paramref name="entries"/> to their
    /// owning leaves, where each entry is committed only if its <b>current</b>
    /// stored value satisfies <paramref name="predicate"/> (the guard). Returns
    /// the set of keys actually written across this shard's leaves so the
    /// caller can distinguish guarded-out keys. The guard is evaluated once,
    /// server-side, at write time; committed entries become ordinary Set
    /// writes that replicate without re-evaluating the predicate.
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> for the same reason as
    /// <see cref="SetManyAsync"/>.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<IReadOnlyList<string>> SetManyWherePredicateAsync(List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// </summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Tombstones all live keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)
    /// by walking the leaf chain. Returns the total number of keys tombstoned.
    /// When <paramref name="predicate"/> is non-<see langword="null"/> only the
    /// in-range live keys whose value satisfies the predicate are tombstoned,
    /// and the matched key set is propagated to observers / the WAL so replay
    /// and replication reproduce it without re-evaluating the predicate.
    /// </summary>
    Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, LatticePredicateNode? predicate = null);

    /// <summary>
    /// Returns the total number of live (non-tombstoned) keys in this shard's B+ tree
    /// by walking the leaf chain and summing per-leaf counts.
    /// </summary>
    Task<int> CountAsync();

    /// <summary>
    /// Returns the number of live (non-tombstoned) keys in this shard's B+ tree
    /// whose key falls in the half-open range [<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>) by walking the leaf chain and pushing the
    /// bound to each leaf. A <see langword="null"/> bound is unbounded on that
    /// side; <c>CountAsync(null, null)</c> is equivalent to <see cref="CountAsync()"/>.
    /// Counting stays server-side: only an integer crosses the wire, never keys.
    /// </summary>
    Task<int> CountAsync(string? startInclusive, string? endExclusive);

    /// <summary>
    /// Returns <see langword="true"/> as soon as this shard is found to hold at
    /// least one live (non-tombstoned) key, short-circuiting at the first
    /// non-empty leaf rather than walking the whole chain and summing as
    /// <see cref="CountAsync()"/> does. A non-empty shard therefore costs one
    /// leaf call in the common case, and an empty shard costs a walk of the
    /// (correspondingly short) chain.
    /// <para>
    /// Liveness is decided by the same per-leaf count this shard's
    /// <see cref="CountAsync()"/> uses, so TTL expiry, tombstones, pending saga
    /// outcomes, and the in-progress split boundary are all honoured
    /// identically by construction.
    /// </para>
    /// <para>
    /// Deliberately does <em>not</em> filter slots that an adaptive split has
    /// moved away. That filtering exists to stop a <em>count</em> from
    /// double-counting a migrating key that is briefly present on both the
    /// source and the destination shard; for an existence question a key seen
    /// twice is still just "a key exists", and the tree does hold it either
    /// way. Skipping the filter keeps the answer one-sided in the safe
    /// direction: this may report a shard non-empty while its last keys are
    /// migrating away, but it can never report empty while a key exists
    /// anywhere.
    /// </para>
    /// </summary>
    Task<bool> AnyAsync();

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
        string? continuationToken = null,
        Orleans.Lattice.LatticePredicateNode? predicate = null);

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
        string? continuationToken = null,
        Orleans.Lattice.LatticePredicateNode? predicate = null);

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
        string? continuationToken = null,
        Orleans.Lattice.LatticePredicateNode? predicate = null);

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
        string? continuationToken = null,
        Orleans.Lattice.LatticePredicateNode? predicate = null);

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
    /// Bulk-loads pre-stamped <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> entries into an empty
    /// shard, preserving the original <see cref="Orleans.Lattice.HybridLogicalClock"/>
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
    /// Pre-activates this shard's current root-node grain (root leaf when
    /// the tree is flat, root internal node otherwise) so the first
    /// traversing write does not pay placement-directory + grain-storage
    /// first-touch cost on the hot path. Idempotent.
    /// <para>
    /// On a brand-new shard with no root yet, warm-up runs the same
    /// <c>EnsureRootAsync</c> path the first traffic write would run -
    /// it creates the deterministic root leaf and persists the shard
    /// root's mapping. This is equivalent to the very first hot-path
    /// write performing root materialization, just moved to startup
    /// time, and it produces no extra grains the first write would not
    /// have produced anyway.
    /// </para>
    /// <para>
    /// Intended for benchmark / production startup proactive warm-up
    /// driven by <see cref="ILattice.WarmUpAsync"/>. Callers must treat
    /// transient <see cref="OrleansMessageRejectionException"/> the same
    /// way they treat it on <c>ReshardAsync</c> - the placement-
    /// directory cache can race a freshly-started silo - and apply
    /// bounded retry.
    /// </para>
    /// </summary>
    Task WarmUpAsync();

    /// <summary>
    /// Permanently purges all grains in this shard (leaves, internal nodes)
    /// by clearing their persistent state and deactivating them, then clears
    /// the shard root's own state. Called by <see cref="ITreeDeletionGrain"/>
    /// after the soft-delete window has elapsed.
    /// </summary>
    Task PurgeAsync();

    /// <summary>
    /// Re-asserts the owning-tree binding on every node this shard still
    /// routes to, repairing a topology left half-torn-down by an interrupted
    /// <see cref="PurgeAsync"/>. Called by <see cref="ITreeDeletionGrain"/> on
    /// the recovery path.
    /// <para>
    /// <see cref="PurgeAsync"/> clears its child nodes before its own state, so
    /// a purge that dies part-way (a grain-call timeout, a storage fault, a
    /// silo restart) leaves this shard root pointing at nodes whose state -
    /// including the <c>TreeId</c> the shard root seeded at creation - has been
    /// wiped. Because the creation branch that seeds a node is guarded by the
    /// shard root's own <c>RootNodeId</c>, nothing ever re-seeds them, and a
    /// recovered tree routes writes to a leaf that rejects every typed CRDT
    /// apply with <see cref="LatticeCrdtShapeNotRegisteredException"/> forever.
    /// </para>
    /// <para>
    /// Cheap on a healthy shard: the leftmost leaf is unconditionally the first
    /// node <see cref="PurgeAsync"/> clears, so a leftmost leaf that still
    /// carries its binding proves no node in this shard was cleared and the
    /// walk is skipped after a single probe. Idempotent, and safe to call on a
    /// shard that was never purged.
    /// </para>
    /// </summary>
    Task ReseedNodeBindingsAsync();

    /// <summary>
    /// Merges entries into this shard using LWW (Last-Writer-Wins) semantics,
    /// preserving original
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> timestamps. Routes
    /// each entry to the correct leaf via tree traversal and handles splits.
    /// Used by the tree merge operation.
    /// <para>
    /// When <paramref name="isCrossShardMigration"/> is <c>true</c>, the
    /// merge runs the asymmetric migration-vs-foreground rule on the leaf:
    /// see <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.MergeManyAsync"/> for the full
    /// contract. The flag is intended for the cross-shard migration
    /// callsites only (the source-shard drain in
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeShardSplitGrain"/>
    /// and the per-write shadow-forward in <c>ShardRootGrain.Split.cs</c>).
    /// All other callers (cross-cluster replication, tree-merge, snapshot
    /// restore, online tree-resize shadow-forward) MUST pass <c>false</c>
    /// so the merge runs the symmetric LWW-by-HLC contract.
    /// </para>
    /// </summary>
    Task MergeManyAsync(Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>> entries, bool isCrossShardMigration = false);

    /// <summary>
    /// Records that ownership of the given <paramref name="sortedMovedSlots"/>
    /// has migrated away from this shard. Walks the leaf chain
    /// (starting from <see cref="GetLeftmostLeafIdAsync"/>) and calls
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.MarkSlotsMovedAwayAsync"/> on every
    /// leaf so the leaf-side read gate and the cache-coherence prune
    /// pass both observe the moved-slot set. Called by
    /// <c>TreeShardSplitGrain.SwapAsync</c> on the source shard
    /// immediately before <c>EnterRejectPhaseAsync</c>, so no read
    /// crosses the Swap boundary observing an unmarked leaf under a
    /// Reject-phase shard. Returns the total count of leaves that
    /// recorded at least one new moved slot (best-effort - tests use
    /// this for assertions; production paths ignore the value).
    /// </summary>
    /// <param name="sortedMovedSlots">Sorted, distinct virtual-slot indices that have moved away.</param>
    /// <param name="virtualShardCount">The virtual shard count in force at the moment of the move.</param>
    Task<int> MarkLeavesMovedAwayAsync(int[] sortedMovedSlots, int virtualShardCount);

    /// <summary>
    /// Routes <paramref name="keys"/> to their owning leaves and
    /// invokes <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.MarkSagaShadowAsync"/> on
    /// each leaf with the subset of keys it owns. Used by the split
    /// coordinator to install destination-side shadow markers naming
    /// the in-flight source-side saga
    /// <paramref name="transactionId"/> whose prepared mutations
    /// touched migrating keys. See <c>MarkSagaShadowAsync</c> on
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/> for the full atomic-visibility
    /// rationale.
    /// <para>
    /// Idempotent on identical input. Empty key lists are a no-op;
    /// <paramref name="transactionId"/> must be non-empty.
    /// </para>
    /// </summary>
    /// <param name="transactionId">Source-side saga id whose prepared mutations affected the listed keys.</param>
    /// <param name="keys">Keys to shadow. Routed per-leaf internally.</param>
    Task MarkSagaShadowAsync(Guid transactionId, IReadOnlyList<string> keys);

    /// <summary>
    /// Returns volatile in-memory hotness counters for this shard. Counters
    /// track reads and writes since grain activation and reset on deactivation.
    /// Used by split coordinators to detect hot shards without persistence overhead.
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> because the implementation is a
    /// pure synchronous read of three private fields wrapped in
    /// <see cref="Task.FromResult{TResult}(TResult)"/> with zero awaits and zero
    /// state mutation - it cannot race any other in-flight turn. Allowing the
    /// hot-shard monitor's sampling RPC to bypass the
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetManyAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}})"/>
    /// reentrancy queue is required so the monitor does not time out (and fire
    /// spurious reshards) when the shard is at sustained producer pressure
    /// (see U9d).
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<ShardHotness> GetHotnessAsync();

    /// <summary>
    /// Returns a complete <see cref="ShardDiagnosticReport"/> for this shard.
    /// Bundles depth, root-is-leaf, split/bulk state, live-key count, and
    /// hotness into a single RPC. When <paramref name="deep"/> is <c>true</c>,
    /// walks the shard's leaf chain to aggregate tombstone-and-expired counts.
    /// Only the shard index is left unset - the caller stamps it from the key.
    /// </summary>
    Task<ShardDiagnosticReport> GetDiagnosticsAsync(bool deep);

    /// <summary>
    /// Returns this shard's byte-accurate storage-usage rollup - the summed
    /// serialized leaf-state byte footprint and the summed persisted-snapshot
    /// byte footprint across every leaf in the shard's chain - used by the
    /// byte-accurate storage-usage aggregator
    /// (<see cref="ILattice.GetStorageUsageAsync"/>). Served in O(1) from
    /// the shard root's incrementally-maintained
    /// <c>ShardRootState.LeafStateBytesTotal</c> and
    /// <c>SnapshotBytesTotal</c>: leaves push their per-leaf
    /// <see cref="State.LeafByteFootprint"/> via
    /// <see cref="PublishLeafByteFootprintAsync"/> on every commit boundary,
    /// and the shard root keeps the running totals current without ever
    /// walking the leaf chain on the read path. An empty shard returns a
    /// zeroed rollup. The deep walk that fans out across every leaf is now
    /// reserved for the operator-driven re-anchor seam
    /// (<see cref="RefreshLeafByteFootprintsAsync"/>) so a routine
    /// dashboard scrape never pins an idle leaf into memory.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read before it touches state.</param>
    Task<ShardStorageUsage> GetStorageUsageAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Records the byte-accurate footprint <paramref name="footprint"/> for
    /// the leaf identified by <paramref name="leafKey"/>. Called by the leaf
    /// grain on every commit boundary so the shard root's running totals
    /// stay current without a periodic fan-out. Passing
    /// <see cref="State.LeafByteFootprint.Removed"/> drops the leaf's
    /// contribution (used after split-donation or merge-removal). Idempotent
    /// on identical re-publishes (the totals are unchanged when the new
    /// footprint matches the previously recorded one).
    /// </summary>
    /// <param name="leafKey">The leaf's Guid grain key.</param>
    /// <param name="footprint">The leaf's freshly-sampled footprint, or <see cref="State.LeafByteFootprint.Removed"/>.</param>
    [AlwaysInterleave]
    Task PublishLeafByteFootprintAsync(Guid leafKey, State.LeafByteFootprint footprint);

    /// <summary>
    /// Operator-driven deep re-anchor of the shard's incrementally-tracked
    /// leaf byte totals. Walks the leaf chain, queries each leaf's current
    /// state-bytes and snapshot-bytes, and overwrites
    /// <c>ShardRootState.LeafByteFootprints</c> plus the running totals to
    /// match. Reserved for <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>;
    /// the polling path never invokes it. Returns the freshly assembled
    /// shard rollup so the caller can return the deep figure directly.
    /// </summary>
    /// <param name="cancellationToken">Cancels the leaf-chain walk between leaves.</param>
    Task<ShardStorageUsage> RefreshLeafByteFootprintsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns a deterministic XxHash128 <see cref="LeafProjectionDigest"/>
    /// for this entire shard - chains every leaf's digest through XxHash128
    /// in leaf-chain order so two silos with the same applied WAL prefix
    /// produce byte-identical digests. Used by chaos tests and operator
    /// tooling to detect cross-silo divergence.
    /// </summary>
    /// <param name="cancellationToken">Cancels the leaf-chain walk between leaves.</param>
    Task<LeafProjectionDigest> GetShardProjectionDigestAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns a deterministic XxHash128 <see cref="LeafProjectionDigest"/>
    /// for the half-open key range [<paramref name="startInclusive"/>,
    /// <paramref name="endExclusive"/>) of this shard's B+ tree - the
    /// range-scoped analogue of <see cref="GetShardProjectionDigestAsync"/>.
    /// A <see langword="null"/> bound denotes negative infinity
    /// (<paramref name="startInclusive"/>) or positive infinity
    /// (<paramref name="endExclusive"/>); passing both as
    /// <see langword="null"/> produces a digest byte-identical to
    /// <see cref="GetShardProjectionDigestAsync"/>. The fold descends the
    /// internal-node tree by separator-key range, touches only the leaves
    /// (and whole subtrees) that overlap the query range, and combines them
    /// with the same algebra the internal nodes use (XOR the raw
    /// projection hashes, sum the entry counts, max-reduce the checkpoint
    /// offsets) before wrapping the result in the same
    /// XxHash128(rawHash || entryCount || checkpointOffset) shape an
    /// internal node spanning exactly that range would publish. Because the
    /// per-entry contribution is content-only, two clusters holding the same
    /// logical entries in the range compute the same raw fold regardless of
    /// how each cluster physically split its leaves. This is the core
    /// primitive that backs the cross-cluster anti-entropy Merkle walk's
    /// separator-key-bounded localisation. Strictly read-only.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower key bound, or <see langword="null"/> for unbounded below.</param>
    /// <param name="endExclusive">Exclusive upper key bound, or <see langword="null"/> for unbounded above.</param>
    /// <param name="cancellationToken">Cancels the range descent between nodes.</param>
    Task<LeafProjectionDigest> GetShardProjectionDigestForRangeAsync(
        string? startInclusive,
        string? endExclusive,
        CancellationToken cancellationToken);

    /// <summary>
    /// Returns a read-only reference to this shard's current root node - its
    /// grain identity and whether it is a leaf (flat tree) or an internal node
    /// - or <see langword="null"/> when the shard has no root yet (empty
    /// shard). The reference is the entry point for a read-only anti-entropy
    /// drift-localisation traversal that descends the internal-node tree by
    /// separator-key range; combined with
    /// <see cref="IBPlusInternalGrain.GetRoutingTableAsync"/> and
    /// <see cref="IBPlusInternalGrain.GetSubtreeProjectionDigestAsync"/> it lets
    /// a caller walk the tree top-down without mutating any state.
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>: the
    /// implementation is a synchronous read of the shard root's in-memory
    /// <c>RootNodeId</c> / <c>RootIsLeaf</c> slots wrapped in
    /// <see cref="Task.FromResult{TResult}(TResult)"/>, so it cannot race any
    /// other in-flight turn and never queues behind producer writes. The
    /// returned reference is a best-effort point-in-time snapshot; it never
    /// mutates data or any cursor.
    /// </para>
    /// </summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<ShardRootNodeRef?> GetRootNodeRefAsync();

    /// <summary>
    /// Returns a read-only <see cref="ShardTopologyNode"/> tree describing
    /// this shard's structure - node key ranges, per-subtree live/tombstone
    /// counts, depth and fanout - reconstructed from the per-child digest
    /// snapshots that mutations already propagate up the internal-node chain.
    /// Internal nodes are expanded down to <paramref name="depthLimit"/>
    /// levels (0 = root summary only); leaves are summarised from their
    /// parent's stored snapshot, so the read never fans out to the leaf
    /// chain except in the flat-tree case, where the single root leaf is
    /// read once. Returns <see langword="null"/> for an empty shard.
    /// </summary>
    Task<ShardTopologyNode?> GetTopologySnapshotAsync(int depthLimit, CancellationToken cancellationToken);

    /// <summary>
    /// Operator-tooling rebuild: clears the materialised projection state
    /// (entries, projection hash, persisted checkpoint offset, pending-tx
    /// machinery) on every leaf in this shard's chain and forces each
    /// leaf to deactivate so its next activation replays the per-shard
    /// write-ahead log from offset <c>0</c> through the existing
    /// activation-time materialiser. Topology-bearing slots (tree id,
    /// shard index, sibling pointers, key range bounds, parent pointer,
    /// split markers) are preserved verbatim so the rebuild observes
    /// the same WAL-filter ownership context the pre-rebuild leaves
    /// used. Used after a corrupt-projection incident or a
    /// <see cref="LatticeOptions.MaxLeafReplayEntries"/> blow-out to
    /// recover the shard state from the durable WAL source of truth.
    /// <para>
    /// Failures propagate. A transient storage failure on a single leaf
    /// aborts the fan-out with the unaffected leaves' rebuilds already
    /// applied; the operation is safe to retry because every leaf
    /// rebuild is independently idempotent.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the leaf-chain walk between leaves.</param>
    Task RebuildShardProjectionAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the materialiser-lag value for this shard - the gap
    /// between the per-shard write-ahead log head offset and the
    /// minimum projection-checkpoint offset across every leaf in this
    /// shard's chain. A non-zero return value means the shard's
    /// projection has not yet caught up to the durable WAL head and is
    /// the back-pressure signal that complements the replication
    /// receiver's apply-lag gauge.
    /// </summary>
    /// <param name="cancellationToken">Cancels the leaf-chain walk between leaves.</param>
    Task<long> GetShardMaterialiserLagAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Captures the shard's current write-ahead-log head offset (the
    /// next-to-be-assigned sequence number) for a zero-observable-
    /// writes snapshot cursor opened by
    /// <see cref="ILattice.OpenSnapshotKeyCursorAsync"/> /
    /// <see cref="ILattice.OpenSnapshotEntryCursorAsync"/>. The
    /// returned offsets are the upper bound of the WAL prefix a
    /// per-shard snapshot leaf will replay to materialise this
    /// shard's view of the snapshot: replay covers offsets
    /// <c>[0, value)</c> per partition, so a write that appends after
    /// this call is invisible by construction.
    /// <para>
    /// Returns one offset per WAL partition (length equal to the
    /// shard's pinned <see cref="LatticeOptions.WalPartitions"/>).
    /// Under the default single-partition shape the array has a
    /// single element. The fan-out across shards is concurrent and
    /// not linearisable in real time; the snapshot's
    /// <see cref="LatticeSnapshotCoordinate.RegistrySnapshotHlc"/>
    /// resolves saga visibility uniformly across shards so any
    /// single atomic write is all-or-nothing on every shard it
    /// touched.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the underlying coordinator RPC.</param>
    Task<long[]> SnapshotWalHeadAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Captures a frozen, fully-materialised projection baseline for this shard
    /// at snapshot-cursor open time, keyed by <paramref name="token"/> (the
    /// per-cursor
    /// <see cref="LatticeSnapshotCoordinate.SnapshotBaselineToken"/>). Walks the
    /// shard's leaf chain, freezes every leaf's committed cache plus
    /// per-partition frontier and prepared sagas, captures a uniform
    /// per-partition <c>capturedHead</c> after every freeze (so
    /// <c>frontier_p &lt;= capturedHead_p</c> with no overshoot), folds each
    /// leaf's own <c>(frontier_p, capturedHead_p]</c> WAL tail exactly once
    /// (CRDT folds are not idempotent), unions the per-leaf results, and
    /// <b>seeds them in memory</b> directly into the transient per-shard
    /// <see cref="ISnapshotLeafGrain"/> the cursor will read.
    /// <para>
    /// The freeze is correctness-forced for every shard, but the durable write
    /// is now lazy (issue #916): the baseline lives only in the snapshot leaf's
    /// memory until the owning cursor's first page returns <c>HasMore = true</c>,
    /// at which point the leaf flushes it to the per-cursor, per-shard
    /// <see cref="Grains.ISnapshotBaselineStorageGrain"/>. A snapshot that drains
    /// in a single page therefore performs no durable baseline write at capture
    /// and no durable delete at close.
    /// </para>
    /// <para>
    /// This is the fix for the class of bug where a zero-observable-writes
    /// snapshot scan replayed the WAL from offset 0 and silently returned
    /// empty / partial results once <c>LatticeWalGc</c> trimmed the prefix the
    /// replay needed. Serving the cursor from the frozen baseline removes the
    /// dependency on the WAL prefix entirely.
    /// </para>
    /// </summary>
    /// <param name="token">
    /// The cursor's per-open baseline token. Must not be <see cref="Guid.Empty"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the leaf-chain walk and the per-leaf folds.</param>
    /// <returns>
    /// The uniform per-partition WAL head the baseline was frozen at (carried
    /// onto the snapshot coordinate for the WAL retention pin and diagnostics)
    /// and the materialised row count used by the snapshot-open budget gate.
    /// </returns>
    Task<SnapshotBaselineCaptureResult> CaptureSnapshotBaselineAsync(Guid token, CancellationToken cancellationToken);

    /// <summary>
    /// Marks this shard as the source of an in-progress adaptive split.
    /// While the returned task is incomplete or the split has not been completed,
    /// every write to a key whose virtual slot is in <paramref name="movedSlots"/>
    /// is mirrored to the shard at <paramref name="targetShardIndex"/> via
    /// <see cref="MergeManyAsync"/>, preserving HLC timestamps for CRDT-safe
    /// convergence. Reads continue to be served locally.
    /// <para>
    /// Idempotent: if the shard is already in <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/>
    /// or <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Drain"/> with a matching
    /// <paramref name="targetShardIndex"/> and <paramref name="movedSlots"/>, the call
    /// is a no-op.
    /// </para>
    /// </summary>
    Task BeginSplitAsync(int targetShardIndex, int[] movedSlots, int virtualShardCount);

    /// <summary>
    /// Transitions this shard's in-progress split to the <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/>
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

    /// <summary>Returns <c>true</c> if this shard is currently participating in an adaptive split as source.</summary>
    Task<bool> IsSplittingAsync();

    /// <summary>
    /// Returns <c>true</c> if this shard has a pending bulk-load or bulk-append
    /// operation that has not yet been fully grafted into the tree. Used by the
    /// auto-split monitor to suppress splits while bulk operations are mid-flight.
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> because the implementation is a
    /// pure synchronous read of <c>state.State.PendingBulkGraft</c> wrapped in
    /// <see cref="Task.FromResult{TResult}(TResult)"/> with zero awaits and zero
    /// state mutation - it cannot race any other in-flight turn. Paired with
    /// <see cref="GetHotnessAsync"/> so the hot-shard monitor's per-tick
    /// fan-out (which awaits both) is not gated on producer
    /// <see cref="SetManyAsync"/> work (see U9d).
    /// </para>
    /// </summary>
    [AlwaysInterleave]
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
    /// Like <see cref="CountForSlotsAsync(int[], int)"/>, but additionally
    /// restricts the count to keys in the half-open range
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// A <see langword="null"/> bound is unbounded on that side. Both the
    /// per-slot ownership filter and the range bound must hold for a key to
    /// count, so the post-split ranged total stays exact against the
    /// authoritative <see cref="ShardMap"/>.
    /// </summary>
    Task<int> CountForSlotsAsync(int[] sortedSlots, int virtualShardCount, string? startInclusive, string? endExclusive);

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
    /// apply the shard's <c>MovedAwaySlots</c> filter - the caller has explicitly
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
        int virtualShardCount,
        Orleans.Lattice.LatticePredicateNode? predicate = null);

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
    /// apply the shard's <c>MovedAwaySlots</c> filter - the caller has explicitly
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
        int virtualShardCount,
        Orleans.Lattice.LatticePredicateNode? predicate = null);

    // ==========================================================================
    //  Online shadow-forwarding primitive
    // ==========================================================================
    //  Used by the coordinator that drives an online copy between physical
    //  trees (e.g. online resize). During the operation, every accepted
    //  mutation on this shard is mirrored in parallel to the corresponding
    //  shard on the destination tree. After the registry alias has been
    //  atomically swapped, the shard rejects new operations with
    //  StaleTreeRoutingException so the caller's LatticeGrain can refresh
    //  its cached routing snapshot and retry against the destination tree.
    //  The four transitions are idempotent under a matching operationId and
    //  refused under a mismatched one.

    /// <summary>
    /// Transitions this shard into the <see cref="ShadowForwardPhase.Draining"/>
    /// phase, mirroring every accepted mutation to the corresponding shard on
    /// <paramref name="destinationPhysicalTreeId"/>. Idempotent for a matching
    /// <paramref name="operationId"/> - repeated calls are no-ops. Refused with
    /// <see cref="InvalidOperationException"/> if the shard is already in a
    /// shadow-forward lifecycle under a different <paramref name="operationId"/>.
    /// </summary>
    /// <param name="destinationPhysicalTreeId">Destination physical tree ID
    /// whose same-indexed shard receives every mirrored mutation.</param>
    /// <param name="operationId">Coordinator-supplied operation ID. Used for
    /// idempotent re-entry and to refuse interference from a stale coordinator.</param>
    /// <param name="logicalTreeId">User-visible logical tree ID. Stamped into
    /// <see cref="StaleTreeRoutingException.LogicalTreeId"/> when the shard
    /// later transitions to <see cref="ShadowForwardPhase.Rejecting"/>, so
    /// callers receive the correct tree name to refresh. May be an empty
    /// string, in which case the physical tree ID is used as a fallback.</param>
    Task BeginShadowForwardAsync(string destinationPhysicalTreeId, string operationId, string logicalTreeId);

    /// <summary>
    /// Transitions this shard from <see cref="ShadowForwardPhase.Draining"/>
    /// to <see cref="ShadowForwardPhase.Drained"/>. Called by the coordinator
    /// after the background drain for this shard has completed. Mutation
    /// forwarding continues until <see cref="EnterRejectingAsync"/> is called.
    /// Idempotent; refused with <see cref="InvalidOperationException"/> on an
    /// <paramref name="operationId"/> mismatch.
    /// </summary>
    Task MarkDrainedAsync(string operationId);

    /// <summary>
    /// Transitions this shard into <see cref="ShadowForwardPhase.Rejecting"/>.
    /// Called by the coordinator after the registry alias has been atomically
    /// redirected to the destination tree. Subsequent operations against this
    /// shard throw <see cref="StaleTreeRoutingException"/>. Idempotent;
    /// refused with <see cref="InvalidOperationException"/> on an
    /// <paramref name="operationId"/> mismatch.
    /// </summary>
    Task EnterRejectingAsync(string operationId);

    /// <summary>
    /// Clears this shard's shadow-forward state entirely. Used by the
    /// coordinator during undo, or at the end of the operation once the
    /// destination tree has fully taken over and the source is about to be
    /// torn down. Idempotent; refused with <see cref="InvalidOperationException"/>
    /// if the persisted state has a different <paramref name="operationId"/>
    /// (so a stale coordinator cannot wipe a newer operation's state).
    /// </summary>
    Task ClearShadowForwardAsync(string operationId);

    // ==========================================================================
    //  Restore shadow-cutover - retained-previous-tree redirect primitive
    // ==========================================================================

    /// <summary>
    /// Marks this shard's physical tree as retained-but-superseded by a
    /// shadow-cutover restore, redirecting logical-alias-routed traffic to
    /// <paramref name="destinationPhysicalTreeId"/>. After this call a
    /// logical-routed operation on this shard throws
    /// <see cref="StaleTreeRoutingException"/> so a stale routing activation
    /// self-heals onto the destination tree; direct-physical access (the
    /// revert path reading this tree by its physical ID) and internal
    /// maintenance keep reading the retained snapshot. Idempotent for a
    /// matching <paramref name="operationId"/>; a call under a different
    /// <paramref name="operationId"/> overwrites the redirect (a newer restore
    /// supersedes an older one).
    /// </summary>
    /// <param name="destinationPhysicalTreeId">Physical tree ID that now owns
    /// the logical alias.</param>
    /// <param name="operationId">Restore operation ID installing the redirect.</param>
    /// <param name="logicalTreeId">User-visible logical tree ID whose alias was
    /// redirected. Stamped into the thrown
    /// <see cref="StaleTreeRoutingException.LogicalTreeId"/>. May be empty, in
    /// which case the physical tree ID is used as a fallback.</param>
    Task MarkRetainedRedirectAsync(string destinationPhysicalTreeId, string operationId, string logicalTreeId);

    /// <summary>
    /// Clears a redirect previously installed by
    /// <see cref="MarkRetainedRedirectAsync"/>. Idempotent: clearing when no
    /// redirect is present is a no-op. Refused with
    /// <see cref="System.InvalidOperationException"/> when a redirect exists
    /// under a different <paramref name="operationId"/>, so a stale coordinator
    /// cannot clear a newer restore's redirect.
    /// </summary>
    /// <param name="operationId">Restore operation ID that installed the
    /// redirect being cleared.</param>
    Task ClearRetainedRedirectAsync(string operationId);

    // ==========================================================================
    //  Saga prepare/commit-broadcast terminal-mark primitive
    // ==========================================================================
    //  Used by AtomicWriteGrain to broadcast a single linearization mark to
    //  every WAL shard a SetManyAtomicAsync saga touched, after every
    //  prepare-phase per-key write has appended (with IsPrepared=true) to
    //  the per-shard WAL. Exactly one TxCommit (or TxAbort) per touched
    //  shard, never per-key. Composes with shadow-forwarding so a shard
    //  in a migration window mirrors the terminal mark to its destination
    //  in the same call shape as a normal mutation.

    /// <summary>
    /// Appends a single saga-terminal mark to this shard's per-shard WAL
    /// - a <see cref="MutationKind.TxCommit"/> when
    /// <paramref name="committed"/> is <c>true</c>, otherwise a
    /// <see cref="MutationKind.TxAbort"/>. The mark surfaces to every leaf
    /// on this shard via the WAL replay path and flips (or drops) every
    /// pending-transaction entry under the matching
    /// <paramref name="transactionId"/>. Exactly one terminal mark per
    /// touched shard per saga; the saga's commit-broadcast loop fans out
    /// in parallel across every shard the prepare phase touched.
    /// Idempotent: re-appending the same terminal mark on a shard that
    /// has already seen one is a no-op via the leaf-side recently-terminal
    /// dedup set.
    /// </summary>
    /// <param name="transactionId">
    /// The saga's persisted transaction id (matching every
    /// prepare-phase per-key emit's
    /// <see cref="LatticeMutation.TransactionId"/>).
    /// </param>
    /// <param name="committed">
    /// <c>true</c> to flip every pending-tx entry under the matching id
    /// into the visible projection (<see cref="MutationKind.TxCommit"/>);
    /// <c>false</c> to drop every pending-tx entry without it ever
    /// becoming visible (<see cref="MutationKind.TxAbort"/>).
    /// </param>
    /// <param name="cancellationToken">
    /// Cooperative cancellation for the WAL append. Cancellation does
    /// not roll back a successfully-appended terminal mark - by then the
    /// linearization decision has been published to the WAL.
    /// </param>
    /// <param name="committedValues">
    /// Optional cross-migration LWW backstop payload. When non-null and
    /// <paramref name="committed"/> is <c>true</c>, the shard root groups the
    /// dictionary by leaf grain id (via the per-key traversal used by
    /// <see cref="SetAsync"/>) and passes each leaf its subset to
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.ApplyTxTerminalAsync"/>. The leaf applies
    /// the values as a LWW-safe write only when it holds no pending bucket
    /// under <paramref name="transactionId"/> - i.e. when a prepare-phase
    /// shadow-forward was dropped by a mid-saga shard-split / drain race.
    /// The dictionary is also threaded through the shadow-forward and
    /// split-shadow-forward mirrors so every destination shard observes
    /// the backstop alongside its own per-shard terminal. Passing
    /// <c>null</c> matches the pre-backstop call shape and remains
    /// supported for wire compatibility.
    /// </param>
    /// <param name="inlineWalAppend">
    /// When <c>true</c> (the default), the shard appends the
    /// constructed terminal-mark <see cref="WalRecord"/> to its WAL
    /// partition before returning - the historical durability shape
    /// every direct caller (cross-cluster replay, shadow-forward,
    /// retroactive prepared-mutation sweep, unit tests) relies on.
    /// When <c>false</c>, the shard builds the record but does not
    /// write it; the record is returned to the caller, which is
    /// expected to durably persist it (e.g. by batching across every
    /// touched shard in one <see cref="Orleans.Lattice.BPlusTree.Grains.ICommitLogWriter.AppendManyAsync"/>
    /// call). Only the saga coordinator
    /// (<see cref="Orleans.Lattice.BPlusTree.Grains.AtomicWriteGrain"/>)
    /// opts out, because it is the unique caller that has every
    /// touched shard's record in hand and can collapse the per-shard
    /// serialised WAL fan-out into a per-partition batched dispatch.
    /// The saga still awaits the batched write before returning to
    /// its own caller, so the WAL durability invariant is preserved.
    /// </param>
    /// <returns>
    /// The constructed terminal-mark <see cref="WalRecord"/> when
    /// <paramref name="inlineWalAppend"/> is <c>false</c> and the shard
    /// has not written it; otherwise <c>null</c>. The shard always
    /// returns <c>null</c> when the WAL adapter is not registered
    /// (single-node / unit-test path), because there is nothing for
    /// the caller to persist.
    /// </returns>
    Task<WalRecord?> AppendTxTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues = null,
        CancellationToken cancellationToken = default,
        bool inlineWalAppend = true);

    /// <summary>
    /// Returns this shard's transitive split-forward destination set -
    /// the union of (a) the in-flight split's
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitInProgress.ShadowTargetShardIndex"/> when one
    /// is recorded and (b) every distinct value in
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/>, with this shard's
    /// own index excluded. Used by the saga's commit-broadcast loop and
    /// by the cross-cluster replication-apply path to pre-resolve the
    /// transitive closure of split destinations before fanning out
    /// terminal marks in parallel, so a deep cascading-split chain does
    /// not collapse into an unbounded recursive RPC depth on the
    /// receiving shard root. Returns an empty list when this shard has
    /// never shadow-forwarded any prepared writes (no in-flight split
    /// recorded and an empty moved-away-slots map).
    /// </summary>
    Task<List<int>> GetSplitForwardTargetsAsync();

    // ==========================================================================
    //  Compaction dirty-leaf fast-path primitive
    // ==========================================================================
    //  Used by TombstoneCompactionGrain to skip activating leaves with no
    //  recent deletes. The shard root populates an in-state dirty set as
    //  Delete mutations route through SetAsync/DeleteAsync/DeleteRangeAsync;
    //  the coordinator pulls a snapshot at the start of each shard's first
    //  batch and clears it (HLC-gated) when the shard's walk completes, so
    //  deletes that arrive mid-pass are retained for the next pass.

    /// <summary>
    /// Returns the current dirty-leaves set for this shard - the leaf grain
    /// ids that have observed at least one routed <c>Delete</c> mutation
    /// since the most recent successful drain - paired with the HLC
    /// watermark at the moment of capture. The compaction coordinator
    /// walks the returned leaves directly instead of activating every
    /// leaf in the shard's chain. An empty list with a zero
    /// <see cref="DirtyLeavesSnapshot.ObservedAdvance"/> signals the
    /// coordinator to fall back to the legacy chain walk; the leaf-walk
    /// fallback then exercises the fast path on the next pass once the
    /// shard root has accumulated dirty signal.
    /// </summary>
    Task<DirtyLeavesSnapshot> GetDirtyLeavesSinceLastCompactionAsync();

    /// <summary>
    /// Drains the shard-root dirty-leaves set up to (and including) the
    /// HLC watermark <paramref name="advance"/>. Entries whose recorded
    /// mark HLC compares as less-than-or-equal to <paramref name="advance"/>
    /// are removed; entries marked with a strictly greater HLC are
    /// preserved so the next compaction pass picks them up. Persists the
    /// new <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.LastDirtyAdvance"/> watermark
    /// alongside the trimmed dictionary in a single state write.
    /// Idempotent: a second call with the same or earlier watermark is
    /// a no-op.
    /// </summary>
    /// <param name="advance">The HLC watermark observed by the
    /// coordinator's snapshot. Entries marked at or before this HLC
    /// are removed.</param>
    Task ClearDirtyLeavesUpToAsync(HybridLogicalClock advance);

    /// <summary>
    /// Engages a durable write fence on this shard for the cross-cluster saga
    /// <paramref name="sagaId"/>. While engaged, every mutation routed through
    /// this shard is refused with a
    /// <see cref="Orleans.Lattice.LatticeWriteFencedException"/> until the
    /// fence is lifted or <paramref name="deadlineTicks"/> (an absolute UTC
    /// tick) passes, whichever comes first; reads are unaffected. Idempotent:
    /// a re-engage for the same saga refreshes the deadline; an engage for a
    /// different saga while one is already engaged is refused so a fence cannot
    /// be silently reassigned mid-cutover.
    /// </summary>
    /// <param name="sagaId">Identifier of the saga engaging the fence.</param>
    /// <param name="deadlineTicks">Absolute UTC tick at which the fence self-lifts.</param>
    Task EngageWriteFenceAsync(string sagaId, long deadlineTicks);

    /// <summary>
    /// Lifts the write fence previously engaged for <paramref name="sagaId"/>.
    /// Idempotent: lifting an already-lifted fence, or a fence engaged by a
    /// different saga, is a no-op so a late terminal decision cannot clear a
    /// newer fence.
    /// </summary>
    /// <param name="sagaId">Identifier of the saga whose fence to lift.</param>
    Task LiftWriteFenceAsync(string sagaId);

    /// <summary>
    /// Reports whether this shard currently refuses writes because a
    /// write fence is engaged and its deadline has not yet passed.
    /// </summary>
    Task<bool> IsWriteFencedAsync();
}
