namespace Orleans.Lattice;

using Orleans.Lattice.Primitives;

/// <summary>
/// Public entry point for a distributed B+ tree.
/// A stateless-worker grain that routes requests to the correct shard
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c> - the tree this grain manages.
/// </summary>
[Alias(TypeAliases.ILattice)]
public interface ILattice : IGrainWithStringKey
{
    /// <summary>Gets the value associated with <paramref name="key"/>, or <c>null</c> if not found.</summary>
    Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the value and its <see cref="HybridLogicalClock"/> version for
    /// <paramref name="key"/>. Returns a <see cref="VersionedValue"/> with <c>null</c>
    /// value and <see cref="HybridLogicalClock.Zero"/> version when the key is absent
    /// or tombstoned. Use the returned version with <see cref="SetIfVersionAsync"/>
    /// for optimistic concurrency.
    /// </summary>
    Task<VersionedValue> GetWithVersionAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned.</summary>
    Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>, fanning out to shards in parallel.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken = default);

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts or updates the value for <paramref name="key"/> with a time-to-live
    ///. The entry is treated as tombstoned on all reads
    /// (<see cref="GetAsync"/>, <see cref="ExistsAsync"/>, <see cref="GetManyAsync"/>,
    /// <see cref="KeysAsync"/>, <see cref="EntriesAsync"/>, <see cref="CountAsync"/>, 
    /// etc.) once <paramref name="ttl"/> has elapsed since the server-side write.
    /// Expired entries are reaped by background tombstone compaction after the
    /// configured <see cref="LatticeOptions.TombstoneGracePeriod"/>.
    /// <para>
    /// The TTL is converted to an absolute UTC expiry at write time on the silo
    /// handling the call, so clock skew between clients does not shift individual
    /// entries' lifetimes. Throws <see cref="ArgumentOutOfRangeException"/> when
    /// <paramref name="ttl"/> is negative or zero.
    /// </para>
    /// </summary>
    /// <param name="key">The key to write.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="ttl">How long the entry remains live. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the routing and shard dispatch. Once the write lands on a shard it completes normally.</param>
    Task SetAsync(string key, byte[] value, TimeSpan ttl, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the entry's
    /// current <see cref="HybridLogicalClock"/> matches <paramref name="expectedVersion"/>.
    /// Returns <c>true</c> if the write was applied, <c>false</c> if the version did not
    /// match (another writer updated the key since it was read). Use
    /// <see cref="GetWithVersionAsync"/> to obtain the current version for the first attempt.
    /// For a new key, pass <see cref="HybridLogicalClock.Zero"/> as the expected version.
    /// </summary>
    Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a producer-side typed CRDT delta to <paramref name="key"/>
    /// under the declared <paramref name="mode"/>. The owning leaf
    /// resolves the matching <see cref="CrdtShape"/> from the registered
    /// <see cref="CrdtShapeRegistry"/>, decodes the current state, folds
    /// the delta into the state via the shape's <c>MergeDelta</c>, and
    /// appends a single WAL record whose <see cref="WalRecord.Delta"/>
    /// slot carries the producer's typed delta bytes verbatim. Returns
    /// the <see cref="HybridLogicalClock"/> stamped on the committed
    /// entry. CRDT delta merges are convergent under any interleaving,
    /// so this surface deliberately omits the optimistic-CAS guard that
    /// <see cref="SetIfVersionAsync"/> carries. Hosts using OR-Map at
    /// this tree must register the matching <c>(TKey, TValue)</c> via
    /// <c>ISiloBuilder.AddOrMapShape</c>; closed-shape modes
    /// (<see cref="LatticeMergeMode.OrSet"/>,
    /// <see cref="LatticeMergeMode.PnCounter"/>,
    /// <see cref="LatticeMergeMode.VersionVector"/>,
    /// <see cref="LatticeMergeMode.MvRegister"/>) resolve through the
    /// registry's global fallback without per-tree registration.
    /// <see cref="LatticeMergeMode.LwwRegister"/> is rejected with
    /// <see cref="ArgumentException"/>.
    /// </summary>
    /// <param name="key">The key to apply the delta against.</param>
    /// <param name="mode">The CRDT merge mode declaring the delta's typed shape.</param>
    /// <param name="deltaBytes">The Orleans-serialised typed delta DTO bytes.</param>
    /// <param name="cancellationToken">Cancels the per-key dispatch.</param>
    Task<HybridLogicalClock> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns the existing value when the key is
    /// already live, or <c>null</c> when the value was newly written.
    /// </summary>
    Task<byte[]?> GetOrSetAsync(string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts or updates multiple key-value pairs, fanning out to shards in parallel.
    /// <para>
    /// <b>Not atomic.</b> A partial failure leaves the batch half-applied with no
    /// compensating rollback. Use <see cref="SetManyAtomicAsync"/> when all-or-nothing
    /// semantics are required.
    /// </para>
    /// </summary>
    Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Conditional bulk write: writes each entry only if the key's
    /// <b>current</b> stored value satisfies the server-side predicate IR
    /// <paramref name="predicate"/> (a compare-then-set guard evaluated once,
    /// server-side, at write time against each key's JSON document view). A key
    /// with no live stored value is treated as non-matching and is skipped.
    /// Returns the set of keys actually written, so the caller can distinguish
    /// guarded-out keys. Committed entries become ordinary Set writes that
    /// replicate without re-evaluating the predicate.
    /// <para>
    /// <b>Not atomic.</b> Like <see cref="SetManyAsync"/>, a partial failure
    /// leaves the batch half-applied; use the guarded atomic variant when
    /// all-or-nothing semantics are required. Intended to be reached through
    /// the typed <c>SetManyAsync&lt;T&gt;</c> extension, which compiles the
    /// predicate expression to IR.
    /// </para>
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<IReadOnlyList<string>> SetManyWherePredicateAsync(List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically writes <paramref name="entries"/> as a saga: reads each key's
    /// pre-saga value up front, applies the writes sequentially, and
    /// compensates (reverts) any already-committed entries if a subsequent
    /// write fails - so the batch is either fully applied or fully rolled back
    /// from the caller's perspective. Crash-recovery is reminder-driven: a
    /// silo failure mid-saga reactivates the coordinator grain on another silo
    /// which resumes from its persisted progress, optionally compensating.
    /// <para>
    /// <b>Partial-visibility window.</b> Readers observing the tree between the
    /// first and last committed write may see a partial view of the batch.
    /// This is inherent to the saga pattern; callers needing strict isolation
    /// should layer version-guarded reads
    /// (<see cref="GetWithVersionAsync"/> + <see cref="SetIfVersionAsync"/>)
    /// on top.
    /// </para>
    /// <para>
    /// Throws <see cref="ArgumentException"/> when <paramref name="entries"/>
    /// contains duplicate keys or null values. Throws
    /// <see cref="InvalidOperationException"/> if a write fails and compensation
    /// completes - the original failure's message is included.
    /// </para>
    /// </summary>
    /// <param name="entries">The key-value pairs to write atomically.</param>
    /// <param name="cancellationToken">Cancels orchestration before the saga is submitted. Once the saga has accepted the batch it drives itself to a terminal state via reminders and is not cooperatively cancelled.</param>
    Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Caller-supplied idempotency-key overload of
    /// <see cref="SetManyAtomicAsync(List{KeyValuePair{string, byte[]}}, CancellationToken)"/>.
    /// The saga is keyed by <c>{treeId}/{operationId}</c>, so re-submitting
    /// with the same <paramref name="operationId"/> re-attaches to the
    /// original saga: if it has already completed the call returns
    /// immediately; if it is still in flight the call awaits its terminal
    /// state. This turns a transport-level timeout or silo crash mid-call
    /// into a recoverable retry - the client simply calls again with the
    /// same <paramref name="operationId"/>.
    /// <para>
    /// <b>Key-set stability.</b> An <paramref name="operationId"/> is bound
    /// to the exact set of keys submitted on the first call. Re-submitting
    /// the same <paramref name="operationId"/> with a different set of keys
    /// (added, removed, or renamed) throws
    /// <see cref="InvalidOperationException"/>. Reordering the keys or
    /// changing their values is allowed - the fingerprint hashes the
    /// sorted key set only.
    /// </para>
    /// <para>
    /// <b>Retention.</b> Completed saga state is retained for
    /// <see cref="LatticeOptions.AtomicWriteRetention"/> (default 48h) so
    /// delayed retries within the window still observe the original
    /// outcome. After the retention window the saga is purged and the
    /// same <paramref name="operationId"/> becomes eligible for a fresh
    /// saga.
    /// </para>
    /// </summary>
    /// <param name="entries">The key-value pairs to write atomically.</param>
    /// <param name="operationId">Stable caller-supplied idempotency key. Must be non-empty and must not contain <c>'/'</c> (reserved as the grain-key separator).</param>
    /// <param name="cancellationToken">Cancels orchestration before the saga is submitted. Once the saga has accepted the batch it drives itself to a terminal state via reminders and is not cooperatively cancelled.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="operationId"/> is null, empty, whitespace, or contains <c>'/'</c>; or when <paramref name="entries"/> contains duplicate keys or null values.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="operationId"/> was previously submitted with a different key set, or when a write fails and compensation completes.</exception>
    Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, string operationId, CancellationToken cancellationToken = default);

    /// <summary>Deletes the value for <paramref name="key"/>. Returns <c>true</c> if it existed.</summary>
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all keys within the lexicographic range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)
    /// by walking the leaf chain and tombstoning matching entries in bulk.
    /// Returns the total number of keys that were tombstoned across all shards.
    /// </summary>
    Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="DeleteRangeAsync"/>, but tombstones only the in-range
    /// keys whose value matches the server-side predicate IR
    /// <paramref name="predicate"/>, evaluated once at write time against each
    /// candidate value's JSON document view inside the owning leaf. The matched
    /// key set is persisted to the WAL and shipped to replication consumers so
    /// replay and cross-cluster apply reproduce exactly that set without
    /// re-evaluating the predicate. Returns the total number of keys tombstoned
    /// across all shards. Intended to be reached through the typed
    /// <c>DeleteRangeAsync&lt;T&gt;</c> extension, which compiles the predicate
    /// expression to IR.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<int> DeleteRangeWherePredicateAsync(LatticePredicateNode predicate, string startInclusive, string endExclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the total number of live (non-tombstoned) keys across all shards.
    /// Fans out to every shard in parallel and sums the per-shard counts.
    /// </summary>
    Task<int> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of live (non-tombstoned) keys in each shard as an ordered list.
    /// The list index corresponds to the shard index (0-based).
    /// Useful for diagnostics and load-balancing analysis.
    /// </summary>
    Task<IReadOnlyList<int>> CountPerShardAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns all live keys in the tree as an ordered async stream.
    /// Keys are returned in lexicographic order (or reverse if <paramref name="reverse"/> is <c>true</c>).
    /// Optionally filters to keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// When <paramref name="prefetch"/> is <c>true</c> (or <c>null</c> and
    /// <see cref="LatticeOptions.PrefetchKeysScan"/> is enabled), the next page from
    /// each shard is fetched in parallel while the current page is being consumed.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="KeysAsync"/>, but evaluates the server-side predicate IR
    /// <paramref name="predicate"/> against each candidate value's JSON document
    /// view inside the owning leaf, yielding only the <b>keys</b> whose value
    /// matches. No values cross the wire. The predicate travels as an explicit
    /// argument (not ambient state), so it is applied consistently on every
    /// per-shard page and reconciliation drain across the whole scan.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    IAsyncEnumerable<string> KeysWherePredicateAsync(LatticePredicateNode predicate, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns all live key-value entries in the tree as an ordered async stream.
    /// Entries are returned in lexicographic key order (or reverse if <paramref name="reverse"/> is <c>true</c>).
    /// Optionally filters to keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// When <paramref name="prefetch"/> is <c>true</c> (or <c>null</c> and
    /// <see cref="LatticeOptions.PrefetchEntriesScan"/> is enabled), the next page from
    /// each shard is fetched in parallel while the current page is being consumed.
    /// Because entries carry <c>byte[]</c> values, pre-fetched pages hold extra
    /// memory proportional to <c>shardCount × KeysPageSize × avgValueSize</c>.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="EntriesAsync"/>, but evaluates the server-side predicate IR
    /// <paramref name="predicate"/> against each candidate value's JSON document
    /// view inside the owning leaf, yielding only the entries whose value matches.
    /// Non-matching values are dropped server-side before paging. The predicate
    /// travels as an explicit argument (not ambient state), so it is applied
    /// consistently on every per-shard page and reconciliation drain across the
    /// whole scan.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesWherePredicateAsync(LatticePredicateNode predicate, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Bulk-loads key-value pairs into an empty tree, building leaves and
    /// internal nodes bottom-up without any splits. This is a one-shot
    /// initial-import primitive: it requires every shard to be empty at call
    /// time and is not safe to call repeatedly against a continuously-fed
    /// tree. Significantly faster than individual <see cref="SetAsync"/>
    /// calls for initial data seeding. Entries do not need to be pre-sorted;
    /// the implementation sorts them internally.
    /// <para>
    /// Throws <see cref="InvalidOperationException"/> if any shard already
    /// contains data, so the second and subsequent calls always fail unless
    /// the operation id matches a previously-completed call (in which case
    /// the call is an idempotent no-op). Streaming append-style ingestion
    /// must use <see cref="SetAsync(string, byte[], CancellationToken)"/>
    /// or the streaming
    /// <c>BulkLoadAsync(IAsyncEnumerable&lt;...&gt;, IGrainFactory, int)</c>
    /// extension on <c>LatticeExtensions</c>, which routes to
    /// <c>ShardRootGrain.BulkAppendAsync</c> instead.
    /// </para>
    /// </summary>
    Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Soft-deletes the entire tree. All shards are immediately marked as deleted,
    /// causing subsequent reads and writes to throw <see cref="InvalidOperationException"/>.
    /// A grain reminder is registered to permanently purge all tree data after the
    /// configured <see cref="LatticeOptions.SoftDeleteDuration"/> has elapsed.
    /// Idempotent - calling on an already-deleted tree is a no-op.
    /// </summary>
    Task DeleteTreeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Recovers a soft-deleted tree, restoring it to normal operation.
    /// All data written before the delete is accessible again.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed (data is gone).
    /// </summary>
    Task RecoverTreeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Immediately purges a soft-deleted tree without waiting for the
    /// <see cref="LatticeOptions.SoftDeleteDuration"/> window to elapse.
    /// Permanently removes all leaf and internal node state.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed.
    /// </summary>
    Task PurgeTreeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Resizes the tree by creating an offline snapshot with new
    /// <see cref="LatticeOptions.MaxLeafKeys"/> and <see cref="LatticeOptions.MaxInternalChildren"/>
    /// values into a new physical tree, then swapping the tree alias so that all
    /// subsequent reads and writes are redirected to the resized tree. The old
    /// physical tree is soft-deleted and will be purged after the configured
    /// <see cref="LatticeOptions.SoftDeleteDuration"/>.
    /// <para>
    /// The tree ID is preserved - it becomes an alias to the new physical tree.
    /// During the snapshot phase, the tree is temporarily locked (offline snapshot).
    /// After the alias swap, the tree is immediately available with the new sizing.
    /// Cache invalidation is automatic: different physical trees produce different
    /// leaf grain IDs, which create fresh cache grain instances.
    /// </para>
    /// </summary>
    /// <param name="newMaxLeafKeys">The new maximum number of keys per leaf node. Must be greater than 1.</param>
    /// <param name="newMaxInternalChildren">The new maximum number of children per internal node. Must be greater than 2.</param>
    /// <param name="cancellationToken">Cancels orchestration before the resize coordinator is submitted. Once the coordinator accepts the request it runs to completion via reminders.</param>
    Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default);

    /// <summary>
    /// Undoes the most recent resize by recovering the old physical tree,
    /// removing the alias, restoring the original registry configuration,
    /// and deleting the new snapshot tree. Only available while the old tree
    /// is still within its <see cref="LatticeOptions.SoftDeleteDuration"/>
    /// window (before purge completes).
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no completed resize exists to undo, or if the old tree has
    /// already been purged.
    /// </exception>
    Task UndoResizeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a snapshot of this tree into a new tree with the given
    /// <paramref name="destinationTreeId"/>. All live key-value pairs are copied
    /// shard-by-shard into the destination tree.
    /// <para>
    /// In <see cref="SnapshotMode.Offline"/> mode, the source tree is locked
    /// (marked deleted) during the copy, guaranteeing a consistent snapshot.
    /// In <see cref="SnapshotMode.Online"/> mode, the source tree remains
    /// available for reads and writes throughout; the result is strictly
    /// consistent via shadow forwarding - every write accepted on the source
    /// before the snapshot completes is reflected on the destination.
    /// </para>
    /// <para>
    /// The source and destination trees must have the same <see cref="LatticeOptions.ShardCount"/>.
    /// The destination tree must not already exist.
    /// </para>
    /// </summary>
    /// <param name="destinationTreeId">The ID for the new tree. Must not already exist.</param>
    /// <param name="mode">Whether to lock the source tree during the snapshot.</param>
    /// <param name="maxLeafKeys">Optional leaf sizing for the destination. If <c>null</c>, uses the source tree's configured value.</param>
    /// <param name="maxInternalChildren">Optional internal node sizing for the destination. If <c>null</c>, uses the source tree's configured value.</param>
    /// <param name="cancellationToken">Cancels orchestration before the snapshot coordinator is submitted. Once the coordinator accepts the request it runs to completion via reminders.</param>
    Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if this tree is registered in the internal tree registry.
    /// A tree is registered on its first write and unregistered when its purge completes.
    /// </summary>
    Task<bool> TreeExistsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the IDs of all registered trees in sorted order.
    /// System-internal trees (prefixed with <c>_lattice_</c>) are excluded.
    /// Physical trees created by <see cref="ResizeAsync"/> and
    /// <see cref="SnapshotAsync"/> are included.
    /// </summary>
    Task<IReadOnlyList<string>> GetAllTreeIdsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets or clears the per-tree override for event publication. When an
    /// override is set it takes priority over the silo-wide
    /// <see cref="LatticeOptions.PublishEvents"/>; when cleared (<paramref name="enabled"/>
    /// is <c>null</c>) the tree falls back to the silo option. The override
    /// is persisted on the tree's registry entry and survives silo restarts.
    /// <para>
    /// Propagation is best-effort: the silo activation that handled this call
    /// observes the change immediately; other activations refresh their cached
    /// value within a few seconds. Writes in flight at the moment of the change
    /// may emit events under the previous setting.
    /// </para>
    /// </summary>
    /// <param name="enabled">
    /// <c>true</c> to force publication on for this tree, <c>false</c> to force
    /// it off, or <c>null</c> to remove the override and inherit the silo default.
    /// </param>
    /// <param name="cancellationToken">Cancels the registry write.</param>
    Task SetPublishEventsEnabledAsync(bool? enabled, CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges all entries from <paramref name="sourceTreeId"/> into this tree
    /// using LWW semantics, preserving original timestamps. For each key present
    /// in both trees, the entry with the higher <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/>
    /// timestamp wins. Tombstones are also merged, ensuring deletes propagate correctly.
    /// <para>
    /// The source tree remains unmodified. Source and target trees may have different
    /// shard counts - entries are re-hashed to the correct target shard during merge.
    /// </para>
    /// </summary>
    /// <param name="sourceTreeId">The tree to merge from. Must exist and differ from this tree.</param>
    /// <param name="cancellationToken">Cancels orchestration before the merge coordinator is submitted. Once the coordinator accepts the request it runs to completion via reminders.</param>
    Task MergeAsync(string sourceTreeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no merge operation is in progress for this tree -
    /// either the most recent merge has completed or no merge has ever been initiated.
    /// </summary>
    Task<bool> IsMergeCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no snapshot operation is in progress for this tree -
    /// either the most recent snapshot has completed or no snapshot has ever been initiated.
    /// </summary>
    Task<bool> IsSnapshotCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no resize operation is in progress for this tree -
    /// either the most recent resize has completed or no resize has ever been initiated.
    /// </summary>
    Task<bool> IsResizeCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Online reshard - grows the tree to <paramref name="newShardCount"/>
    /// distinct physical shards by iteratively splitting the largest-slot-owning
    /// existing shards. The tree continues to serve reads and writes throughout;
    /// every underlying split drains moved virtual slots online and then
    /// atomically swaps the <see cref="ShardMap"/>, so key routing shifts
    /// transparently. Returns once orchestration has been accepted by the
    /// coordinator grain; the migration then runs anchored by reminders so
    /// it survives silo restarts. Poll completion with
    /// <see cref="IsReshardCompleteAsync"/>.
    /// <para>
    /// <b>Grow-only.</b> <paramref name="newShardCount"/> must be strictly
    /// greater than the current number of distinct physical shards, and
    /// less than or equal to
    /// <see cref="Orleans.Lattice.BPlusTree.LatticeConstants.DefaultVirtualShardCount"/>
    /// (4096). Throws <see cref="ArgumentOutOfRangeException"/> otherwise.
    /// </para>
    /// <para>
    /// Idempotent: a call with the same <paramref name="newShardCount"/>
    /// while a reshard is in progress is a no-op. A call with a different
    /// target throws <see cref="InvalidOperationException"/>.
    /// </para>
    /// </summary>
    /// <param name="newShardCount">The desired number of distinct physical shards.</param>
    /// <param name="cancellationToken">Cancels orchestration before the reshard coordinator is submitted. Once the coordinator accepts the request it runs to completion via reminders.</param>
    Task ReshardAsync(int newShardCount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no reshard operation is in progress for this tree -
    /// either the most recent reshard has completed or no reshard has ever been initiated.
    /// </summary>
    Task<bool> IsReshardCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the effective routing context for this tree - the resolved
    /// physical tree ID (after registry alias resolution) and the per-tree
    /// <see cref="ShardMap"/>. Used by infrastructure helpers (e.g. the
    /// streaming bulk loader) that need to address shard grains directly
    /// without re-implementing alias resolution and shard-map fetching.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    ValueTask<RoutingInfo> GetRoutingAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Force-refresh overload of <see cref="GetRoutingAsync(CancellationToken)"/>.
    /// When <paramref name="forceRefresh"/> is <see langword="true"/>, the
    /// <see cref="LatticeGrain"/> activation's cached <see cref="ShardMap"/>,
    /// cached resolved physical tree id (alias), and <see cref="RoutingInfo"/>
    /// are all invalidated before the routing snapshot is re-resolved.
    /// Used by external coordinators (e.g. the atomic-write saga's
    /// <c>CaptureShardAsync</c> / <c>MarkOneShardAsync</c> retry loops)
    /// whose stale-routing recovery would otherwise spin against the
    /// activation's cached snapshot indefinitely - the
    /// <see cref="LatticeGrain"/> is a
    /// <see cref="Orleans.Placement.StatelessWorkerPlacement"/> with
    /// per-activation routing caching, and its private invalidation
    /// hooks only fire on the grain's own internal stale-routing throws,
    /// so an external caller cannot otherwise force the cache to refresh.
    /// Clearing the alias as well is required to escape a
    /// <see cref="StaleTreeRoutingException"/> retry loop after an online
    /// resize / reshard swapped the alias; refreshing only the shard map
    /// would re-resolve to the same stale physical tree id.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    ValueTask<RoutingInfo> GetRoutingAsync(bool forceRefresh, CancellationToken cancellationToken = default);

    // ── Diagnostics ─────────────────────────────────────

    /// <summary>
    /// Returns a <see cref="TreeDiagnosticReport"/> aggregating per-shard
    /// health - depth, live-key count, hotness, split/bulk state - plus a
    /// bounded ring buffer of recent adaptive-split events. Repeated calls
    /// are served from a short in-memory cache configured via
    /// <see cref="LatticeOptions.DiagnosticsCacheTtl"/> (default 5 seconds).
    /// <para>
    /// When <paramref name="deep"/> is <c>true</c>, each shard walks its leaf
    /// chain to aggregate tombstone-and-expired counts - populating
    /// <see cref="TreeDiagnosticReport.TotalTombstones"/> and the per-shard
    /// <see cref="ShardDiagnosticReport.Tombstones"/> /
    /// <see cref="ShardDiagnosticReport.TombstoneRatio"/> fields. Deep mode
    /// is cached independently of shallow mode; the trade-off is one grain
    /// call per leaf rather than one per shard.
    /// </para>
    /// </summary>
    /// <param name="deep">Whether to compute tombstone counts (walks the leaf chain per shard).</param>
    /// <param name="cancellationToken">Cancels the diagnostics fan-out before it begins.</param>
    Task<TreeDiagnosticReport> DiagnoseAsync(bool deep = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a byte-accurate <see cref="TreeStorageUsageReport"/> for this
    /// tree - the retained on-wire footprint across the three physical
    /// surfaces a tree occupies: write-ahead-log (WAL) rows, persisted
    /// snapshot blobs, and leaf/shard-root grain state. Unlike
    /// <see cref="DiagnoseAsync"/> (which reports entry counts), this surface
    /// reports exact retained bytes so operators can size storage and drive
    /// retention policy.
    /// <para>
    /// Repeated calls are served from a short in-memory cache configured via
    /// <see cref="LatticeOptions.StorageUsageCacheTtl"/> (default 10 seconds).
    /// A cache-miss fans out to every physical shard root (leaf-state and
    /// snapshot bytes) and every WAL partition (retained WAL bytes).
    /// </para>
    /// <para>
    /// When the configured <see cref="IWalStorageProvider"/> does not support
    /// byte accounting (it does not override
    /// <see cref="IWalStorageProvider.GetRetainedByteSizeAsync"/>), the WAL
    /// surface contributes <c>0</c> and
    /// <see cref="TreeStorageUsageReport.Partial"/> is set - the reported
    /// total is then a lower bound. The default in-memory provider and the
    /// Azure Table provider both support byte accounting.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the storage-usage fan-out before it begins.</param>
    Task<TreeStorageUsageReport> GetStorageUsageAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Pre-activates every <see cref="IShardRootGrain"/> for this tree so the
    /// first hot-path write does not pay first-touch Orleans activation cost
    /// (placement-directory round-trip, grain-storage <c>ReadStateAsync</c>,
    /// constructor execution) inline with caller latency. Resolves routing
    /// once, enumerates the physical shard indices from the current
    /// <see cref="ShardMap"/>, and issues a bounded-concurrency fan-out of
    /// the cheapest read-only probe on each shard root (currently
    /// <see cref="IShardRootGrain.IsDeletedAsync"/>). Returns once every probe
    /// has completed.
    /// <para>
    /// Idempotent and safe to call repeatedly: re-activating an already-live
    /// shard root is a placement-directory lookup plus a single
    /// in-activation field read. Failures during warm-up are propagated as
    /// an <see cref="AggregateException"/> via <c>Task.WhenAll</c>; the
    /// caller decides whether to fail-fast or proceed with a partially-warm
    /// tree.
    /// </para>
    /// <para>
    /// Intended as an operational hook for ingest gateways and benchmark
    /// silos that need a non-zero warm-start: call after the silo has
    /// reported ready but before the first external write lands, so the
    /// cold-start placement-directory storm is absorbed while the silo is
    /// idle rather than against producer-driven flush concurrency.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the warm-up fan-out before the next shard probe is dispatched. In-flight probes are not cooperatively cancelled.</param>
    Task WarmUpAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a deterministic XxHash128 <see cref="LeafProjectionDigest"/>
    /// for the physical shard at <paramref name="shardIndex"/>. Used by
    /// operators and chaos tests to detect cross-silo divergence the
    /// moment the per-shard write-ahead log (WAL) becomes the rebuild
    /// source of truth - two silos that have applied the same
    /// prefix of the same WAL produce byte-identical digests.
    /// <para>
    /// The shard's leaf chain is walked once and every leaf's digest is
    /// chained through XxHash128 so divergence at any leaf surfaces in the
    /// shard total. Reports the summed entry count and the summed
    /// projection-checkpoint offset across every leaf so a digest
    /// mismatch can be triaged quickly.
    /// </para>
    /// <para>
    /// Throws <see cref="InvalidOperationException"/> when the per-tree
    /// <see cref="LatticeOptions.MaintainProjectionDigest"/> opt-out is
    /// set to <c>false</c>: the persisted aggregates are not maintained
    /// in that mode, so polling the digest would return stale bytes; the
    /// API fails loudly instead.
    /// </para>
    /// </summary>
    /// <param name="shardIndex">The physical shard index resolved from the per-tree <c>ShardMap</c>.</param>
    /// <param name="cancellationToken">Cancels the leaf-chain walk before the next leaf.</param>
    Task<LeafProjectionDigest> GetLeafProjectionDigestAsync(int shardIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Operator-tooling rebuild: clears the materialised projection
    /// state (entries, projection hash, persisted checkpoint offset,
    /// pending-tx machinery) on every leaf in the shard's chain and
    /// forces each leaf to deactivate so its next activation replays
    /// the per-shard write-ahead log from offset <c>0</c> through the
    /// existing activation-time materialiser. Topology-bearing slots
    /// (tree id, shard index, sibling pointers, key range bounds,
    /// parent pointer, split markers) are preserved verbatim so the
    /// rebuild observes the same WAL-filter ownership context the
    /// pre-rebuild leaves used. Used after a corrupt-projection
    /// incident or a <see cref="LatticeOptions.MaxLeafReplayEntries"/>
    /// blow-out to recover the shard state from the durable WAL
    /// source of truth.
    /// <para>
    /// The operator surface deliberately does not expose "edit the
    /// projection in place" or "skip a WAL entry" - those would defeat
    /// the determinism contract. The recourse for a deterministically
    /// failing entry is the replication dead-letter queue or, for a
    /// structural rewrite, an explicit compensating WAL entry committed
    /// via <c>MutationCategory.Maintenance</c>.
    /// </para>
    /// <para>
    /// Failures propagate. A transient storage failure on a single leaf
    /// aborts the fan-out with the unaffected leaves' rebuilds already
    /// applied; the operation is safe to retry because every leaf
    /// rebuild is independently idempotent.
    /// </para>
    /// </summary>
    /// <param name="shardIndex">The physical shard index resolved from the per-tree <c>ShardMap</c>.</param>
    /// <param name="cancellationToken">Cancels the rebuild fan-out before the next leaf.</param>
    Task RebuildLeafProjectionAsync(int shardIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Operator-tooling tombstone-compaction request: schedules an
    /// out-of-cycle compaction pass scoped to a single physical shard,
    /// bypassing the per-shard cooldown gate that the policy-trigger
    /// path enforces. Useful for triage when an operator suspects a
    /// shard has accumulated a tombstone backlog and wants to reap
    /// before the next reminder tick. Returns <see langword="false"/>
    /// when compaction is disabled
    /// (<see cref="LatticeOptions.TombstoneGracePeriod"/> =
    /// <see cref="Timeout.InfiniteTimeSpan"/>) or when a pass is
    /// already in flight; returns <see langword="true"/> when the
    /// request was accepted and the coordinator transitioned into a
    /// scoped pass.
    /// </summary>
    /// <param name="shardIndex">The physical shard index resolved from the per-tree <c>ShardMap</c>.</param>
    /// <param name="cancellationToken">Cancels the dispatch before the coordinator grain call.</param>
    Task<bool> CompactShardAsync(int shardIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the largest materialiser-lag value across every
    /// physical shard of this tree - the gap between each shard's
    /// per-shard write-ahead-log head offset and the minimum
    /// projection-checkpoint offset across that shard's leaf chain.
    /// A non-zero return value means at least one shard's projection
    /// has not yet caught up to the durable WAL head and is the
    /// back-pressure signal that complements the replication
    /// receiver's apply-lag gauge.
    /// </summary>
    /// <param name="cancellationToken">Cancels the per-shard fan-out before the next shard.</param>
    Task<long> GetMaterialiserLagAsync(CancellationToken cancellationToken = default);

    // ── Stateful cursors ────────────────────────────────

    /// <summary>
    /// Opens a stateful key-enumeration cursor over the given range and
    /// returns an opaque cursor ID that callers pass to
    /// <see cref="NextKeysAsync"/> and <see cref="CloseCursorAsync"/>.
    /// Unlike <see cref="KeysAsync"/> (stateless, bounded by
    /// <see cref="LatticeOptions.MaxScanRetries"/>), a cursor checkpoints its
    /// progress server-side after every page so long-running scans survive
    /// silo failovers, client restarts, and topology changes (shard splits).
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the first key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the end of the tree.</param>
    /// <param name="reverse">When <c>true</c>, the cursor walks keys in descending lexicographic order.</param>
    /// <param name="pointInTime">
    /// When <c>true</c>, the cursor captures a saga-decision snapshot at
    /// open time and serves every subsequent <see cref="NextKeysAsync"/>
    /// page against that same snapshot, so a multi-page scan is
    /// linearizable against atomic-write sagas committing concurrently
    /// with the cursor. The captured snapshot is pinned by the
    /// per-tree TxRegistry against tombstone-prune eviction for the
    /// cursor's lifetime; a step past
    /// <see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/> throws
    /// <see cref="LatticeCursorSnapshotExpiredException"/>, and opening a
    /// point-in-time cursor when the registry-wide pin footprint cap
    /// would be exceeded throws
    /// <see cref="LatticeCursorRegistryPinExhaustedException"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the open before any state is persisted.</param>
    /// <returns>An opaque cursor handle scoped to this tree.</returns>
    Task<string> OpenKeyCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="OpenKeyCursorAsync"/>, but persists the predicate IR
    /// <paramref name="predicate"/> on the cursor spec so every page yields
    /// only keys whose value matches, server-side. The IR survives silo
    /// failover (the spec is persisted) and composes with point-in-time mode.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<string> OpenKeyCursorWherePredicateAsync(LatticePredicateNode predicate, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a stateful entry-enumeration cursor. Semantically identical to
    /// <see cref="OpenKeyCursorAsync"/> but yields
    /// <see cref="KeyValuePair{TKey,TValue}"/> via <see cref="NextEntriesAsync"/>.
    /// </summary>
    Task<string> OpenEntryCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="OpenEntryCursorAsync"/>, but persists the predicate IR
    /// <paramref name="predicate"/> on the cursor spec so every page yields
    /// only entries whose value matches, server-side.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<string> OpenEntryCursorWherePredicateAsync(LatticePredicateNode predicate, string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool pointInTime = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a zero-observable-writes snapshot key cursor over the given
    /// range. Every page is served against a tree-wide
    /// <see cref="LatticeSnapshotCoordinate"/> captured at open time, so
    /// foreground non-saga writes, atomic-write sagas, range deletes,
    /// replication apply, and topology changes that commit after the
    /// capture are all invisible to the cursor's view. The
    /// <see cref="LatticeOptions.MaxSnapshotReplayEntries"/> gate
    /// rejects opens whose projected WAL-replay cost would dominate
    /// the call; a stalled snapshot whose pin TTL elapses throws
    /// <see cref="LatticeSnapshotExpiredException"/> on the next step.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound, or <c>null</c> for the first key.</param>
    /// <param name="endExclusive">Exclusive upper bound, or <c>null</c> for the end of the tree.</param>
    /// <param name="reverse">When <c>true</c>, the cursor walks keys in descending lexicographic order.</param>
    /// <param name="cancellationToken">Cancels the open before any state is persisted.</param>
    /// <returns>An opaque cursor handle scoped to this tree.</returns>
    Task<string> OpenSnapshotKeyCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="OpenSnapshotKeyCursorAsync"/>, but persists the
    /// predicate IR <paramref name="predicate"/> on the cursor spec so every
    /// snapshot page yields only matching keys. The filter composes with the
    /// WAL-coordinate replay and the frozen saga-decision snapshot.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<string> OpenSnapshotKeyCursorWherePredicateAsync(LatticePredicateNode predicate, string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a zero-observable-writes snapshot entry cursor. Semantically
    /// identical to <see cref="OpenSnapshotKeyCursorAsync"/> but yields
    /// <see cref="KeyValuePair{TKey,TValue}"/> via
    /// <see cref="NextEntriesAsync"/>.
    /// </summary>
    Task<string> OpenSnapshotEntryCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="OpenSnapshotEntryCursorAsync"/>, but persists the
    /// predicate IR <paramref name="predicate"/> on the cursor spec so every
    /// snapshot page yields only matching entries.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<string> OpenSnapshotEntryCursorWherePredicateAsync(LatticePredicateNode predicate, string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a stateful, resumable range-delete cursor over
    /// [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// Each <see cref="DeleteRangeStepAsync"/> call tombstones at most
    /// <c>maxToDelete</c> keys and persists progress so the operation can be
    /// resumed across silo failovers. The unbounded
    /// <see cref="DeleteRangeAsync"/> remains available for short ranges.
    /// </summary>
    Task<string> OpenDeleteRangeCursorAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Like <see cref="OpenDeleteRangeCursorAsync"/>, but each
    /// <see cref="DeleteRangeStepAsync"/> tombstones only the in-range keys whose
    /// value matches the server-side predicate IR <paramref name="predicate"/>.
    /// The predicate is persisted on the cursor spec, so a durable cursor that
    /// reactivates after a silo failover re-applies the identical filter and the
    /// continuation tombstones the same logical set. Each step records its matched
    /// key set in the WAL so replay and replication reproduce it without
    /// re-evaluating the predicate. Intended to be reached through the typed
    /// <c>OpenDeleteRangeCursorAsync&lt;T&gt;</c> extension, which compiles the
    /// predicate expression to IR.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<string> OpenDeleteRangeCursorWherePredicateAsync(LatticePredicateNode predicate, string startInclusive, string endExclusive, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the next page of up to <paramref name="pageSize"/> keys from
    /// the cursor identified by <paramref name="cursorId"/>. Returns an empty
    /// page with <see cref="LatticeCursorKeysPage.HasMore"/> <c>false</c>
    /// once the cursor is fully drained. Throws
    /// <see cref="InvalidOperationException"/> if the cursor was not opened,
    /// has been closed, or was opened for a different kind of scan.
    /// </summary>
    Task<LatticeCursorKeysPage> NextKeysAsync(string cursorId, int pageSize, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the next page of up to <paramref name="pageSize"/> entries
    /// from the cursor identified by <paramref name="cursorId"/>. See
    /// <see cref="NextKeysAsync"/> for exhaustion and error semantics.
    /// </summary>
    Task<LatticeCursorEntriesPage> NextEntriesAsync(string cursorId, int pageSize, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances a delete-range cursor by up to <paramref name="maxToDelete"/>
    /// keys and returns the resulting progress. Safe to call again after
    /// <see cref="LatticeCursorDeleteProgress.IsComplete"/> becomes <c>true</c> -
    /// subsequent calls are idempotent no-ops.
    /// </summary>
    Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(string cursorId, int maxToDelete, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the cursor identified by <paramref name="cursorId"/>, clears
    /// its persisted state, and releases the underlying grain activation.
    /// Idempotent - calling on an unknown or already-closed cursor is a
    /// no-op.
    /// </summary>
    Task CloseCursorAsync(string cursorId, CancellationToken cancellationToken = default);
}
