namespace Orleans.Lattice;

using Orleans.Lattice.Primitives;

/// <summary>
/// Public entry point for a distributed B+ tree.
/// A stateless-worker grain that routes requests to the correct shard root
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c> — the tree this grain manages.
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
    /// Atomically writes <paramref name="entries"/> as a saga: reads each key's
    /// pre-saga value up front, applies the writes sequentially, and
    /// compensates (reverts) any already-committed entries if a subsequent
    /// write fails — so the batch is either fully applied or fully rolled back
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
    /// completes — the original failure's message is included.
    /// </para>
    /// </summary>
    /// <param name="entries">The key-value pairs to write atomically.</param>
    /// <param name="cancellationToken">Cancels orchestration before the saga is submitted. Once the saga has accepted the batch it drives itself to a terminal state via reminders and is not cooperatively cancelled.</param>
    Task SetManyAtomicAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default);

    /// <summary>Deletes the value for <paramref name="key"/>. Returns <c>true</c> if it existed.</summary>
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes all keys within the lexicographic range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)
    /// by walking the leaf chain and tombstoning matching entries in bulk.
    /// Returns the total number of keys that were tombstoned across all shards.
    /// </summary>
    Task<int> DeleteRangeAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default);

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
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, bool? prefetch = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns all live key-value entries in the tree as an ordered async stream.
    /// Entries are returned in lexicographic key order (or reverse if <paramref name="reverse"/> is <c>true</c>).
    /// Optionally filters to keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// </summary>
    IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Bulk-loads key-value pairs into an empty tree, building leaves and
    /// internal nodes bottom-up without any splits. Significantly faster than
    /// individual <see cref="SetAsync"/> calls for initial data seeding.
    /// Entries do not need to be pre-sorted; the implementation sorts them internally.
    /// Throws <see cref="InvalidOperationException"/> if any shard already contains data.
    /// </summary>
    Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Soft-deletes the entire tree. All shards are immediately marked as deleted,
    /// causing subsequent reads and writes to throw <see cref="InvalidOperationException"/>.
    /// A grain reminder is registered to permanently purge all tree data after the
    /// configured <see cref="LatticeOptions.SoftDeleteDuration"/> has elapsed.
    /// Idempotent — calling on an already-deleted tree is a no-op.
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
    /// The tree ID is preserved — it becomes an alias to the new physical tree.
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
    /// available; the result is a best-effort point-in-time copy.
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
    /// Merges all entries from <paramref name="sourceTreeId"/> into this tree
    /// using LWW semantics, preserving original timestamps. For each key present
    /// in both trees, the entry with the higher <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/>
    /// timestamp wins. Tombstones are also merged, ensuring deletes propagate correctly.
    /// <para>
    /// The source tree remains unmodified. Source and target trees may have different
    /// shard counts — entries are re-hashed to the correct target shard during merge.
    /// </para>
    /// </summary>
    /// <param name="sourceTreeId">The tree to merge from. Must exist and differ from this tree.</param>
    /// <param name="cancellationToken">Cancels orchestration before the merge coordinator is submitted. Once the coordinator accepts the request it runs to completion via reminders.</param>
    Task MergeAsync(string sourceTreeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no merge operation is in progress for this tree —
    /// either the most recent merge has completed or no merge has ever been initiated.
    /// </summary>
    Task<bool> IsMergeCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no snapshot operation is in progress for this tree —
    /// either the most recent snapshot has completed or no snapshot has ever been initiated.
    /// </summary>
    Task<bool> IsSnapshotCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if no resize operation is in progress for this tree —
    /// either the most recent resize has completed or no resize has ever been initiated.
    /// </summary>
    Task<bool> IsResizeCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Online reshard — grows the tree to <paramref name="newShardCount"/>
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
    /// less than or equal to <see cref="LatticeOptions.VirtualShardCount"/>.
    /// Throws <see cref="ArgumentOutOfRangeException"/> otherwise.
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
    /// Returns <c>true</c> if no reshard operation is in progress for this tree —
    /// either the most recent reshard has completed or no reshard has ever been initiated.
    /// </summary>
    Task<bool> IsReshardCompleteAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the effective routing context for this tree — the resolved
    /// physical tree ID (after registry alias resolution) and the per-tree
    /// <see cref="ShardMap"/>. Used by infrastructure helpers (e.g. the
    /// streaming bulk loader) that need to address shard grains directly
    /// without re-implementing alias resolution and shard-map fetching.
    /// </summary>
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    Task<RoutingInfo> GetRoutingAsync(CancellationToken cancellationToken = default);

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
    /// <returns>An opaque cursor handle scoped to this tree.</returns>
    Task<string> OpenKeyCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a stateful entry-enumeration cursor. Semantically identical to
    /// <see cref="OpenKeyCursorAsync"/> but yields
    /// <see cref="KeyValuePair{TKey,TValue}"/> via <see cref="NextEntriesAsync"/>.
    /// </summary>
    Task<string> OpenEntryCursorAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false, CancellationToken cancellationToken = default);

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
    /// <see cref="LatticeCursorDeleteProgress.IsComplete"/> becomes <c>true</c> —
    /// subsequent calls are idempotent no-ops.
    /// </summary>
    Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(string cursorId, int maxToDelete, CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the cursor identified by <paramref name="cursorId"/>, clears
    /// its persisted state, and releases the underlying grain activation.
    /// Idempotent — calling on an unknown or already-closed cursor is a
    /// no-op.
    /// </summary>
    Task CloseCursorAsync(string cursorId, CancellationToken cancellationToken = default);
}
