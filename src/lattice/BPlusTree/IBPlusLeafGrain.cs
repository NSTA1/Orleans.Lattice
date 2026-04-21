namespace Orleans.Lattice.BPlusTree;

using Orleans.Lattice.Primitives;

/// <summary>
/// A leaf node grain in the B+ tree. Stores key-value pairs as
/// <see cref="Primitives.LwwValue{T}"/> entries for monotonic conflict resolution.
/// </summary>
[Alias(TypeAliases.IBPlusLeafGrain)]
internal interface IBPlusLeafGrain : IGrainWithGuidKey
{
    /// <summary>Gets the value for <paramref name="key"/>, or <c>null</c> if absent/tombstoned.</summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>
    /// Gets the value and its <see cref="Primitives.HybridLogicalClock"/> version for
    /// <paramref name="key"/>. Returns a <see cref="VersionedValue"/> with
    /// <c>null</c> value and <see cref="Primitives.HybridLogicalClock.Zero"/> version
    /// when the key is absent or tombstoned.
    /// </summary>
    Task<VersionedValue> GetWithVersionAsync(string key);

    /// <summary>Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned.</summary>
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys);

    /// <summary>
    /// Inserts or updates a key-value pair.
    /// Returns a <see cref="SplitResult"/> if the leaf split as a consequence, otherwise <c>null</c>.
    /// </summary>
    Task<SplitResult?> SetAsync(string key, byte[] value);

    /// <summary>
    /// Inserts or updates a key-value pair with an absolute expiry
    ///. The entry's <see cref="Orleans.Lattice.Primitives.LwwValue{T}.ExpiresAtTicks"/>
    /// is set to <paramref name="expiresAtTicks"/>; once the current UTC wall
    /// clock passes that value the entry is treated as tombstoned on reads
    /// and reaped by background compaction after the configured grace
    /// period. Pass <c>0</c> to write a non-expiring entry (equivalent to
    /// <see cref="SetAsync(string, byte[])"/>).
    /// </summary>
    Task<SplitResult?> SetAsync(string key, byte[] value, long expiresAtTicks);

    /// <summary>
    /// Returns the raw persisted entry for <paramref name="key"/> exactly as
    /// stored — including expiry metadata and tombstone flag — or
    /// <c>null</c> if the key has never been written to this leaf. Does
    /// <b>not</b> filter expired entries: this method is used by replication
    /// and split-shadow paths that must forward the authoritative record to
    /// another shard without stripping its TTL. Not for general reads — use
    /// <see cref="GetAsync"/> or <see cref="GetWithVersionAsync"/> instead.
    /// <para>
    /// Returns an <see cref="LwwEntry"/> rather than
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> directly because
    /// the Orleans type-alias encoder has a codec-generation race when a
    /// grain-interface signature uses
    /// <c>Task&lt;LwwValue&lt;byte[]&gt;?&gt;</c> — it intermittently emits
    /// malformed alias strings like <c>ol.lwv[[byte[]]]]]</c>. Wrapping in
    /// the flat <see cref="LwwEntry"/> DTO sidesteps the race.
    /// </para>
    /// </summary>
    Task<LwwEntry?> GetRawEntryAsync(string key);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns a <see cref="GetOrSetResult"/> containing
    /// the existing value when the key is live, or <c>null</c> existing value when the
    /// write was performed.
    /// </summary>
    Task<GetOrSetResult> GetOrSetAsync(string key, byte[] value);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the entry's
    /// current <see cref="Primitives.HybridLogicalClock"/> matches <paramref name="expectedVersion"/>.
    /// Returns a <see cref="CasResult"/> indicating whether the write was applied.
    /// </summary>
    Task<CasResult> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion);

    /// <summary>
    /// Inserts or updates multiple key-value pairs.
    /// Returns the last <see cref="SplitResult"/> if any split occurred, otherwise <c>null</c>.
    /// </summary>
    Task<SplitResult?> SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// </summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Tombstones all live keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// Returns a <see cref="RangeDeleteResult"/> containing the number of tombstoned keys
    /// and a <c>PastRange</c> flag indicating whether this leaf has observed any key
    /// <c>&gt;= endExclusive</c>. The shard-root coordinator uses <c>PastRange</c> to
    /// stop walking the leaf chain deterministically on sparse multi-shard trees, where
    /// a leaf may legitimately delete zero keys even when later leaves contain
    /// range-matching entries.
    /// </summary>
    Task<RangeDeleteResult> DeleteRangeAsync(string startInclusive, string endExclusive);

    /// <summary>Returns the number of live (non-tombstoned) keys in this leaf.</summary>
    Task<int> CountAsync();

    /// <summary>
    /// Returns a point-in-time count of live and tombstoned-or-expired entries
    /// in this leaf in a single call. Used by the diagnostics aggregation path
    /// (<see cref="ILattice.DiagnoseAsync"/>) to compute tombstone ratios without
    /// a second grain round-trip. Expired entries count toward
    /// <see cref="LeafStats.Tombstones"/> because they are reaped by the same
    /// compaction pass.
    /// </summary>
    Task<LeafStats> GetStatsAsync();

    /// <summary>Returns the grain identity of the right sibling leaf, or <c>null</c>.</summary>
    Task<GrainId?> GetNextSiblingAsync();

    /// <summary>Sets the right sibling pointer (called during splits).</summary>
    Task SetNextSiblingAsync(GrainId? siblingId);

    /// <summary>Returns the grain identity of the left sibling leaf, or <c>null</c>.</summary>
    Task<GrainId?> GetPrevSiblingAsync();

    /// <summary>Sets the left sibling pointer (called during splits).</summary>
    Task SetPrevSiblingAsync(GrainId? siblingId);

    /// <summary>
    /// Associates this leaf with a tree, enabling named options resolution.
    /// Called once by the shard root after creating the grain. Idempotent.
    /// </summary>
    Task SetTreeIdAsync(string treeId);

    /// <summary>Returns the tree ID this leaf is associated with, or <c>null</c> if not yet set.</summary>
    Task<string?> GetTreeIdAsync();

    /// <summary>
    /// Returns a <see cref="StateDelta"/> containing all entries whose timestamp is
    /// newer than what <paramref name="sinceVersion"/> has seen.
    /// Returns an empty delta if the caller is already up to date.
    /// </summary>
    Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion);

    /// <summary>
    /// Returns a <see cref="StateDelta"/> containing only the entries whose
    /// virtual slot is in <paramref name="sortedMovedSlots"/> and whose
    /// timestamp is newer than what <paramref name="sinceVersion"/> has seen.
    /// Used by the adaptive split coordinator to drain only moved-slot
    /// data from each leaf, eliminating the cost of serialising entries the
    /// coordinator would otherwise discard. The slot list must be sorted in
    /// ascending order; lookup uses binary search.
    /// </summary>
    Task<StateDelta> GetDeltaSinceForSlotsAsync(VersionVector sinceVersion, int[] sortedMovedSlots, int virtualShardCount);

    /// <summary>
    /// Bulk-merges entries (including tombstones) into this leaf using LWW semantics,
    /// preserving original timestamps. Used during splits to transfer entries without
    /// re-stamping them. Idempotent — re-merging the same entries is a no-op.
    /// </summary>
    Task MergeEntriesAsync(Dictionary<string, LwwValue<byte[]>> entries);

    /// <summary>
    /// Returns the sorted list of live (non-tombstoned) keys in this leaf
    /// that fall within the optional [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// If <paramref name="afterExclusive"/> is provided, only keys strictly greater than
    /// that value are returned. If <paramref name="beforeExclusive"/> is provided, only keys
    /// strictly less than that value are returned. These parameters support continuation-token
    /// pagination to avoid transferring keys that will be discarded by the caller.
    /// </summary>
    Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null);

    /// <summary>
    /// Returns the sorted list of live (non-tombstoned) key-value pairs in this leaf
    /// that fall within the optional [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// If <paramref name="afterExclusive"/> is provided, only entries with keys strictly
    /// greater than that value are returned. If <paramref name="beforeExclusive"/> is provided,
    /// only entries with keys strictly less than that value are returned. These parameters
    /// support continuation-token pagination to avoid transferring values that will be
    /// discarded by the caller.
    /// </summary>
    Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null);

    /// <summary>
    /// Removes tombstones whose wall-clock age exceeds <paramref name="gracePeriod"/>.
    /// Returns the number of tombstones removed. Tracks a <c>LastCompactionVersion</c>
    /// to skip redundant scans when no writes have occurred since the last compaction.
    /// </summary>
    Task<int> CompactTombstonesAsync(TimeSpan gracePeriod);

    /// <summary>
    /// Returns all live (non-tombstoned) key-value pairs in this leaf.
    /// Used by the tree resize operation to drain entries before purging.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetLiveEntriesAsync();

    /// <summary>
    /// Returns all live (non-tombstoned, non-expired) entries in this leaf as
    /// raw <see cref="LwwValue{T}"/> records, preserving both the source
    /// <see cref="Orleans.Lattice.Primitives.HybridLogicalClock"/> version and
    /// the absolute <c>ExpiresAtTicks</c> ( TTL). Used by snapshot /
    /// restore paths that must not lose TTL metadata when transferring entries
    /// between shards or trees. Not a read-path API: public read contracts
    /// continue to filter expired entries via <see cref="GetLiveEntriesAsync"/>.
    /// Returns a <see cref="List{T}"/> of <see cref="LwwEntry"/> rather than a
    /// <c>Dictionary&lt;string, LwwValue&lt;byte[]&gt;&gt;</c> to avoid Orleans
    /// type-alias encoding issues with nested-generic return shapes.
    /// </summary>
    Task<List<LwwEntry>> GetLiveRawEntriesAsync();

    /// <summary>
    /// Merges entries into this leaf using LWW semantics, preserving original
    /// timestamps. Unlike <see cref="MergeEntriesAsync"/>, this method checks
    /// for leaf overflow and triggers a split if needed.
    /// Returns a <see cref="SplitResult"/> if the leaf split, otherwise <c>null</c>.
    /// </summary>
    Task<SplitResult?> MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries);

    /// <summary>
    /// Clears all persistent state for this grain and deactivates it.
    /// Used during tree purge to permanently remove leaf data.
    /// </summary>
    Task ClearGrainStateAsync();
}
