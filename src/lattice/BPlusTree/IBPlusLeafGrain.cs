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
    /// stored - including expiry metadata and tombstone flag - or
    /// <c>null</c> if the key has never been written to this leaf. Does
    /// <b>not</b> filter expired entries: this method is used by replication
    /// and split-shadow paths that must forward the authoritative record to
    /// another shard without stripping its TTL. Not for general reads - use
    /// <see cref="GetAsync"/> or <see cref="GetWithVersionAsync"/> instead.
    /// <para>
    /// Returns an <see cref="LwwEntry"/> rather than
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> directly because
    /// the Orleans type-alias encoder has a codec-generation race when a
    /// grain-interface signature uses
    /// <c>Task&lt;LwwValue&lt;byte[]&gt;?&gt;</c> - it intermittently emits
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
    /// Persists the logical chain-shard index this leaf belongs to -
    /// the <c>shardIndex</c> half of the owning
    /// <c>ShardRootGrain</c>'s <c>{treeId}/{shardIndex}</c> grain key.
    /// Called once by the shard root after creating the grain (next to
    /// <see cref="SetTreeIdAsync"/>). Idempotent: subsequent calls are
    /// no-ops once the slot has been seeded. The persisted value is
    /// stamped onto every <see cref="LatticeMutation"/> the leaf
    /// commits to the WAL and consulted at activation time to filter
    /// out records authored by sibling chain shards sharing a WAL
    /// partition.
    /// </summary>
    Task SetShardIndexAsync(int shardIndex);

    /// <summary>
    /// Persists the [<paramref name="lowKeyInclusive"/>,
    /// <paramref name="highKeyExclusive"/>) ownership range for this
    /// leaf. Called exactly once at sibling-birth time by
    /// <c>CompleteSplitAsync</c> on the donor leaf - the donor stamps
    /// the split key as the sibling's low and the donor's pre-split
    /// high as the sibling's high. Idempotent: subsequent calls are
    /// no-ops once <see cref="State.LeafNodeState.LowKeyInclusive"/>
    /// has been seeded. The persisted bounds are consulted at
    /// activation time by the WAL materialiser to filter out records
    /// whose key falls outside this leaf's range (intra-shard
    /// sibling-leaf fanout regression). A <see langword="null"/>
    /// bound means "no constraint on that side" - used both for the
    /// chain's leftmost leaf (low = null) and the chain's rightmost
    /// leaf (high = null), and for legacy state shapes that
    /// pre-date this slot.
    /// </summary>
    Task SetKeyRangeAsync(string? lowKeyInclusive, string? highKeyExclusive);

    /// <summary>
    /// Stamps an initial projection-checkpoint offset on a freshly
    /// created leaf so its first activation can skip replaying WAL
    /// entries that were already materialised into its
    /// <see cref="State.LeafNodeState.Entries"/> at birth. Called by
    /// <c>CompleteSplitAsync</c> on the donor leaf with the shard's
    /// WAL head offset captured at split time, after the donor has
    /// populated the sibling's entries via
    /// <see cref="MergeEntriesAsync"/>. Routes through
    /// <c>ILeafProjection.SetCheckpointOffsetAsync</c> so the
    /// existing unresolved-prepare clamp is honoured; for a sibling
    /// at birth there are no unresolved prepares so the clamp is a
    /// no-op. Idempotent: a re-call with a smaller offset is a
    /// no-op (the underlying seam enforces monotonic non-decrease).
    /// </summary>
    Task SetCheckpointOffsetHintAsync(long offset);

    /// <summary>
    /// Returns a <see cref="StateDelta"/> containing all entries whose timestamp is
    /// newer than what <paramref name="sinceVersion"/> has seen.
    /// Returns an empty delta if the caller is already up to date.
    /// </summary>
    Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion);

    /// <summary>
    /// Returns the set of keys that currently have a pending-tx mutation
    /// on this leaf (the saga has prepared a write but the registry has
    /// not yet recorded a terminal decision, OR the terminal has been
    /// recorded but the leaf has not yet drained the pending bucket).
    /// Returns an empty list when the leaf has no pending-tx activity.
    /// <para>
    /// Used by <see cref="ILeafCacheGrain"/> to identify keys whose
    /// cached <see cref="LwwValue{T}"/> may differ from the strict
    /// atomic-visibility outcome - those reads bypass the cache and
    /// dial back to the primary leaf, whose read paths consult the
    /// per-tree <see cref="ITxRegistryGrain"/> for the recorded saga
    /// outcome. Lightweight: returns the in-memory pending-key set
    /// without touching persistent state or doing any merge work.
    /// </para>
    /// </summary>
    Task<List<string>> GetPendingKeysAsync();

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
    /// re-stamping them. Idempotent - re-merging the same entries is a no-op.
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

    /// <summary>
    /// Returns a deterministic XxHash128 <see cref="LeafProjectionDigest"/>
    /// of this leaf's materialised projection. Two leaves that have
    /// applied the same prefix of the same per-shard WAL produce
    /// byte-identical digests; used by chaos tests and operator tooling
    /// to detect cross-silo divergence the moment the WAL becomes the
    /// rebuild source of truth.
    /// </summary>
    Task<LeafProjectionDigest> GetProjectionDigestAsync();

    /// <summary>
    /// Applies the saga terminal mark for <paramref name="transactionId"/>
    /// against this leaf's pending-transaction map. When
    /// <paramref name="committed"/> is <c>true</c> every prepared mutation
    /// recorded under the transaction is flipped into the visible
    /// projection via LWW merge; when <c>false</c> every prepared mutation
    /// is dropped. Idempotent - replaying the same terminal mark for a
    /// leaf that holds no pending bucket under <paramref name="transactionId"/>
    /// is a no-op (the dedup guarantee survives until activation
    /// recycling, after which the WAL replay reseeds it).
    /// <para>
    /// Called by the saga coordinator's terminal-broadcast loop via
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/>; not intended
    /// for direct user invocation.
    /// </para>
    /// <para>
    /// <b>Cross-migration LWW backstop.</b> When the optional
    /// <paramref name="committedValues"/> dictionary is supplied (only on the
    /// commit path), it carries the saga's committed (key, value) pairs that
    /// route to this leaf <em>at terminal-broadcast time</em>. The leaf
    /// applies the values as a LWW-safe backstop only when it holds no
    /// pending bucket for <paramref name="transactionId"/> - i.e. when the
    /// prepare-phase shadow-forward into this leaf was dropped by a mid-saga
    /// shard-split / drain race. In every other case the existing
    /// pending-flip path is the authoritative source and the backstop is a
    /// no-op. The backstop stamp is <see cref="Primitives.HybridLogicalClock.Tick"/>
    /// of the leaf's current clock, guaranteeing strict-greater HLC ordering
    /// against any stale pre-saga value already in
    /// <see cref="State.LeafNodeState.Entries"/>. The backstop persists via
    /// <see cref="IPersistentState{T}.WriteStateAsync"/> so a subsequent
    /// reactivation observes the post-saga projection even though the
    /// WAL on this leaf contains no prepare for this saga.
    /// </para>
    /// </summary>
    /// <param name="transactionId">The saga's transaction id stamped on every prepared mutation. Must not be <see cref="Guid.Empty"/>; the call is a no-op for that value.</param>
    /// <param name="committed"><c>true</c> to flip pending mutations into the visible projection; <c>false</c> to drop them.</param>
    /// <param name="committedValues">
    /// Optional cross-migration LWW backstop. When non-null and
    /// <paramref name="committed"/> is <c>true</c>, the leaf applies each
    /// <c>(key, value)</c> as a LWW-safe write only when it holds no
    /// pending bucket under <paramref name="transactionId"/>. The dictionary
    /// must be restricted to keys this leaf owns (the shard root performs
    /// the per-key-to-leaf grouping). Passing <c>null</c> is the
    /// pre-backstop call shape and remains supported for wire compatibility.
    /// </param>
    Task ApplyTxTerminalAsync(
        Guid transactionId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues = null);

    /// <summary>
    /// Returns the leaf's current
    /// <see cref="Primitives.HybridLogicalClock"/>. Used by
    /// <see cref="IShardRootGrain.AppendTxTerminalAsync"/> to compute a
    /// terminal-mark HLC strictly greater than every prepare's stamp in
    /// the saga, so cross-cluster receivers - which merge inbound
    /// records by HLC across WAL partitions - observe the saga's
    /// prepared writes <em>before</em> the terminal mark and never
    /// flush an empty pending bucket on a too-early terminal arrival.
    /// <para>
    /// Read-only accessor: does not advance the leaf's clock and does
    /// not touch persistent state. Cheap on the steady-state hot path
    /// (a single in-memory field read).
    /// </para>
    /// </summary>
    Task<HybridLogicalClock> GetClockAsync();
}
