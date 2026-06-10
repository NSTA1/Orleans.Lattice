namespace Orleans.Lattice.BPlusTree;

using Orleans.Concurrency;
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
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> per U9p step 8c-c-iv-c2-iii
    /// so multiple producer turns can run concurrently on the same
    /// activation. Orleans serialises synchronous code between awaits,
    /// so the per-key LWW merge, HLC tick, and projection-hash updates
    /// are race-free; the split state machine is serialised separately
    /// by the per-activation <c>_splitGate</c> documented on
    /// <see cref="Grains.BPlusLeafGrain"/>.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<SplitResult?> SetAsync(string key, byte[] value);

    /// <summary>
    /// Inserts or updates a key-value pair with an absolute expiry
    ///. The entry's <see cref="Orleans.Lattice.Primitives.LwwValue{T}.ExpiresAtTicks"/>
    /// is set to <paramref name="expiresAtTicks"/>; once the current UTC wall
    /// clock passes that value the entry is treated as tombstoned on reads
    /// and reaped by background compaction after the configured grace
    /// period. Pass <c>0</c> to write a non-expiring entry (equivalent to
    /// <see cref="SetAsync(string, byte[])"/>).
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> for the same reason
    /// as <see cref="SetAsync(string, byte[])"/> - see that overload's
    /// summary for the interleave-safety argument.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
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
    /// Batched variant of <see cref="GetRawEntryAsync(string)"/>. Returns
    /// the raw persisted entry for every key in <paramref name="keys"/>,
    /// with the result list aligned by index with the input list
    /// (so <c>result[i]</c> corresponds to <c>keys[i]</c>). Per-element
    /// semantics match the single-key variant: <c>null</c> if the key
    /// has never been written to this leaf, otherwise the raw record
    /// including expiry metadata and tombstone flag. Does <b>not</b>
    /// filter expired entries.
    /// </summary>
    Task<List<LwwEntry?>> GetRawEntriesAsync(List<string> keys);

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
    /// Applies a producer-side typed CRDT delta to <paramref name="key"/>
    /// under the declared <paramref name="mode"/>. The leaf resolves the
    /// matching <see cref="CrdtShape"/> from the registered
    /// <see cref="CrdtShapeRegistry"/>, deserialises <paramref name="deltaBytes"/>
    /// into the typed delta DTO, decodes the current state from
    /// <see cref="State.LeafNodeState.Entries"/> (or constructs an empty
    /// instance when the key is absent), folds the delta into the state
    /// via the shape's <c>MergeDelta</c>, re-serialises the post-merge
    /// state for the legacy byte[] row, appends a single
    /// <see cref="MutationKind.Set"/> WAL record whose
    /// <see cref="WalRecord.Delta"/> slot carries the producer's typed
    /// delta bytes verbatim, and returns the
    /// <see cref="HybridLogicalClock"/> stamped on the committed entry.
    /// <para>
    /// No CAS - CRDT delta merges are convergent under any interleaving,
    /// so a single-shot apply suffices. The returned HLC equals the
    /// committed entry's <see cref="LwwValue{T}.Timestamp"/> and is the
    /// per-key version observable through the existing read path.
    /// </para>
    /// <para>
    /// <see cref="LatticeMergeMode.LwwRegister"/> is rejected with
    /// <see cref="ArgumentException"/> - LWW writes flow through
    /// <see cref="SetAsync(string, byte[])"/> / <see cref="SetIfVersionAsync"/>.
    /// </para>
    /// </summary>
    Task<CrdtApplyResult> ApplyCrdtDeltaAsync(string key, LatticeMergeMode mode, byte[] deltaBytes);

    /// <summary>
    /// Inserts or updates multiple key-value pairs.
    /// Returns the last <see cref="SplitResult"/> if any split occurred, otherwise <c>null</c>.
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> per U9p step 8c-c-iv-c2-iii.
    /// See <see cref="SetAsync(string, byte[])"/> for the interleave-safety
    /// argument that applies to every mutation-surface method on this
    /// interface (the per-activation <c>_splitGate</c> serialises the
    /// split state machine).
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<SplitResult?> SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Conditional bulk write: commits only the entries whose <b>current</b>
    /// stored value satisfies <paramref name="predicate"/> (the guard),
    /// evaluated once here at write time against each key's committed value.
    /// A key with no live committed value is treated as non-matching and is
    /// skipped. Returns a <see cref="ConditionalSetManyResult"/> carrying the
    /// committed key subset and any resulting <see cref="SplitResult"/>.
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> for the same
    /// interleave-safety reason as <see cref="SetManyAsync"/>.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<ConditionalSetManyResult> SetManyWherePredicateAsync(List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> per U9p step 8c-c-iv-c2-iii.
    /// See <see cref="SetAsync(string, byte[])"/> for the interleave-safety
    /// argument.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Tombstones all live keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// Returns a <see cref="RangeDeleteResult"/> containing the number of tombstoned keys
    /// and a <c>PastRange</c> flag indicating whether this leaf has observed any key
    /// <c>&gt;= endExclusive</c>. The shard-root coordinator uses <c>PastRange</c> to
    /// stop walking the leaf chain deterministically on sparse multi-shard trees, where
    /// a leaf may legitimately delete zero keys even when later leaves contain
    /// range-matching entries.
    /// <para>
    /// When <paramref name="predicate"/> is non-<see langword="null"/> the leaf
    /// tombstones only the in-range live keys whose value satisfies the
    /// predicate (evaluated once, here, at write time) and records the matched
    /// key set on <see cref="RangeDeleteResult.MatchedKeys"/> and in the WAL
    /// record, so replay and replication reproduce exactly that set without
    /// re-evaluating the predicate.
    /// </para>
    /// </summary>
    Task<RangeDeleteResult> DeleteRangeAsync(string startInclusive, string endExclusive, LatticePredicateNode? predicate = null);

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
    /// Stores a grain reference to the parent internal node so this leaf
    /// can propagate its <see cref="ChildDigestSnapshot"/> upward when
    /// its projection digest changes. Called once by the shard root
    /// after creating the grain (next to <see cref="SetTreeIdAsync"/>);
    /// also re-called when a split rotates the leaf beneath a new
    /// parent. A <see langword="null"/> parent marks this leaf as the
    /// shard root itself (the flat-tree case where the root is a leaf),
    /// so digest propagation stops at the leaf and the shard reads the
    /// leaf's digest directly. Idempotent: a re-call with the same id
    /// is a no-op; a re-call with a different id overwrites the slot
    /// and triggers a fresh full-fold republish so the new parent
    /// converges with the leaf's current digest.
    /// </summary>
    Task SetParentAsync(GrainId? parentId);

    /// <summary>
    /// Returns this leaf's current contribution to its parent internal
    /// node's subtree fold: the raw 16-byte
    /// <c>ProjectionHash</c>, the total entry count (live plus
    /// tombstoned), and the persisted projection-checkpoint offset.
    /// Distinct from <see cref="GetProjectionDigestAsync"/>, which folds
    /// these three fields into a single XxHash128 fingerprint for
    /// public consumption. Used by the parent's lazy-backfill path when
    /// no prior snapshot has been recorded for this leaf (e.g. a
    /// freshly-activated internal node observing legacy state, or a
    /// crash-recovery rebuild).
    /// </summary>
    Task<ChildDigestSnapshot> GetChildDigestSnapshotAsync();

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
    /// Seeds every birth-time metadata slot on a freshly created split
    /// sibling in a single round-trip: tree id, shard index, ownership
    /// key range, and the next/prev sibling pointers. Replaces the five
    /// separate gated setter RPCs (<see cref="SetTreeIdAsync"/>,
    /// <see cref="SetShardIndexAsync"/>, <see cref="SetKeyRangeAsync"/>,
    /// <see cref="SetNextSiblingAsync"/>, <see cref="SetPrevSiblingAsync"/>)
    /// the donor used to issue serially, each behind its own gate acquire
    /// and its own <c>WriteStateAsync</c>. Carries the same idempotent
    /// semantics as the individual setters - the write-once slots
    /// (tree id, shard index, key-range low bound) are skipped when
    /// already seeded - but acquires the per-activation split gate once
    /// and persists once for the whole batch, collapsing the split
    /// fast-path's sibling-seeding cost from five cross-grain
    /// persist round-trips to one.
    /// </summary>
    Task InitializeSiblingAsync(SiblingInitialization init);

    /// <summary>
    /// Applies a batch of per-partition projection-checkpoint hints in a
    /// single round-trip. <paramref name="offsetsByPartition"/> index
    /// <c>p</c> is the WAL head offset to hint for partition <c>p</c>;
    /// a non-positive entry is skipped. Replaces the per-partition
    /// <see cref="SetCheckpointOffsetHintAsync"/> fan-out the split
    /// donor used to issue once per WAL partition, each a separate
    /// cross-grain round-trip. Each partition's hint is still applied
    /// under that partition's <c>LatticeApplyOffsetContext</c> scope so
    /// the sibling's clamp targets the correct offset space.
    /// </summary>
    Task SetCheckpointOffsetHintsAsync(long[] offsetsByPartition);

    /// <summary>
    /// Records that ownership of the given <paramref name="sortedMovedSlots"/>
    /// has migrated away from this leaf's owning shard. Subsequent read
    /// entrypoints on this leaf (<see cref="GetAsync"/>,
    /// <see cref="GetWithVersionAsync"/>, <see cref="ExistsAsync"/>,
    /// <see cref="GetManyAsync"/>) return null/false for any key whose
    /// <c>ShardMap.GetVirtualSlot(key, virtualShardCount)</c> falls in
    /// the moved-slot set, sealing the persistent-orphan read path that
    /// the cache-coherence prune pass cannot reach via the
    /// <see cref="ILeafCacheGrain"/> pending-key delegation hole.
    /// <para>
    /// The implementation does NOT touch <see cref="State.LeafNodeState.Entries"/>
    /// - storage stays inconsistent on the source for moved slots so
    /// the k-way merge ordering invariant in <c>LatticeGrain.KeysAsync</c>
    /// remains intact. Physically removing the entries would re-shape
    /// the leaf chain mid-scan and break the ordering guarantee that
    /// the merge relies on; sealing the read path at the leaf preserves
    /// the invariant and is provably unobservable through any reader
    /// because the cache prunes on the next
    /// <see cref="GetDeltaSinceAsync"/>.
    /// </para>
    /// <para>
    /// Idempotent on identical input. Persists the moved-slot set,
    /// bumps the version vector + revision cookie, and persists once.
    /// </para>
    /// </summary>
    /// <param name="sortedMovedSlots">Sorted, distinct virtual-slot indices that have moved away.</param>
    /// <param name="virtualShardCount">The virtual shard count in force at the moment of the move.</param>
    Task MarkSlotsMovedAwayAsync(int[] sortedMovedSlots, int virtualShardCount);

    /// <summary>
    /// Returns a <see cref="StateDelta"/> containing all entries whose timestamp is
    /// newer than what <paramref name="sinceVersion"/> has seen.
    /// Returns an empty delta if the caller is already up to date.
    /// </summary>
    Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion);

    /// <summary>
    /// Returns a <see cref="StateDelta"/> containing every entry the
    /// caller has not yet observed, addressed by an activation-scoped
    /// <see cref="LeafDeliveryCursor"/> rather than the LWW
    /// <see cref="VersionVector"/>. The cursor decouples delivery from
    /// LWW HLC ordering so cross-cluster applies whose source HLC is
    /// below the destination leaf's published clock are still
    /// delivered correctly.
    /// <para>
    /// Behavior:
    /// <list type="bullet">
    ///   <item>
    ///     If <paramref name="sinceCursor"/>'s
    ///     <see cref="LeafDeliveryCursor.Epoch"/> does not match the
    ///     leaf activation's epoch (i.e. <see cref="LeafDeliveryCursor.Empty"/>
    ///     from a fresh cache, or a stale cursor from a previous
    ///     leaf activation), the leaf returns a full snapshot of every
    ///     live entry and stamps <see cref="StateDelta.DeliveryCursor"/>
    ///     with the leaf's current cursor for the caller to adopt.
    ///   </item>
    ///   <item>
    ///     Otherwise the leaf scans its per-key sequence map and
    ///     returns only entries whose sequence is strictly greater
    ///     than <paramref name="sinceCursor"/>'s
    ///     <see cref="LeafDeliveryCursor.Sequence"/>, with
    ///     <see cref="StateDelta.DeliveryCursor"/> set to the leaf's
    ///     current cursor.
    ///   </item>
    /// </list>
    /// </para>
    /// </summary>
    Task<StateDelta> GetDeltaSinceCursorAsync(LeafDeliveryCursor sinceCursor);

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
    /// Returns a snapshot of every prepared mutation currently buffered
    /// in this leaf's pending-tx map whose key hashes into one of the
    /// virtual slots in <paramref name="sortedMovedSlots"/>. Used by the
    /// retroactive shadow-forward sweep at the entry of a shard
    /// split's <c>BeginShadowWrite</c> phase so prepares that landed on
    /// the source shard <em>before</em> the split's shadow-forward
    /// window opened are replayed into the destination shard's pending
    /// bucket.
    /// <para>
    /// <paramref name="sortedMovedSlots"/> must be ascending; lookup
    /// uses <see cref="Array.BinarySearch(int[], int)"/>. The
    /// <paramref name="virtualShardCount"/> is the
    /// <see cref="State.ShadowForwardState.VirtualShardCount"/>-equivalent
    /// constant from the active <see cref="ShardMap"/> - the same value
    /// every saga-coordinator + shard-split slot calculation already
    /// uses. Returns an empty list when no pending bucket exists, no
    /// pending key falls into a migrating slot, or the moved-slot
    /// array is empty (steady-state hot path).
    /// </para>
    /// <para>
    /// The returned <see cref="PendingMutationSnapshot"/> carries the
    /// authoring <c>(Timestamp, OriginClusterId, VectorClock)</c> tuple
    /// verbatim so the retroactive shadow-forward sweep can re-stamp each replay through
    /// <see cref="LatticeHlcOverrideContext"/> +
    /// <see cref="LatticeOriginContext"/> +
    /// <see cref="LatticeVectorClockContext"/> scopes; the destination
    /// leaf's pending-tx bucket then carries the same identity as the
    /// source leaf's, and the saga's terminal mark drains both via the
    /// same LWW path. Lightweight: returns the in-memory snapshot
    /// without touching persistent state.
    /// </para>
    /// </summary>
    Task<List<PendingMutationSnapshot>> GetPendingMutationsForSlotsAsync(int[] sortedMovedSlots, int virtualShardCount);

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
    Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, Orleans.Lattice.LatticePredicateNode? predicate = null);

    /// <summary>
    /// Returns the sorted list of live (non-tombstoned) key-value pairs in this leaf
    /// that fall within the optional [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// If <paramref name="afterExclusive"/> is provided, only entries with keys strictly
    /// greater than that value are returned. If <paramref name="beforeExclusive"/> is provided,
    /// only entries with keys strictly less than that value are returned. These parameters
    /// support continuation-token pagination to avoid transferring values that will be
    /// discarded by the caller.
    /// </summary>
    Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, Orleans.Lattice.LatticePredicateNode? predicate = null);

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
    /// <para>
    /// When <paramref name="isCrossShardMigration"/> is <c>true</c>, the
    /// merge runs the asymmetric migration-vs-foreground rule: an incoming
    /// entry is dropped if the destination already holds a non-migration
    /// entry for the same key, and otherwise the stored entry is stamped
    /// with <c>IsMigrated = true</c>. The flag is intended for the
    /// cross-shard migration callsites only (the source-shard drain in
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeShardSplitGrain"/>
    /// and the per-write shadow-forward in
    /// <c>ShardRootGrain.Split.cs</c>). All other callers (cross-cluster
    /// replication, tree-merge, snapshot restore, intra-shard
    /// sibling-merge) MUST pass <c>false</c> so the merge runs the
    /// symmetric LWW-by-HLC contract and the incoming entry's own
    /// <c>IsMigrated</c> flag is preserved verbatim.
    /// </para>
    /// </summary>
    Task<SplitResult?> MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration = false);

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
    /// Returns the persisted projection-checkpoint offset for this leaf -
    /// the highest WAL offset whose mutation has been durably applied to
    /// the in-memory projection via
    /// <see cref="State.LeafNodeState.ProjectionCheckpointOffset"/>.
    /// Read-only diagnostic accessor used by the operator-facing
    /// materialiser-lag surface (<c>ILattice.GetMaterialiserLagAsync</c>)
    /// to compute the shard-wide <c>WAL_head - min(leaf.checkpoint)</c>
    /// back-pressure metric without forcing a checkpoint flush.
    /// </summary>
    Task<long> GetProjectionCheckpointOffsetAsync();

    /// <summary>
    /// Operator-facing projection rebuild seam. Resets this leaf's
    /// materialised projection (<see cref="State.LeafNodeState.Entries"/>,
    /// the incremental <see cref="State.LeafNodeState.ProjectionHash"/>,
    /// the persisted <see cref="State.LeafNodeState.ProjectionCheckpointOffset"/>,
    /// and the per-leaf saga pending-tx map) to its post-activation
    /// zero state, persists the cleared projection slots in a single
    /// <see cref="IPersistentState{T}.WriteStateAsync"/> call, and
    /// deactivates the grain so the next activation replays the
    /// per-shard WAL from offset <c>0</c> through the existing
    /// activation-time materialiser. Topology-bearing slots
    /// (<see cref="State.LeafNodeState.TreeId"/>,
    /// <see cref="State.LeafNodeState.ShardIndex"/>, sibling pointers,
    /// key-range bounds, split markers, parent pointer) are preserved
    /// verbatim so the rebuild observes the same WAL-filter context the
    /// pre-rebuild leaf used. Used after a corrupt-projection incident
    /// or a <see cref="LatticeOptions.MaxLeafReplayEntries"/> blow-out
    /// to recover the leaf state from the durable WAL source of truth.
    /// <para>
    /// Asynchronous failure mode: a transient storage failure on the
    /// persist surfaces back to the caller; the leaf state is left in
    /// the pre-rebuild shape on a persist failure, so the operation is
    /// safe to retry. A subsequent successful call completes the rebuild
    /// from a clean state.
    /// </para>
    /// </summary>
    Task RebuildProjectionFromWalAsync();

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

    /// <summary>
    /// Installs a per-saga shadow marker on this destination leaf
    /// claiming that the source-side saga <paramref name="transactionId"/>
    /// affected each key in <paramref name="keys"/>. The marker is
    /// consulted by the read path whenever an
    /// <see cref="LwwValue{T}.IsMigrated"/>=<c>true</c> entry is about
    /// to be surfaced for one of those keys: an in-flight or aborted
    /// saga lets the migrated pre-saga value pass through (strict
    /// isolation), while a saga that has committed at the registry
    /// but whose backstop terminal has not yet reached this leaf
    /// triggers a <see cref="StaleShardRoutingException"/> so the
    /// <c>LatticeGrain</c> deadline-bounded retry loop re-fans once
    /// the backstop arrives.
    /// <para>
    /// Installed by the split coordinator
    /// (<see cref="ITreeShardSplitGrain"/>) during the drain and
    /// retroactive-sweep phases of an online shard split, naming
    /// every in-flight saga whose prepared mutations touched keys
    /// migrating into the destination shard. The marker is cleared
    /// automatically by <see cref="ApplyTxTerminalAsync"/> when the
    /// saga's terminal reaches this leaf - so the marker has at most
    /// saga-lifetime memory footprint and degenerates to a no-op
    /// outside of an active split.
    /// </para>
    /// <para>
    /// Idempotent on identical input; an empty <paramref name="keys"/>
    /// list is a no-op (the saga touched only non-moved slots on
    /// source). <paramref name="transactionId"/> must be non-empty -
    /// the matching terminal mark needs a non-default key to clear
    /// the marker.
    /// </para>
    /// </summary>
    /// <param name="transactionId">
    /// The source-side saga identifier whose prepared mutations
    /// affected the listed keys. Must not be <see cref="Guid.Empty"/>.
    /// </param>
    /// <param name="keys">
    /// The keys this saga affected on source whose virtual slots are
    /// migrating to the destination shard owning this leaf. Must not
    /// be <c>null</c>.
    /// </param>
    Task MarkSagaShadowAsync(Guid transactionId, IReadOnlyList<string> keys);

    /// <summary>
    /// Captures a point-in-time snapshot of this leaf's per-activation
    /// entry cache and persists it into the dedicated
    /// <see cref="Grains.ILeafSnapshotStorageGrain"/> keyed by this
    /// leaf's grain id. The captured snapshot carries the leaf's
    /// already-persisted <c>ProjectionCheckpointOffset</c> as its
    /// <c>SnapshotOffset</c>; subsequent activations that find a
    /// snapshot whose offset exceeds the persisted checkpoint may
    /// rehydrate the cache from the snapshot instead of replaying
    /// the WAL from the checkpoint forward.
    /// <para>
    /// Driven by the leaf itself: the activation hook captures once
    /// after tail replay when the fall-off-log detector raises the
    /// <see cref="Grains.FallOffLogDecision.SnapshotPending"/> advisory,
    /// and every <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// successful checkpoint persist re-classifies and (on advisory)
    /// re-captures. The call is a no-op when the leaf has not yet
    /// applied any WAL entry (the persisted checkpoint is the
    /// "nothing applied" sentinel <c>-1</c>) or when the leaf has no
    /// resolved tree id (uninitialised activation). The snapshot grain
    /// holds at most one blob per leaf; a successful capture overwrites
    /// any previous snapshot.
    /// </para>
    /// </summary>
    Task CaptureSnapshotAsync();

    /// <summary>
    /// Test-only seam: requests that the grain runtime collect this
    /// activation by calling <c>DeactivateOnIdle</c> from inside the
    /// grain. Integration tests use this to exercise activation-time
    /// snapshot rehydration end-to-end: capture a snapshot, force the
    /// activation to deactivate, then re-acquire the grain and observe
    /// that the rehydrate path restored the cache without replaying
    /// the full WAL. Production callers must not invoke this; the
    /// silo's idle-collection scheduler is the canonical driver of
    /// activation lifetime.
    /// </summary>
    Task ForceDeactivateAsync();
}
