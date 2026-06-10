using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for a leaf grain. The per-key projection was collapsed out
/// of this row: per-key data now lives in a per-activation in-memory cache
/// (<c>LeafEntryCache</c>) rehydrated from the WAL on every activation, gated by
/// <see cref="ProjectionCheckpointOffset"/>. This persisted row carries only
/// topology (sibling/parent pointers, key range, shard index, split lifecycle),
/// the projection-digest fold (<see cref="ProjectionHash"/>), the checkpoint
/// offsets, and the HLC clock plus version vectors. See the reserved
/// <c>[Id(0)]</c> slot note below.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafNodeState)]
internal sealed class LeafNodeState
{
    // [Id(0)] previously held a SortedDictionary<string, LwwValue<byte[]>>
    // Entries per-key projection. The persisted leaf row was collapsed:
    // per-key data now lives in a per-activation in-memory cache
    // (LeafEntryCache) rehydrated from the WAL on every activation, and
    // ProjectionCheckpointOffset gates the replay scope. The Id(0) slot is
    // reserved - never reuse it, because doing so would silently shadow
    // pre-step-6 persisted state during a rolling upgrade.

    /// <summary>Grain identity of the right sibling leaf (for range scans), or <c>null</c>.</summary>
    [Id(1)] public GrainId? NextSibling { get; set; }

    /// <summary>Monotonic split lifecycle state.</summary>
    [Id(2)] public SplitState SplitState { get; set; }

    /// <summary>If split has occurred, the key at which this node was split.</summary>
    [Id(3)] public string? SplitKey { get; set; }

    /// <summary>If split has occurred, the grain identity of the new right sibling created by the split.</summary>
    [Id(4)] public GrainId? SplitSiblingId { get; set; }

    /// <summary>The current logical clock for this grain.</summary>
    [Id(5)] public HybridLogicalClock Clock { get; set; }

    /// <summary>
    /// Version vector tracking causal history. Each write ticks the local
    /// replica entry, enabling delta extraction for replication.
    /// </summary>
    [Id(6)] public VersionVector Version { get; set; } = new();

    /// <summary>The tree this leaf belongs to. Used to resolve named <see cref="BPlusTree.LatticeOptions"/>.</summary>
    [Id(7)] public string? TreeId { get; set; }

    /// <summary>Grain identity of the left sibling leaf (for reverse scans), or <c>null</c>.</summary>
    [Id(8)] public GrainId? PrevSibling { get; set; }

    /// <summary>
    /// The version vector as of the last tombstone compaction pass. Entries
    /// that have not changed since this version are skipped during the next
    /// compaction scan, avoiding redundant work.
    /// </summary>
    [Id(10)] public VersionVector LastCompactionVersion { get; set; } = new();

    /// <summary>
    /// During a split, the previous value of <see cref="NextSibling"/> before it was
    /// overwritten with the new sibling. Persisted in Phase 1 so that
    /// <see cref="BPlusTree.Grains.BPlusLeafGrain.CompleteSplitAsync"/> can link the
    /// new sibling into the doubly-linked list even after a crash-recovery.
    /// </summary>
    [Id(9)] public GrainId? OldNextSibling { get; set; }

    /// <summary>
    /// Highest write-ahead-log offset whose mutation has been durably
    /// applied to this leaf's projection via the
    /// <c>ILeafProjection.Apply</c> seam. Persisted alongside the
    /// projection so a re-activation can resume replay from
    /// <c>ProjectionCheckpointOffset + 1</c> rather than scanning the
    /// full leaf state. Defaults to <c>0</c> on freshly persisted
    /// state to preserve the published empty-tree digest shape
    /// (<c>digest.CheckpointOffset == 0</c> for an empty leaf).
    /// <para>
    /// The "nothing applied" sentinel is <c>-1</c>, matching
    /// <see cref="IWalStorageProvider.GetHighestOffsetAsync"/>'s
    /// empty-WAL convention. The per-key entry cache is per-activation
    /// only, so a freshly activated leaf
    /// whose persisted checkpoint says "0" but whose cache is empty
    /// would silently skip offset 0 in the WAL on restart. The
    /// activation path
    /// (<see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.<c>OnActivateAsync</c>)
    /// therefore resets the persisted checkpoint to <c>-1</c>
    /// whenever the cache starts empty and no snapshot rehydrate
    /// occurred, ensuring the replay below covers the full readable
    /// window. The operator-driven projection rebuild path
    /// (<c>RebuildLeafProjectionAsync</c>) likewise sets this slot
    /// to <c>-1</c> after clearing the entry cache.
    /// </para>
    /// </summary>
    [Id(11)] public long ProjectionCheckpointOffset { get; set; }

    /// <summary>
    /// Incremental XOR-fold projection fingerprint: a 16-byte buffer that holds
    /// the running XxHash128 XOR over per-key contributions of every entry in
    /// the leaf's entry cache. Each entry contributes a deterministic 16-byte
    /// XxHash128 hash (over key, HLC, tombstone flag, expiry, origin, vector
    /// clock, and value); insertions XOR the contribution in, deletions XOR it
    /// out, and updates XOR out the old contribution and XOR in the new. The
    /// XOR fold is commutative and self-inverse, so the field is mathematically
    /// identical to a fresh walk over the entry cache at every commit
    /// boundary, regardless of the order writes arrived in.
    /// <para>
    /// The public <c>GetProjectionDigestAsync</c> surface chains this field
    /// with the entry count and <see cref="ProjectionCheckpointOffset"/> into a
    /// final XxHash128 block, preserving the published hash shape.
    /// </para>
    /// <para>
    /// <c>null</c> on grain state persisted before this slot was introduced
    /// (or carrying a buffer whose width does not match the current algorithm);
    /// the leaf lazily backfills it (one full walk) on the first mutation or
    /// the first digest read after activation, then maintains it incrementally
    /// thereafter.
    /// </para>
    /// </summary>
    [Id(12)] public byte[]? ProjectionHash { get; set; }

    /// <summary>
    /// Logical chain-shard index this leaf belongs to - i.e. the
    /// <c>shardIndex</c> half of the owning
    /// <c>ShardRootGrain</c>'s <c>{treeId}/{shardIndex}</c> grain key.
    /// Persisted exactly once by <c>SetShardIndexAsync</c>, called by
    /// the shard-root coordinator alongside <see cref="TreeId"/> at
    /// every leaf-create site. Consulted by the activation-time WAL
    /// materialiser to filter out records authored by sibling chain
    /// shards that share a WAL partition - without this slot a leaf
    /// reading a shared WAL partition would absorb every other shard's
    /// keys into its own projection on every reactivation. The slot is
    /// nullable for back-compat with the V1 single-shard layout: a leaf
    /// whose state pre-dates this field decodes as <c>null</c>, and the
    /// materialiser treats that as "apply unconditionally" so the
    /// upgrade-time replay path is unchanged for the V1 single-shard
    /// case (every chain shard is shard 0).
    /// </summary>
    [Id(13)] public int? ShardIndex { get; set; }

    /// <summary>
    /// Inclusive lower bound of this leaf's owned key range, or
    /// <see langword="null"/> when the leaf has no persisted lower
    /// bound (the chain's leftmost leaf, or any leaf whose state
    /// pre-dates this slot). Persisted exactly once at sibling-birth
    /// time by <c>SetKeyRangeAsync</c>, called from
    /// <c>CompleteSplitAsync</c> on the donor leaf. Donor leaves never
    /// call <c>SetKeyRangeAsync</c> for their own slot - they update
    /// <see cref="HighKeyExclusive"/> directly when a split narrows
    /// their range. Consulted by the activation-time WAL materialiser
    /// to filter out records whose key falls outside this leaf's
    /// current ownership range - without this slot a leaf reading the
    /// shared shard WAL partition would absorb every sibling chain
    /// leaf's keys into its own projection on every reactivation. The
    /// slot is nullable for back-compat with the V1 single-leaf
    /// layout: a leaf whose state pre-dates this field decodes as
    /// <see langword="null"/>, and the materialiser treats both
    /// bounds as "no constraint" so the upgrade-time replay path is
    /// unchanged.
    /// </summary>
    [Id(14)] public string? LowKeyInclusive { get; set; }

    /// <summary>
    /// Exclusive upper bound of this leaf's owned key range, or
    /// <see langword="null"/> when the leaf has no persisted upper
    /// bound (the chain's rightmost leaf, or any leaf whose state
    /// pre-dates this slot). Donors narrow their own
    /// <see cref="HighKeyExclusive"/> directly to the split key at
    /// <c>CompleteSplitAsync</c> time; siblings inherit their high
    /// from the donor's pre-split high via <c>SetKeyRangeAsync</c>.
    /// See <see cref="LowKeyInclusive"/> for the legacy-compat /
    /// rebuild semantics shared by both bounds.
    /// </summary>
    [Id(15)] public string? HighKeyExclusive { get; set; }

    /// <summary>
    /// Sorted, distinct set of virtual slot indices whose ownership has
    /// migrated away from this leaf's owning shard. Populated by
    /// <see cref="BPlusTree.Grains.BPlusLeafGrain.MarkSlotsMovedAwayAsync"/>
    /// at the Swap phase of an adaptive shard split. Once a slot is
    /// recorded here, the leaf's read entrypoints (<c>GetAsync</c>,
    /// <c>GetWithVersionAsync</c>, <c>ExistsAsync</c>, <c>GetManyAsync</c>)
    /// return null/false for any key hashing into that slot, sealing the
    /// persistent-orphan read path that the cache-coherence prune pass
    /// cannot reach via the <see cref="BPlusTree.Grains.ILeafCacheGrain"/>
    /// pending-key delegation hole. The list is sticky once written
    /// (slots never un-move).
    /// <para>
    /// <see langword="null"/> in the steady state (no slot has ever moved
    /// away from this leaf), so non-resharded leaves pay zero per-leaf
    /// allocation cost. Lazily allocated the first time
    /// <see cref="BPlusTree.Grains.BPlusLeafGrain.MarkSlotsMovedAwayAsync"/>
    /// fires and the underlying split shape actually changes ownership
    /// of one of this leaf's slots.
    /// </para>
    /// </summary>
    [Id(16)] public int[]? MovedAwaySlots { get; set; }

    /// <summary>
    /// The <see cref="ShardMap.VirtualShardCount"/> in force at the moment
    /// <see cref="MovedAwaySlots"/> was populated. Required to recompute
    /// the slot for an incoming key via
    /// <see cref="ShardMap.GetVirtualSlot(string, int)"/>. <c>null</c>
    /// when <see cref="MovedAwaySlots"/> is empty (the leaf has never
    /// recorded a moved-away slot).
    /// </summary>
    [Id(17)] public int? MovedAwayVirtualShardCount { get; set; }

    /// <summary>
    /// Grain reference to this leaf's parent internal node, or
    /// <see langword="null"/> when this leaf is the shard root (the
    /// flat-tree case where the root is a leaf, or any leaf whose state
    /// pre-dates this slot). Persisted exactly once by
    /// <c>SetParentAsync</c>, called by the shard root at leaf-create
    /// time and re-called on split when a new sibling is grafted
    /// beneath a parent. Consulted whenever the leaf's incremental
    /// <see cref="ProjectionHash"/> changes so the leaf can forward
    /// a <see cref="ChildDigestSnapshot"/> upward to maintain the
    /// chained internal-node fold (an internal-only optimisation
    /// behind the public <see cref="LeafProjectionDigest"/> surface).
    /// </summary>
    [Id(18)] public GrainId? ParentId { get; set; }

    /// <summary>
    /// Per-partition projection-checkpoint offsets when the leaf's
    /// owning tree is configured with <see cref="LatticeOptions.WalPartitions"/>
    /// greater than <c>1</c>. The array is indexed by WAL partition;
    /// each slot holds the highest WAL offset whose mutation has been
    /// durably applied to this leaf's projection from that specific
    /// partition (semantics identical to the legacy scalar
    /// <see cref="ProjectionCheckpointOffset"/> slot, but scoped to a
    /// single partition's offset space).
    /// <para>
    /// <see langword="null"/> in the steady state (single-partition
    /// trees and any leaf whose persisted state pre-dates this slot),
    /// in which case the activation-time materialiser falls back to
    /// the scalar <see cref="ProjectionCheckpointOffset"/> for
    /// partition <c>0</c> only - preserving the legacy single-
    /// partition replay semantics for wire-compat. When non-null, the
    /// array's length is exactly <c>WalPartitions</c> and partition
    /// <c>0</c>'s entry is kept in sync with the scalar slot so a
    /// downgrade to a legacy silo (or a re-read by a legacy-shaped
    /// consumer) degrades gracefully into the single-partition replay
    /// path on read.
    /// </para>
    /// <para>
    /// Per-entry "nothing applied" sentinel is <c>-1</c>, matching the
    /// scalar slot's convention.
    /// </para>
    /// </summary>
    [Id(19)] public long[]? ProjectionCheckpointOffsetsByPartition { get; set; }
}
