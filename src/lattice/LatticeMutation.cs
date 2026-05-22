using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A single mutation observed by an <see cref="IMutationObserver"/>.
/// For <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>
/// the record describes a single key's post-commit LWW metadata; for
/// <see cref="MutationKind.DeleteRange"/> it describes the half-open range
/// <c>[StartKey, EndExclusiveKey)</c> that was tombstoned.
/// <para>
/// The shape is deliberately flat (instead of embedding
/// <c>LwwValue&lt;byte[]&gt;</c>) to keep the public extensibility contract
/// independent of the library's internal wire DTOs.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeMutation)]
[Immutable]
public readonly record struct LatticeMutation
{
    /// <summary>The logical tree id the mutation was committed to.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The kind of mutation.</summary>
    [Id(1)] public MutationKind Kind { get; init; }

    /// <summary>
    /// The key for <see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>,
    /// or the inclusive start key for <see cref="MutationKind.DeleteRange"/>.
    /// </summary>
    [Id(2)] public string Key { get; init; }

    /// <summary>
    /// The exclusive end key for <see cref="MutationKind.DeleteRange"/>;
    /// <c>null</c> for <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>.
    /// </summary>
    [Id(3)] public string? EndExclusiveKey { get; init; }

    /// <summary>
    /// The committed value for <see cref="MutationKind.Set"/>; <c>null</c>
    /// for deletes and range deletes.
    /// </summary>
    [Id(4)] public byte[]? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the committed entry
    /// for <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>.
    /// For <see cref="MutationKind.DeleteRange"/> this carries the HLC of the
    /// tombstone batch (or <see cref="HybridLogicalClock.Zero"/> when the
    /// range matched nothing).
    /// </summary>
    [Id(5)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// <c>true</c> when the committed entry is a tombstone
    /// (<see cref="MutationKind.Delete"/> and <see cref="MutationKind.DeleteRange"/>
    /// always set this).
    /// </summary>
    [Id(6)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the committed entry expires, or <c>0</c>
    /// when it does not expire. Preserved end-to-end for
    /// <see cref="MutationKind.Set"/>; always <c>0</c> for deletes.
    /// </summary>
    [Id(7)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored this mutation, or
    /// <c>null</c> for a local write. Populated at commit time from the
    /// ambient <see cref="LatticeOriginContext"/> so replication-aware
    /// observers can skip re-forwarding mutations that originated
    /// elsewhere and avoid replication loops. Always <c>null</c> on
    /// <see cref="MutationKind.DeleteRange"/> unless the range-delete call
    /// was itself stamped with an origin - range deletes read the context
    /// at publish time rather than pulling from a per-key <c>LwwValue</c>.
    /// </summary>
    [Id(8)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Sparse vector-clock frontier captured at commit time, or
    /// <c>null</c> when the writer did not supply one. Mirrors
    /// <see cref="LwwValue{T}.VectorClock"/> on per-key
    /// <see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>
    /// observations and the ambient
    /// <see cref="LatticeVectorClockContext"/> on
    /// <see cref="MutationKind.DeleteRange"/>. Replication-aware observers
    /// pin or compare the frontier as needed; the library itself does
    /// not interpret it.
    /// </summary>
    /// <remarks>
    /// <see cref="Primitives.VersionVector"/> is a mutable reference type
    /// whose <see cref="Primitives.VersionVector.Entries"/> dictionary is
    /// publicly mutable. The instance carried on this slot is shared with
    /// the originating commit site and may continue to be advanced after
    /// <see cref="IMutationObserver.OnMutationAsync"/> returns. Observers
    /// that retain the frontier past the observer call - for example to
    /// stamp it on a downstream wire envelope - must defensively snapshot
    /// the value (typically via <see cref="Primitives.VersionVector.Clone"/>);
    /// the replication package's built-in observer does this internally so
    /// every emitted <c>WalRecord</c> is detached from later producer-side
    /// advances.
    /// </remarks>
    [Id(9)] public Primitives.VersionVector? VectorClock { get; init; }

    /// <summary>
    /// Identifier of the logical transaction that produced this mutation.
    /// Single-key writes (<c>SetAsync</c>, <c>DeleteAsync</c>, <c>SetIfVersionAsync</c>,
    /// <c>GetOrSetAsync</c>) get a fresh <see cref="Guid"/> per call.
    /// A non-atomic <c>SetManyAsync</c> batch shares one id across every
    /// per-key emit. A user <c>DeleteRangeAsync</c> call shares one id
    /// across every per-shard <see cref="MutationKind.DeleteRange"/> emit.
    /// An atomic-write saga (<c>SetManyAtomicAsync</c>) shares a single,
    /// persisted id across every per-key emit produced by both the
    /// execute and compensate phases - replication consumers can therefore
    /// capture vector-clock frontier (or any other batch-wide invariant)
    /// once per transaction and apply it identically to every emit.
    /// Defaults to <see cref="Guid.Empty"/> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(10)] public Guid TransactionId { get; init; }

    /// <summary>
    /// Classifies the mutation as a user-driven write
    /// (<see cref="MutationCategory.User"/>, the default) or a
    /// library-internal maintenance write
    /// (<see cref="MutationCategory.Maintenance"/>). Replication-aware
    /// observers skip the WAL append for
    /// <see cref="MutationCategory.Maintenance"/> emits on replicated
    /// trees so structural maintenance does not cross cluster boundaries.
    /// Independent of <see cref="OriginClusterId"/> - a remote-origin
    /// maintenance emit would still be
    /// <see cref="MutationCategory.Maintenance"/>. Defaults to
    /// <see cref="MutationCategory.User"/> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(11)] public MutationCategory Category { get; init; }

    /// <summary>
    /// Pre-merge author's delta in opaque-bytes form, or
    /// <see langword="null"/> when the producer did not author a typed
    /// CRDT delta (plain LWW <c>Set</c> / <c>Delete</c> writes). When
    /// non-<see langword="null"/>, the bytes are the Orleans-serialised
    /// form of one of the public typed delta DTOs - the one matching the
    /// <see cref="LatticeMergeMode"/> stamped on the same record. The
    /// receiver dispatches on the mode to pick the right deserialiser
    /// and call <c>MergeDelta</c> on the loaded primitive, which is what
    /// makes CRDT replication converge by replaying the author's intent
    /// rather than the post-merge state (the latter loses concurrent-
    /// write information for non-LWW CRDTs).
    /// <para>
    /// The wire id <c>13</c> matches the slot previously named
    /// <c>DeltaPayload</c>; the rename is source-breaking but wire-
    /// compatible. The companion <c>DeltaKind</c> string (formerly id
    /// <c>12</c>) was retired in the same change because receivers now
    /// dispatch on <see cref="LatticeMergeMode"/>; that wire id is
    /// permanently reserved and must never be reused for a different
    /// field.
    /// </para>
    /// </summary>
    [Id(13)] public byte[]? Delta { get; init; }

    /// <summary>
    /// Total number of mutations in the enclosing atomic transaction
    /// (a <c>SetManyAtomicAsync</c> saga). Single-key writes and
    /// non-atomic batches stamp <c>0</c>; an atomic N-key write stamps
    /// <c>N</c> on every per-key emit produced by both the execute and
    /// compensate phases. Sibling membership is keyed by
    /// <see cref="TransactionId"/>; this slot is the canonical
    /// completeness signal a receiver-side staging buffer reads to
    /// detect when every entry of a batch has arrived. Independent of
    /// <see cref="OriginClusterId"/> and <see cref="Category"/>: a
    /// remote-origin or maintenance atomic emit (no such caller exists
    /// today, but the slot is shape-stable for it) still carries the
    /// same size. Defaults to <c>0</c> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(14)] public int AtomicBatchSize { get; init; }

    /// <summary>
    /// Zero-based position of this mutation within the enclosing
    /// atomic transaction. Defined only when
    /// <see cref="AtomicBatchSize"/> is greater than <c>0</c>; for
    /// non-atomic writes (<see cref="AtomicBatchSize"/> = <c>0</c>) the
    /// slot is unused and stamps <c>0</c>. Within a batch the index
    /// covers <c>0..AtomicBatchSize-1</c> exactly once each, derived
    /// deterministically from the saga's per-operation iteration order;
    /// compensation rolls inherit the original prepare's index for
    /// each key. Defaults to <c>0</c> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(15)] public int AtomicBatchIndex { get; init; }

    /// <summary>
    /// <c>true</c> when this mutation is a saga prepare-phase write that
    /// must route into the per-leaf pending-tx map rather than the
    /// visible projection. Flipped to live state by a subsequent terminal
    /// <see cref="MutationKind.TxCommit"/> mutation under the same
    /// <see cref="TransactionId"/>; dropped by a terminal
    /// <see cref="MutationKind.TxAbort"/>. Always <c>false</c> on
    /// <see cref="MutationKind.TxCommit"/> and
    /// <see cref="MutationKind.TxAbort"/> terminal-mark mutations
    /// themselves. Defaults to <c>false</c> for wire compatibility with
    /// observers persisted before this field existed.
    /// </summary>
    [Id(16)] public bool IsPrepared { get; init; }

    /// <summary>
    /// Logical chain-shard index that authored this mutation - i.e. the
    /// <c>shardIndex</c> half of the originating
    /// <c>ShardRootGrain</c>'s <c>{treeId}/{shardIndex}</c> grain key.
    /// Stamped at commit time by the foreground commit path (the
    /// per-key writer reads it from the leaf's persisted shard index;
    /// the saga terminal writer reads it from the shard root's parsed
    /// key). Used by activation-time WAL replay on the leaf to filter
    /// out records authored by sibling chain shards that share a WAL
    /// partition - without this slot a leaf in shard <c>5</c> reading
    /// the same WAL partition as a leaf in shard <c>2</c> would absorb
    /// the sibling shard's keys into its own projection on every
    /// reactivation. Independent of <see cref="OriginClusterId"/> (which
    /// identifies the originating cluster, not the originating shard).
    /// Strictly additive on the wire: legacy peers and entries authored
    /// before this slot existed decode as <c>0</c>, which a leaf with no
    /// persisted <c>LeafNodeState.ShardIndex</c> (also a legacy state
    /// shape) treats as "apply unconditionally" for back-compat with
    /// the V1 single-shard layout.
    /// </summary>
    [Id(17)] public int ShardIndex { get; init; }

    /// <summary>
    /// <c>true</c> when this mutation is a cross-migration LWW backstop
    /// write authored by the leaf-side terminal handler
    /// (<c>BPlusLeafGrain.ApplyTxTerminalAsync</c>) for a saga key whose
    /// prepare-phase shadow-forward was lost to a mid-saga shard-split
    /// or drain race. Distinguishes the write from an ordinary
    /// <see cref="MutationKind.Set"/> on the wire and on the
    /// <c>orleans.lattice.leaf.write.duration</c> histogram, where the
    /// backstop is tagged <c>kind=backstop</c> so operators can size
    /// backstop traffic against ordinary writes. Semantically the
    /// backstop is just a Set at the projection level - receiver-side
    /// LWW resolution treats it identically - but carrying the flag
    /// lets downstream consumers filter, count, or alert on the
    /// failure mode independently. Defaults to <c>false</c> for wire
    /// compatibility with mutations persisted before this field
    /// existed.
    /// </summary>
    [Id(18)] public bool IsBackstop { get; init; }

    /// <summary>
    /// Total number of source-cluster shards the enclosing atomic-write
    /// saga touched. Stamped only on terminal mutations
    /// (<see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/>) by the saga coordinator at
    /// terminal-broadcast time, reading the authoritative participant
    /// set from <c>TxRegistryState.Participants</c>. Every prepare-phase
    /// per-key emit stamps <c>0</c>; non-saga single-key writes stamp
    /// <c>0</c>; a saga whose participant union is empty (degenerate
    /// zero-entries case) stamps <c>0</c>. Mirrored verbatim onto
    /// <see cref="WalRecord.AtomicShardCount"/> by the replication
    /// observer so the cross-cluster apply path can gate the
    /// per-tree <c>ITxRegistryGrain</c> linearization mark until every
    /// per-shard terminal has arrived.
    /// <para>
    /// Defaults to <c>0</c> for wire compatibility with observers
    /// persisted before this field existed; the receiver-side gating
    /// path treats <c>0</c> as "no gating information" and falls back
    /// to the legacy "mark on first terminal" semantics, so rolling
    /// upgrades involving mixed-version shippers degrade gracefully
    /// to the best-effort cross-cluster atomic-visibility contract.
    /// </para>
    /// </summary>
    [Id(19)] public int AtomicShardCount { get; init; }

    /// <summary>
    /// <c>true</c> when this mutation is a leaf-level merge write
    /// authored by <c>BPlusLeafGrain.MergeEntriesAsync</c> or
    /// <c>BPlusLeafGrain.MergeManyAsync</c> (replication apply, tree
    /// merge, snapshot restore, sibling redistribute on split, or
    /// cross-shard migration import) or a tombstone-of-tombstone
    /// authored by <c>BPlusLeafGrain.CompactTombstonesAsync</c>.
    /// Distinguishes the write from an ordinary
    /// <see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>
    /// on the wire and on the
    /// <c>orleans.lattice.leaf.write.duration</c> histogram, where the
    /// merge / compact paths are tagged <c>kind=merge</c> and
    /// <c>kind=compact</c> so operators can size sibling-redistribute,
    /// replication-apply, and compaction traffic against ordinary
    /// writes. Semantically the merge / compact envelope is just a
    /// Set / Delete at the projection level - receiver-side LWW
    /// resolution treats it identically - but carrying the flag lets
    /// downstream consumers filter, count, or alert on the routing
    /// independently. Defaults to <c>false</c> for wire compatibility
    /// with mutations persisted before this field existed.
    /// </summary>
    [Id(20)] public bool IsMerge { get; init; }
}
