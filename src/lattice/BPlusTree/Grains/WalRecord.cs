using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A single change-feed record captured at commit time by the replication
/// package. Authored synchronously inside the originating grain's write
/// path (via the core <see cref="IMutationObserver"/> hook) and forwarded
/// to the registered <c>IReplogSink</c> before the grain method returns,
/// so the captured value is guaranteed to be the value that was just
/// committed - replication consumers never need to re-read the primary.
/// <para>
/// The shape is deliberately flat: every field necessary to apply the
/// mutation on a remote cluster (op, key, range bound, value bytes,
/// hybrid-logical clock, tombstone flag, expiry, origin cluster id) is a
/// top-level <c>[Id]</c> slot so the record round-trips through the
/// Orleans serializer without depending on any internal core DTO.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalRecord)]
[Immutable]
public readonly record struct WalRecord
{
    /// <summary>The logical tree id the mutation was committed to.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The kind of mutation.</summary>
    [Id(1)] public MutationKind Op { get; init; }

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
    /// <para>
    /// <b>Wire-shape note:</b> the canonical
    /// <see cref="OrleansBinaryWalRecordEncoder"/> codec strips this
    /// slot on CRDT-mode <see cref="MutationKind.Set"/> entries that
    /// carry a non-<see langword="null"/> <see cref="Delta"/> (every
    /// <see cref="LatticeMergeMode"/> other than
    /// <see cref="LatticeMergeMode.LwwRegister"/>): the receiver-side
    /// apply path dispatches every typed CRDT mode through
    /// <see cref="Delta"/> + the primitive's <c>MergeDelta</c>, so
    /// the full-state byte payload is pure overhead on both the
    /// storage WAL and the cross-cluster wire. The non-prepared
    /// CRDT-delta producer no longer materialises the post-merge state
    /// into this slot at all - <see cref="Orleans.Lattice.BPlusTree.Grains.WalRecordBuilder.ForCrdtDelta"/>
    /// leaves it <see langword="null"/> in the in-grain instance too, so
    /// the durable writer path pays no O(state) post-merge serialisation
    /// to feed a slot the encoder drops. Both the receiver-side apply and
    /// the activation-time cold-rebuild replay reconstruct the post-fold
    /// state by folding <see cref="Delta"/> into the current visible state.
    /// Prepared saga entries (<see cref="IsPrepared"/>) are the exception:
    /// they keep <see cref="Value"/> at both layers because the receiver
    /// buckets the merged-state value into its per-tx pending bucket and
    /// folds the typed <see cref="Delta"/> only on the terminal commit.
    /// <see cref="LatticeMergeMode.LwwRegister"/> entries are unaffected
    /// and continue to carry the canonical payload at both wire and
    /// storage layers.
    /// </para>
    /// </summary>
    [Id(4)] public byte[]? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the committed entry
    /// for <see cref="MutationKind.Set"/> and <see cref="MutationKind.Delete"/>.
    /// For <see cref="MutationKind.DeleteRange"/> this carries the
    /// producer's authoring issue HLC - the single HLC pinned across the
    /// entire range-delete fan-out via <see cref="LatticeHlcOverrideContext"/>
    /// and stamped verbatim on every per-leaf tombstone. Receivers honour
    /// this value on their own apply seam so cross-origin LWW resolution
    /// agrees with the producer: a DeleteRange authored at frontier <c>T</c>
    /// cannot overwrite a foreign-origin write whose HLC is strictly
    /// greater than <c>T</c>. Legacy WAL entries persisted before this
    /// invariant was enforced carry <see cref="HybridLogicalClock.Zero"/>;
    /// receivers detect the sentinel and fall back to a freshly-ticked
    /// local HLC for back-compat (the historical buggy behaviour - cross
    /// -origin LWW is not preserved in that mode and operators should
    /// upgrade producers).
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
    /// Identifier of the cluster that authored this mutation. Stamped at
    /// commit time from either the mutation's pre-existing
    /// <see cref="LatticeMutation.OriginClusterId"/> (for replays of a
    /// remote write) or the validated local cluster id (for local-origin
    /// writes when the replication package is registered; empty in
    /// single-cluster hosts). Receivers use this to attribute origin and
    /// break replication cycles. Will be non-empty for every entry
    /// produced by the replication-package commit-time observer because
    /// its options validator rejects an empty cluster id at first-resolve
    /// time; empty for entries authored by single-cluster hosts and for
    /// hand-constructed entries used in tests.
    /// </summary>
    [Id(8)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Declared <see cref="LatticeMergeMode"/> the receiver should use to
    /// apply this entry. Stamped at commit time from the producer-side
    /// <see cref="ILatticeMergeModeResolver"/>; defaults to
    /// <see cref="LatticeMergeMode.LwwRegister"/> for hand-constructed
    /// entries and for entries decoded from older wire formats that
    /// pre-date this field.
    /// <para>
    /// <b>Wire-shape note:</b> this slot is serialised at wire id
    /// <c>26</c> so the declared merge mode is durable on the encoded
    /// record and recoverable without any out-of-band context. The
    /// canonical <see cref="OrleansBinaryWalRecordEncoder"/> codec omits
    /// the slot from the bytes whenever it holds the enum default
    /// (<see cref="LatticeMergeMode.LwwRegister"/>) - the overwhelmingly
    /// common plain-LWW write - so the steady-state byte shape is
    /// unchanged; only the typed-CRDT modes (which already drop the
    /// stripped post-merge <see cref="Value"/> payload) pay the one or
    /// two extra bytes. Persisting the mode is required because the WAL
    /// <i>storage</i> replay path has no per-batch framing header to
    /// hoist it from, and the activation-cached
    /// <see cref="ILatticeMergeModeResolver.Resolve(string)"/> result is
    /// not a sound source: it returns <see langword="null"/> for every
    /// tree the resolver does not know (every tree on a single-cluster
    /// host, and any replicated-host tree absent from the configured
    /// replicated set), so a delta-only CRDT record would replay as an
    /// LWW null and silently empty the key (see issue #926).
    /// <para>
    /// The cross-cluster ship path still carries the mode once per batch
    /// in the framing header (<c>EncodedBatchHeader.Mode</c>) and the
    /// receiver-side apply seam re-stamps every decoded entry via
    /// <see cref="IWalRecordEncoder.Decode(System.ReadOnlySpan{byte}, string, LatticeMergeMode)"/>;
    /// that override is now idempotent because the decoded record already
    /// carries the same mode from its own bytes. The wire id <c>9</c>
    /// (the slot's original tag, retired when it was briefly de-tagged)
    /// is permanently reserved and must never be reused for a different
    /// field, because legacy WAL bytes authored while it was tagged still
    /// carry it; Orleans silently drops the unknown id <c>9</c> on decode
    /// and such records fall back to the default, exactly as a pre-CRDT
    /// LWW entry does.
    /// </para>
    /// </para>
    /// </summary>
    [Id(26)]
    public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// Sparse causal-plus vector-clock frontier
    /// (<c>{originClusterId &#8594; HybridLogicalClock}</c>) captured at
    /// commit time. Mirrors the ambient
    /// <see cref="LatticeVectorClockContext"/> on the originating grain
    /// write (preserved verbatim from
    /// <see cref="LatticeMutation.VectorClock"/>) so receivers can run a
    /// causal dependency check before applying the entry. The slot is
    /// strictly additive on the wire: legacy peers and entries authored
    /// before this slot existed decode as <see langword="null"/>, which
    /// receivers treat as the empty frontier so the apply path is
    /// indistinguishable from today's per-origin-only high-water-mark
    /// check. Stored as the absolute frontier on the in-memory record;
    /// transports that need a more compact form encode through
    /// <c>VectorClockCodec</c>
    /// </summary>
    [Id(10)] public Orleans.Lattice.VersionVector? VectorClock { get; init; }

    /// <summary>
    /// Compact representation of the causal predecessors of this entry.
    /// Initially aliased one-to-one with <see cref="VectorClock"/>: the
    /// canonical commit-time observer stamps an identical reference into
    /// both slots so a receiver that only consults
    /// <see cref="DependencySummary"/> reads the same frontier the
    /// dependency check sees. Reserved as a distinct slot so a future
    /// summary shape (for example a Bloom filter over predecessor HLCs)
    /// can ship without re-numbering the wire format. Decodes as
    /// <see langword="null"/> for legacy peers and pre-causal-plus
    /// entries.
    /// </summary>
    [Id(11)] public Orleans.Lattice.VersionVector? DependencySummary { get; init; }

    /// <summary>
    /// Pre-merge author's delta in opaque-bytes form, or
    /// <see langword="null"/> when the producer did not author a typed
    /// CRDT delta. Mirrored verbatim from
    /// <see cref="LatticeMutation.Delta"/>. When non-<see langword="null"/>,
    /// the bytes are the Orleans-serialised form of the public typed
    /// delta DTO matching <see cref="Mode"/>; receivers dispatch on the
    /// mode to pick the deserialiser and call <c>MergeDelta</c> on the
    /// loaded primitive. Strictly additive on the wire; legacy peers
    /// and entries authored before this slot existed decode as
    /// <see langword="null"/>, which receivers treat as "no typed delta,
    /// fall back to the LWW / opaque-bytes apply over
    /// <see cref="Value"/>".
    /// <para>
    /// The wire id <c>13</c> matches the slot previously named
    /// <c>DeltaPayload</c>; the rename is source-breaking but wire-
    /// compatible. The companion <c>DeltaKind</c> string (formerly id
    /// <c>12</c>) was retired in the same change because receivers now
    /// dispatch on <see cref="Mode"/>; that wire id is permanently
    /// reserved and must never be reused for a different field.
    /// </para>
    /// </summary>
    [Id(13)] public byte[]? Delta { get; init; }

    /// <summary>
    /// Total number of entries in the enclosing atomic transaction
    /// (a <c>SetManyAtomicAsync</c> saga). Single-key writes and
    /// non-atomic batches stamp <c>0</c>; an atomic N-key write stamps
    /// <c>N</c> on every per-key emit. Mirrored verbatim from the
    /// producing <see cref="LatticeMutation.AtomicBatchSize"/>. Sibling
    /// membership is keyed by the existing <c>TransactionId</c> the
    /// core mutation observer already supplies; this slot is the
    /// canonical completeness signal a receiver-side staging buffer
    /// reads to detect when every entry of a batch has arrived.
    /// Strictly additive on the wire: legacy peers and entries
    /// authored before this slot existed decode as <c>0</c>, which a
    /// receiver with atomic-batch delivery enabled treats identically
    /// to a single-key write.
    /// </summary>
    [Id(14)] public int AtomicBatchSize { get; init; }

    /// <summary>
    /// Zero-based position of this entry within the enclosing atomic
    /// transaction; <c>0</c> for non-atomic writes. Mirrored verbatim
    /// from the producing <see cref="LatticeMutation.AtomicBatchIndex"/>.
    /// Strictly additive on the wire: legacy peers and entries
    /// authored before this slot existed decode as <c>0</c>.
    /// </summary>
    [Id(15)] public int AtomicBatchIndex { get; init; }

    /// <summary>
    /// Stable identifier of the enclosing atomic transaction
    /// (<c>SetManyAtomicAsync</c> saga). Mirrored verbatim from the
    /// producing <see cref="LatticeMutation.TransactionId"/>; every
    /// per-key emit inside the same saga shares a single
    /// transaction id, including compensation rolls. Single-key
    /// non-saga writes mirror whatever the producer-side ambient
    /// <c>LatticeTransactionContext</c> supplies; in practice this
    /// is <see cref="Guid.Empty"/> for plain
    /// <c>SetAsync</c> / <c>DeleteAsync</c> calls.
    /// <para>
    /// Strictly additive on the wire: legacy peers and entries
    /// authored before this slot existed decode as
    /// <see cref="Guid.Empty"/>, which receivers treat identically
    /// to a single-key non-atomic write regardless of the
    /// <see cref="AtomicBatchSize"/> slot's value (an entry without
    /// a transaction id has no sibling membership to detect).
    /// </para>
    /// </summary>
    [Id(16)] public Guid TransactionId { get; init; }

    /// <summary>
    /// <c>true</c> when this entry is a saga prepare-phase per-key
    /// write - a <see cref="MutationKind.Set"/> or <see cref="MutationKind.Delete"/>
    /// authored inside a <c>SetManyAtomicAsync</c> saga whose terminal
    /// mark has not yet been appended on its WAL partition. Mirrored
    /// verbatim from the producing
    /// <see cref="LatticeMutation.IsPrepared"/>. Receivers route
    /// <c>IsPrepared=true</c> entries into the per-leaf in-memory
    /// pending-tx map and filter them out of the visible projection
    /// until the matching <see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/> terminal mark replays through
    /// the standard projection path. <see cref="MutationKind.TxCommit"/>
    /// and <see cref="MutationKind.TxAbort"/> entries always carry
    /// <see cref="IsPrepared"/> as <c>false</c> - terminals are the
    /// resolution, not the prepare. Strictly additive on the wire:
    /// legacy peers and entries authored before this slot existed
    /// decode as <c>false</c>, which preserves the legacy
    /// "every Set / Delete is immediately visible" semantics.
    /// </summary>
    [Id(17)] public bool IsPrepared { get; init; }

    /// <summary>
    /// Logical chain-shard index that authored this entry - mirrored
    /// verbatim from <see cref="LatticeMutation.ShardIndex"/>. Stamped
    /// at commit time by the foreground commit path. Used by
    /// activation-time WAL replay on the receiving leaf to filter out
    /// records authored by sibling chain shards that share a WAL
    /// partition. Strictly additive on the wire: legacy peers and
    /// entries authored before this slot existed decode as <c>0</c>,
    /// which a receiving leaf with no persisted
    /// <c>LeafNodeState.ShardIndex</c> (also a legacy state shape)
    /// treats as "apply unconditionally" for back-compat with the V1
    /// single-shard layout. Replication-compatible: the slot rides
    /// the existing producer/consumer wire path so peers running
    /// pre-Option A code ignore the field, and post-Option A peers
    /// stamp the receiver's local shard index when re-committing
    /// through the local foreground path (so the filter at the
    /// receiver is keyed against the receiver's own routing).
    /// </summary>
    [Id(18)] public int ShardIndex { get; init; }

    /// <summary>
    /// Total number of source-cluster shards the enclosing atomic-write
    /// saga touched - i.e. the count of distinct
    /// <c>(originClusterId, sourceShardIndex)</c> terminal records the
    /// producer ships for this transaction. Stamped only on terminal
    /// records (<see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/>) at terminal-broadcast time
    /// from the saga coordinator's authoritative
    /// <c>TxRegistryState.Participants</c> set - which records every
    /// source shard that handled a prepare-phase write, including
    /// shadow-forward destinations. Every prepare-phase entry stamps
    /// <c>0</c>; non-saga writes stamp <c>0</c>.
    /// <para>
    /// Used by the receiver's <c>LatticeGrain.ApplyTxTerminalAsync</c>
    /// to gate the per-tree <c>ITxRegistryGrain</c> linearization mark
    /// until every per-shard terminal of the saga has arrived. Without
    /// this gate a receiver reader concurrent with replication of a
    /// multi-shard <c>SetManyAtomicAsync</c> can observe a strict
    /// subset of the saga's keys at the new value while the remaining
    /// shards still show the pre-saga value, because the first
    /// terminal flips the registry to <c>Committed</c> and unblocks
    /// reader-side resolution against the partial set of shards whose
    /// prepared records have arrived. The gating tally is keyed by
    /// source shard index, tolerates duplicate-delivery retries, and
    /// adopts a monotonically non-decreasing
    /// <c>max(seenExpected, incomingAtomicShardCount)</c> view of the
    /// expected count so a source-side mid-saga shadow-forward split
    /// that grows the touched-shard set between successive terminals
    /// is absorbed without missing the gate.
    /// </para>
    /// <para>
    /// Strictly additive on the wire: legacy peers and entries
    /// authored before this slot existed decode as <c>0</c>, which
    /// the receiver treats as "no gating information available" and
    /// falls back to the legacy "mark on first terminal" semantics -
    /// preserving today's best-effort cross-cluster atomic visibility
    /// against legacy shippers.
    /// </para>
    /// </summary>
    [Id(19)] public int AtomicShardCount { get; init; }

    /// <summary>
    /// <c>true</c> when the entry was authored by the leaf's merge
    /// channel (<see cref="MutationKind.Set"/> / <see cref="MutationKind.Delete"/>
    /// produced by <c>MergeEntriesAsync</c>, sibling-redistribute,
    /// snapshot-restore, or replication-apply) rather than a foreground
    /// caller-issued write. Mirrored verbatim from the producing
    /// <see cref="LatticeMutation.IsMerge"/>. Strictly additive on the
    /// wire: legacy peers and entries authored before this slot existed
    /// decode as <c>false</c>, which preserves the pre-slot
    /// "every entry is a foreground write" assumption.
    /// </summary>
    [Id(20)] public bool IsMerge { get; init; }

    /// <summary>
    /// <c>true</c> when the entry was authored by the saga
    /// cross-migration backstop path (a destination leaf catching up
    /// missing pre-saga values from the source leaf at terminal time).
    /// Mirrored verbatim from the producing
    /// <see cref="LatticeMutation.IsBackstop"/>. Strictly additive on
    /// the wire: legacy peers and entries authored before this slot
    /// existed decode as <c>false</c>.
    /// </summary>
    [Id(21)] public bool IsBackstop { get; init; }

    /// <summary>
    /// Classifies the entry as a user-driven write
    /// (<see cref="MutationCategory.User"/>) or a maintenance-driven
    /// write (<see cref="MutationCategory.Maintenance"/> - structural
    /// rewrites, tombstone reaps). Mirrored verbatim from the producing
    /// <see cref="LatticeMutation.Category"/>. Strictly additive on the
    /// wire: legacy peers and entries authored before this slot existed
    /// decode as <see cref="MutationCategory.User"/>, matching the
    /// pre-slot wire-compat default applied by
    /// <c>WalRecordConverter.FromWalRecord</c>.
    /// </summary>
    [Id(22)] public MutationCategory Category { get; init; }

    /// <summary>
    /// Explicit set of keys a predicate-filtered
    /// <see cref="MutationKind.DeleteRange"/> matched at write time, or
    /// <see langword="null"/> for an ordinary (unconditional) range delete.
    /// <para>
    /// A predicate-filtered range delete is not a pure
    /// "tombstone <c>[Key, EndExclusiveKey)</c>" closure: the authoring leaf
    /// evaluates the predicate <b>once</b> against each candidate value and
    /// records the matched keys here, so replay and replication apply tombstone
    /// exactly this set with no predicate re-evaluation. This keeps recovery
    /// deterministic and lets receivers - whose stored values may differ -
    /// reproduce the identical tombstone set. When <see langword="null"/> the
    /// range bounds drive the closure exactly as before, so entries persisted
    /// before this slot existed (and every non-predicate range delete) decode
    /// unchanged. Strictly additive on the wire.
    /// </para>
    /// </summary>
    [Id(23)] public IReadOnlyList<string>? MatchedKeys { get; init; }

    /// <summary>
    /// The cross-tree coordinator key (the caller-supplied
    /// <c>operationId</c>) when this terminal belongs to a sub-saga of a
    /// multi-tree atomic write, or <see langword="null"/> for a
    /// single-tree saga terminal and every non-terminal entry. Stamped
    /// only on terminal records (<see cref="MutationKind.TxCommit"/> /
    /// <see cref="MutationKind.TxAbort"/>) of a cross-tree sub-saga, from
    /// the ambient set by <c>AtomicWriteGrain.BroadcastTerminalsAsync</c>.
    /// <para>
    /// Used together with <see cref="CrossTreeParticipants"/> by the
    /// receiver's replication-apply path to drive the <b>receiver-side
    /// cross-tree visibility barrier</b>: instead of flipping this tree's
    /// per-tree registry the moment this tree's per-shard terminal tally
    /// completes (which would expose tree A committed while tree B is
    /// still pre-saga on the receiver), the receiver delegates this tree's
    /// decision to a per-operation <c>ILatticeCrossTreeReceiverGrain</c>
    /// keyed by <c>(originClusterId, operationId)</c> and only releases
    /// every participating tree together once every participant tree the
    /// receiver replicates has had its terminal arrive.
    /// </para>
    /// <para>
    /// Strictly additive on the wire: legacy peers and entries authored
    /// before this slot existed (and every single-tree saga terminal)
    /// decode as <see langword="null"/>, which the receiver treats as
    /// "not a cross-tree terminal" and routes through the existing
    /// single-tree per-shard gate unchanged.
    /// </para>
    /// </summary>
    [Id(24)] public string? CrossTreeOperationId { get; init; }

    /// <summary>
    /// The full, canonical (ordinal-sorted, de-duplicated) set of logical
    /// tree ids that participate in the enclosing cross-tree atomic write,
    /// or <see langword="null"/> for a single-tree saga terminal and every
    /// non-terminal entry. Stamped only on the terminal records of a
    /// cross-tree sub-saga alongside <see cref="CrossTreeOperationId"/>.
    /// <para>
    /// The receiver intersects this set with the trees it actually
    /// replicates (from <c>LatticeReplicationOptions.ReplicatedTrees</c>)
    /// to compute the <i>wait set</i> the receiver-side barrier blocks on.
    /// A receiver that replicates only a subset of the participating trees
    /// waits only for that subset's terminals - a cross-tree batch
    /// spanning a mix of replicated and non-replicated trees is a valid,
    /// supported configuration. Every terminal of one operation carries
    /// the identical participant set; the receiver freezes it on the first
    /// terminal and rejects a mismatching later terminal.
    /// </para>
    /// <para>
    /// Strictly additive on the wire: legacy / single-tree terminals
    /// decode as <see langword="null"/>.
    /// </para>
    /// </summary>
    [Id(25)] public IReadOnlyList<string>? CrossTreeParticipants { get; init; }
}

