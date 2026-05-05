using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

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
[Alias(ReplicationTypeAliases.ReplogEntry)]
[Immutable]
public readonly record struct ReplogEntry
{
    /// <summary>The logical tree id the mutation was committed to.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The kind of mutation.</summary>
    [Id(1)] public ReplogOp Op { get; init; }

    /// <summary>
    /// The key for <see cref="ReplogOp.Set"/> / <see cref="ReplogOp.Delete"/>,
    /// or the inclusive start key for <see cref="ReplogOp.DeleteRange"/>.
    /// </summary>
    [Id(2)] public string Key { get; init; }

    /// <summary>
    /// The exclusive end key for <see cref="ReplogOp.DeleteRange"/>;
    /// <c>null</c> for <see cref="ReplogOp.Set"/> and <see cref="ReplogOp.Delete"/>.
    /// </summary>
    [Id(3)] public string? EndExclusiveKey { get; init; }

    /// <summary>
    /// The committed value for <see cref="ReplogOp.Set"/>; <c>null</c>
    /// for deletes and range deletes.
    /// </summary>
    [Id(4)] public byte[]? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the committed entry
    /// for <see cref="ReplogOp.Set"/> and <see cref="ReplogOp.Delete"/>.
    /// For <see cref="ReplogOp.DeleteRange"/> this carries
    /// <see cref="HybridLogicalClock.Zero"/> because a single range may
    /// produce many per-leaf HLCs that cannot be faithfully collapsed.
    /// </summary>
    [Id(5)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// <c>true</c> when the committed entry is a tombstone
    /// (<see cref="ReplogOp.Delete"/> and <see cref="ReplogOp.DeleteRange"/>
    /// always set this).
    /// </summary>
    [Id(6)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the committed entry expires, or <c>0</c>
    /// when it does not expire. Preserved end-to-end for
    /// <see cref="ReplogOp.Set"/>; always <c>0</c> for deletes.
    /// </summary>
    [Id(7)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored this mutation. Stamped at
    /// commit time from either the mutation's pre-existing
    /// <see cref="LatticeMutation.OriginClusterId"/> (for replays of a
    /// remote write) or the validated local
    /// <see cref="LatticeReplicationOptions.ClusterId"/> for local-origin
    /// writes. Receivers use this to attribute origin and break replication
    /// cycles. Will be non-empty for every entry produced by the standard
    /// commit-time observer because the options validator rejects an
    /// empty <c>ClusterId</c> at first-resolve time; <c>null</c> only on
    /// hand-constructed entries used in tests.
    /// </summary>
    [Id(8)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Declared <see cref="ReplicationMode"/> the receiver should use to
    /// apply this entry. Stamped at commit time from the producer-side
    /// <see cref="IReplicationModeResolver"/>; defaults to
    /// <see cref="ReplicationMode.LwwRegister"/> for hand-constructed
    /// entries and for entries decoded from older wire formats that
    /// pre-date this field.
    /// </summary>
    [Id(9)] public ReplicationMode Mode { get; init; }

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
    /// <see cref="VectorClockCodec"/>
    /// </summary>
    [Id(10)] public Primitives.VersionVector? VectorClock { get; init; }

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
    [Id(11)] public Primitives.VersionVector? DependencySummary { get; init; }

    /// <summary>
    /// Stable identifier of the typed pre-merge delta authored at the
    /// originating call site, mirrored verbatim from the producing
    /// <see cref="LatticeMutation.DeltaKind"/>. Populated by the typed
    /// CRDT accessors (OR-Set, PN-Counter, version-vector tick / merge,
    /// etc.) and by callers that opt in via
    /// <see cref="LatticeDeltaContext"/>; <see langword="null"/> for
    /// plain <c>Set</c> / <c>Delete</c> writes that did not author a
    /// delta. Strictly additive on the wire: legacy peers and entries
    /// authored before this slot existed decode as <see langword="null"/>,
    /// which receivers treat as "no typed delta available, fall back to
    /// the post-merge state". Receivers that recognise the kind dispatch
    /// to the matching delta decoder; unknown kinds are forwarded as
    /// opaque bytes for forward compatibility.
    /// </summary>
    [Id(12)] public string? DeltaKind { get; init; }

    /// <summary>
    /// Opaque payload carrying the typed pre-merge delta identified by
    /// <see cref="DeltaKind"/>. Mirrored verbatim from the producing
    /// <see cref="LatticeMutation.DeltaPayload"/>. The payload is
    /// authored once at the originating call site so every replica that
    /// applies it converges by replaying the author's intent rather than
    /// the post-merge state. <see langword="null"/> when no delta was
    /// authored. Strictly additive on the wire.
    /// </summary>
    [Id(13)] public byte[]? DeltaPayload { get; init; }

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
}

