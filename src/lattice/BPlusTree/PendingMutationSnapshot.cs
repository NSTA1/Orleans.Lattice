using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A per-<c>(transactionId, key)</c> snapshot of a single in-flight
/// prepared mutation buffered in a leaf's pending-tx map (the saga reader-isolation
/// primitive). Used by the retroactive shadow-forward
/// sweep: at the start of a shard split's
/// <c>BeginShadowWrite</c> phase the coordinator asks every source
/// leaf to enumerate its pending mutations whose key hashes into a
/// migrating virtual slot, then replays each snapshot through the
/// destination shard's standard write path under
/// <see cref="LatticeTransactionContext"/> +
/// <see cref="LatticePreparedContext"/> +
/// <see cref="LatticeOriginContext"/> +
/// <see cref="LatticeVectorClockContext"/> +
/// <see cref="LatticeHlcOverrideContext"/> scopes so the destination
/// leaf buckets the value into its own pending-tx map under the same
/// <c>(txid, key)</c> identity.
/// <para>
/// The fields are stored flat (rather than as a nested
/// <c>LwwValue&lt;byte[]&gt;</c> property) to sidestep the Orleans
/// type-alias encoder's codec-generation race when a DTO used in a
/// grain-interface signature embeds <c>LwwValue&lt;byte[]&gt;</c> as a
/// field - see <see cref="LwwEntry"/> for the same constraint and
/// rationale. Not part of the end-user API surface.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.PendingMutationSnapshot)]
[Immutable]
internal readonly record struct PendingMutationSnapshot
{
    /// <summary>The saga transaction id that authored the prepared mutation.</summary>
    [Id(0)] public Guid TransactionId { get; init; }

    /// <summary>The key the prepared mutation targets.</summary>
    [Id(1)] public string Key { get; init; }

    /// <summary>The prepared value; <c>null</c> for prepared tombstones.</summary>
    [Id(2)] public byte[]? Value { get; init; }

    /// <summary>The hybrid-logical-clock timestamp stamped on the prepared mutation at author time.</summary>
    [Id(3)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary><c>true</c> when the prepared mutation is a delete; <c>false</c> for a Set.</summary>
    [Id(4)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the prepared mutation's entry expires,
    /// or <c>0</c> when the entry never expires.
    /// </summary>
    [Id(5)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored the prepared mutation,
    /// or <c>null</c> for a local write. Round-trips verbatim through
    /// <see cref="LwwValue{T}.OriginClusterId"/> across the retroactive
    /// replay so the destination leaf's pending-tx entry carries the
    /// same origin as the source leaf's.
    /// </summary>
    [Id(6)] public string? OriginClusterId { get; init; }

    /// <summary>
    /// Sparse <c>{originClusterId → HybridLogicalClock}</c> frontier
    /// captured at prepare time, or <c>null</c> when the writer did
    /// not supply one. Round-trips verbatim through
    /// <see cref="LwwValue{T}.VectorClock"/> across the retroactive
    /// replay so the destination leaf's pending-tx entry carries the
    /// same VC as the source leaf's.
    /// </summary>
    [Id(7)] public VersionVector? VectorClock { get; init; }

    /// <summary>
    /// Earliest WAL offset recorded for the prepared mutation on the
    /// source leaf, or <c>0</c> when no offset was stamped (foreground
    /// commits author the WAL and do not stamp the per-(txid) offset
    /// map). Surfaced for diagnostics only - the retroactive replay
    /// does not feed this value back into the destination's pending-tx
    /// offset map because the destination's offsets are authored by
    /// its own WAL appends.
    /// </summary>
    [Id(8)] public long WalOffset { get; init; }

    /// <summary>
    /// The typed CRDT delta the prepared mutation carried, or <c>null</c>
    /// for a plain last-writer-wins prepared write. Round-trips through
    /// the retroactive shadow-forward replay so a resharded prepared
    /// CRDT entry still folds its per-replica delta into the destination
    /// leaf's current visible state on the saga's terminal commit
    /// (the per-replica union) instead of installing the prepared LWW
    /// value verbatim. Legacy snapshots authored before this field
    /// existed decode to <c>null</c> = the byte-for-byte unchanged LWW
    /// replay.
    /// </summary>
    [Id(9)] public byte[]? Delta { get; init; }

    /// <summary>
    /// The merge mode of the prepared mutation's tree.
    /// <see cref="LatticeMergeMode.LwwRegister"/> (the default, and the
    /// decode value for legacy snapshots) keeps the entry on the
    /// unchanged LWW replay; any CRDT mode pairs with <see cref="Delta"/>
    /// to route the destination's terminal commit through the typed-delta
    /// fold.
    /// </summary>
    [Id(10)] public LatticeMergeMode Mode { get; init; }
}
