using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A single key-value record produced by an
/// <see cref="ISnapshotProvider"/> export. Each entry carries the
/// per-key value and the
/// <see cref="HybridLogicalClock"/> stamped on it at write time so the
/// receiver can pin the value at the same logical timestamp on apply,
/// preserving the snapshot's as-of cut on every replica.
/// <para>
/// The first three slots (<c>[Id(0..2)]</c>) carry the committed
/// projection: a live, non-tombstoned, non-expired value at its
/// commit-time HLC. The trailing slots (<c>[Id(3..9)]</c>) are an
/// additive widening that ships any saga the producer's tx registry
/// recorded as <see cref="Orleans.Lattice.BPlusTree.TxStatus.InFlight"/>
/// at the snapshot's linearization point: such prepared per-key
/// mutations are emitted as <see cref="SnapshotEntry"/> rows with
/// <see cref="IsPrepared"/> set, alongside any already-committed
/// projection rows. The receiver routes prepared entries through
/// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedSetAsync"/>
/// / <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>
/// into the per-tx pending bucket; the matching terminal record
/// arrives subsequently via the post-snapshot incremental WAL stream
/// and flips visibility atomically per saga via
/// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyTxTerminalAsync"/>,
/// exactly as in the steady-state pipeline.
/// </para>
/// <para>
/// Sagas already decided at snapshot time
/// (<see cref="Orleans.Lattice.BPlusTree.TxStatus.Committed"/> or
/// <see cref="Orleans.Lattice.BPlusTree.TxStatus.Aborted"/>) are
/// folded into the committed-projection stream by the exporter:
/// Committed outcomes inline the post-saga value at the prepare's
/// HLC, Aborted outcomes drop the prepared mutation entirely. No
/// separate terminal-decision segment is required because the
/// receiver-side per-tx pending bucket has nothing buffered for those
/// txs at apply time.
/// </para>
/// <para>
/// Old senders that pre-date this widening leave the trailing slots
/// at their default zero values; the receiver treats
/// <see cref="IsPrepared"/> as the discriminator and dispatches every
/// such entry through the legacy committed-projection path. The
/// per-entry <c>OriginClusterId</c> / <c>VectorClock</c> slots remain
/// omitted - the receiver stamps every committed entry with the
/// bootstrap sender's id as before, and prepared entries inherit the
/// same convention; full per-entry origin/VC preservation across
/// bootstrap is a separate concern tracked elsewhere.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.SnapshotEntry)]
[Immutable]
public readonly record struct SnapshotEntry
{
    /// <summary>The exported key.</summary>
    [Id(0)] public string Key { get; init; }

    /// <summary>
    /// The exported value bytes. For a committed projection row this
    /// is the live value at <see cref="Timestamp"/>. For a prepared
    /// mutation (<see cref="IsPrepared"/> = <see langword="true"/>)
    /// this is the prepared post-saga value when
    /// <see cref="IsTombstone"/> is <see langword="false"/>, or
    /// (semantically) ignored when <see cref="IsTombstone"/> is
    /// <see langword="true"/>.
    /// </summary>
    [Id(1)] public byte[] Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the value at
    /// commit time. The receiver applies the value at exactly this
    /// timestamp so the snapshot's as-of cut is preserved across
    /// replicas (including for transitive replication paths). For a
    /// prepared mutation, this is the HLC the producer stamped on the
    /// prepare-phase write; the receiver re-stamps the per-tx pending
    /// bucket bit-identically so the eventual terminal flip lands the
    /// value at the source's exact HLC.
    /// </summary>
    [Id(2)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// <see langword="true"/> when this entry represents a prepared
    /// (but not yet terminally decided) per-key saga mutation captured
    /// in the producer's per-leaf pending-tx bucket at snapshot start;
    /// <see langword="false"/> for a committed projection row.
    /// Prepared entries are dispatched on the receiver via
    /// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedSetAsync"/>
    /// or <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>
    /// so they land in the receiver's per-tx pending bucket and remain
    /// invisible to readers until the post-snapshot incremental WAL
    /// delivers the matching terminal record. Defaults to
    /// <see langword="false"/> on the wire so a legacy sender's
    /// missing slot still decodes as a committed projection row.
    /// </summary>
    [Id(3)] public bool IsPrepared { get; init; }

    /// <summary>
    /// <see langword="true"/> when the prepared mutation is a delete
    /// rather than a set; only meaningful when
    /// <see cref="IsPrepared"/> is <see langword="true"/>. Committed
    /// projection rows always carry the live value and never the
    /// tombstone slot; the snapshot enumerator skips tombstoned keys
    /// on the committed-projection path.
    /// </summary>
    [Id(4)] public bool IsTombstone { get; init; }

    /// <summary>
    /// The saga transaction id that authored the prepared mutation;
    /// <see cref="Guid.Empty"/> on a committed projection row. Used by
    /// the receiver to route the prepared mutation into its per-tx
    /// pending bucket; the receiver correlates this id with the
    /// matching terminal record delivered later through the
    /// incremental WAL stream.
    /// </summary>
    [Id(5)] public Guid TransactionId { get; init; }

    /// <summary>
    /// Reserved for future use. Snapshot-emitted prepared entries
    /// today do not need to surface the producer-side shard index
    /// because the receiver's terminal-arrival tally is keyed off the
    /// terminal record's <c>ShardIndex</c>, not the prepared
    /// record's. Always <c>0</c> on the wire; ignored by the receiver.
    /// </summary>
    [Id(6)] public int SourceShardIndex { get; init; }

    /// <summary>
    /// The producer-stamped atomic-batch size for the saga that
    /// authored this prepared mutation, or <c>0</c> when the saga did
    /// not stamp a batch envelope. Mirrors
    /// <c>WalRecord.AtomicBatchSize</c>; round-trips through
    /// <c>LatticeAtomicBatchContext</c> on the receiver so the
    /// pending-tx bucket carries the same envelope as on the source.
    /// </summary>
    [Id(7)] public int AtomicBatchSize { get; init; }

    /// <summary>
    /// The producer-stamped atomic-batch index (zero-based position of
    /// this mutation within the saga's per-batch fan-out). Mirrors
    /// <c>WalRecord.AtomicBatchIndex</c>; meaningful only when
    /// <see cref="AtomicBatchSize"/> is positive.
    /// </summary>
    [Id(8)] public int AtomicBatchIndex { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the prepared mutation's entry
    /// expires, or <c>0</c> when the entry never expires. Mirrors
    /// <c>LwwValue.ExpiresAtTicks</c>; preserved verbatim across the
    /// snapshot boundary so the receiver's per-tx pending bucket
    /// stamps the same TTL the source recorded.
    /// </summary>
    [Id(9)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// The typed CRDT delta the prepared mutation carried, or
    /// <see langword="null"/> for a plain last-writer-wins prepared
    /// write. Mirrors <c>WalRecord.Delta</c>. When present (and
    /// <see cref="Mode"/> is a CRDT mode) the receiver folds this delta
    /// into its current visible state on the saga's terminal commit
    /// instead of installing <see cref="Value"/> verbatim, so a
    /// bootstrap-restored prepared CRDT entry converges by the
    /// per-replica typed-delta union exactly as a steady-state prepared
    /// entry does. Legacy senders that pre-date this widening leave the
    /// slot at its default <see langword="null"/>, which decodes to the
    /// byte-for-byte unchanged LWW prepared path.
    /// </summary>
    [Id(10)] public byte[]? Delta { get; init; }

    /// <summary>
    /// The merge mode of the prepared mutation's tree. Mirrors
    /// <c>WalRecord.Mode</c>.
    /// <see cref="Orleans.Lattice.LatticeMergeMode.LwwRegister"/> (the
    /// default, and the decode value for legacy senders) keeps the entry
    /// on the unchanged LWW path; any CRDT mode pairs with
    /// <see cref="Delta"/> to route the receiver's terminal commit
    /// through the typed-delta fold.
    /// </summary>
    [Id(11)] public Orleans.Lattice.LatticeMergeMode Mode { get; init; }
}
