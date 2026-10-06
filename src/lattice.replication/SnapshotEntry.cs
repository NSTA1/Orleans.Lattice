using System.Collections.Immutable;
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
/// Slots <c>[Id(0..2)]</c> carry the committed projection: a
/// live, non-tombstoned, non-expired value at its commit-time HLC;
/// <c>[Id(9)]</c> carries that row's absolute expiry. The other
/// trailing slots (<c>[Id(3..8)]</c> and <c>[Id(10..11)]</c>) are
/// additive widenings that ship any saga the producer's tx registry
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
    /// <see langword="true"/> when the entry is a delete rather than a set.
    /// On a prepared row it marks a prepared delete. On a committed row it
    /// marks a delete the source committed: the default exporter emits one
    /// only for a saga it resolves from the recorded verdict behind an
    /// aged-out decision (#4481), and the bootstrap drain applies it as a
    /// delete so a re-bootstrap over an existing receiver copy removes the
    /// key.
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
    /// Absolute UTC tick at which the entry expires, or <c>0</c> when the
    /// entry never expires. Mirrors <c>LwwValue.ExpiresAtTicks</c> and is
    /// carried on committed and prepared rows alike. Last-writer-wins
    /// receivers install that expiry verbatim; typed CRDT committed rows
    /// are folded through the state-based merge path, which currently
    /// writes the resulting key as durable even when this value is set.
    /// Prepared rows carry the value into the receiver's per-tx pending
    /// bucket.
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

    /// <summary>
    /// Set on a <b>decision row</b>: a row that carries no key or value and
    /// records that the snapshot settled the saga <see cref="TransactionId"/>:
    /// <see langword="true"/> for a commit, <see langword="false"/> for an
    /// abort. The
    /// bootstrap drain records the outcome in the receiver's transaction
    /// registry, so a saga record the source's write-ahead log still retains
    /// from before the cut, re-shipped by the incremental stream after the
    /// bootstrap, is settled against it instead of being staged in a pending
    /// bucket no terminal will ever drain (#4482). <see langword="null"/> on
    /// every other row. A receiver that predates this slot sees a row with no
    /// value that is neither prepared nor a tombstone, which its drain skips.
    /// </summary>
    [Id(12)] public bool? SettledDecision { get; init; }

    /// <summary>Whether this entry is a decision row (see <see cref="SettledDecision"/>).</summary>
    public bool IsDecision => SettledDecision is not null;

    /// <summary>
    /// The cross-tree atomic write the saga <see cref="TransactionId"/> belongs
    /// to, on a decision row or a prepared row of a sub-saga the source authored
    /// as part of one (issue #4683), or <see langword="null"/>. A receiver
    /// that imports a cross-tree sub-saga's decision records the tree's arrival
    /// at its cross-tree barrier for this operation, as a shipped terminal
    /// would, so the sibling trees are not left waiting for a terminal the
    /// import replaced. A receiver that predates this slot ignores it.
    /// </summary>
    [Id(13)] internal string? CrossTreeOperationId { get; init; }

    /// <summary>
    /// The trees the cross-tree write <see cref="CrossTreeOperationId"/>
    /// touched, ordinal-sorted, or empty when the row names no operation.
    /// </summary>
    [Id(14)] internal ImmutableArray<string> CrossTreeParticipants { get; init; }

    /// <summary>
    /// The decision stamps of the cross-tree write
    /// <see cref="CrossTreeOperationId"/> (issue #4684): per participating tree,
    /// that tree's export epoch read at the source after the decision was
    /// durable, or <see langword="null"/> when the row names no operation or the
    /// operation was decided before stamping. The receiver's barrier compares a
    /// participant's stamp with the export it imported the participant from.
    /// </summary>
    [Id(15)] internal ImmutableDictionary<string, long>? CrossTreeDecisionStamps { get; init; }

    /// <summary>
    /// The decision sequences of the cross-tree write
    /// <see cref="CrossTreeOperationId"/> (issue #4733), or <see langword="null"/>.
    /// </summary>
    [Id(16)] internal ImmutableDictionary<string, long>? CrossTreeDecisionSequences { get; init; }

    /// <summary>
    /// Compares two entries by value, with <see cref="Value"/> and
    /// <see cref="Delta"/> compared by content. The compiler-generated
    /// record-struct equality compares each <see cref="byte"/> array with
    /// <see cref="EqualityComparer{T}.Default"/> (reference equality), so two
    /// structurally identical entries built from independently allocated but
    /// byte-identical payloads - and, in particular, an entry and its
    /// post-serialization self - would otherwise never compare equal.
    /// </summary>
    /// <param name="other">The entry to compare against.</param>
    public bool Equals(SnapshotEntry other) =>
        string.Equals(Key, other.Key, StringComparison.Ordinal)
        && ByteArrayEquality.ContentEquals(Value, other.Value)
        && Timestamp.Equals(other.Timestamp)
        && IsPrepared == other.IsPrepared
        && IsTombstone == other.IsTombstone
        && TransactionId == other.TransactionId
        && SourceShardIndex == other.SourceShardIndex
        && AtomicBatchSize == other.AtomicBatchSize
        && AtomicBatchIndex == other.AtomicBatchIndex
        && ExpiresAtTicks == other.ExpiresAtTicks
        && ByteArrayEquality.ContentEquals(Delta, other.Delta)
        && Mode == other.Mode
        && SettledDecision == other.SettledDecision
        && string.Equals(CrossTreeOperationId, other.CrossTreeOperationId, StringComparison.Ordinal)
        && (CrossTreeParticipants.IsDefaultOrEmpty
            ? other.CrossTreeParticipants.IsDefaultOrEmpty
            : !other.CrossTreeParticipants.IsDefaultOrEmpty
                && CrossTreeParticipants.AsSpan().SequenceEqual(other.CrossTreeParticipants.AsSpan()))
        && StampsEqual(CrossTreeDecisionStamps, other.CrossTreeDecisionStamps)
        && StampsEqual(CrossTreeDecisionSequences, other.CrossTreeDecisionSequences);

    private static bool StampsEqual(ImmutableDictionary<string, long>? left, ImmutableDictionary<string, long>? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var (tree, stamp) in left)
        {
            if (!right.TryGetValue(tree, out var other) || other != stamp)
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Key, StringComparer.Ordinal);
        if (Value is { } value)
        {
            hash.AddBytes(value);
        }

        hash.Add(Timestamp);
        hash.Add(IsPrepared);
        hash.Add(IsTombstone);
        hash.Add(TransactionId);
        hash.Add(SourceShardIndex);
        hash.Add(AtomicBatchSize);
        hash.Add(AtomicBatchIndex);
        hash.Add(ExpiresAtTicks);
        if (Delta is { } delta)
        {
            hash.AddBytes(delta);
        }

        hash.Add(Mode);
        hash.Add(SettledDecision);
        hash.Add(CrossTreeOperationId, StringComparer.Ordinal);
        return hash.ToHashCode();
    }
}
