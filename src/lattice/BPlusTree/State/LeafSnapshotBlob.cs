namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persisted state row for <see cref="Grains.ILeafSnapshotStorageGrain"/>.
/// Holds a point-in-time copy of a single leaf grain's entry cache
/// together with the WAL offset the snapshot is consistent with.
/// <para>
/// The blob is the safety net for the leaf-state collapse: a
/// leaf whose persisted <c>ProjectionCheckpointOffset</c> would
/// otherwise fall off WAL retention captures its current projection
/// into this row, and the reactivation path prefers the snapshot
/// over a from-scratch WAL replay whenever the snapshot offset is
/// strictly newer than the persisted checkpoint.
/// </para>
/// <para>
/// At most one snapshot exists per leaf; a successful capture
/// overwrites the previous blob via a single Orleans
/// <c>WriteStateAsync</c> call on the storage grain. No historical
/// retention is intended; the WAL remains the long-term audit trail.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotBlob)]
internal sealed class LeafSnapshotBlob
{
    /// <summary>
    /// WAL offset (under "applied through offset N inclusive"
    /// semantics) at which the projection in <see cref="Rows"/> is
    /// consistent. A reactivation that prefers this snapshot must
    /// resume WAL replay strictly after this offset
    /// (<c>SnapshotOffset + 1</c>). Defaults to the "nothing
    /// captured" sentinel <c>-1</c>.
    /// </summary>
    [Id(0)] public long SnapshotOffset { get; set; } = -1;

    /// <summary>
    /// Canonical byte-row contents of the leaf's entry cache at
    /// <see cref="SnapshotOffset"/>. May be empty when the leaf had
    /// no live keys at capture time (a freshly created leaf whose
    /// projection is empty still has a meaningful checkpoint
    /// offset). Never <see langword="null"/>.
    /// </summary>
    [Id(1)] public IReadOnlyList<LeafSnapshotRow> Rows { get; set; } = Array.Empty<LeafSnapshotRow>();

    /// <summary>
    /// Wall-clock <see cref="DateTime.Ticks"/> at the moment the
    /// snapshot was captured. Diagnostic only - the reactivation
    /// preference logic compares <see cref="SnapshotOffset"/>
    /// against the leaf's persisted checkpoint, not this stamp.
    /// </summary>
    [Id(2)] public long CapturedAtTicks { get; set; }

    /// <summary>
    /// Precomputed byte-accurate footprint of <see cref="Rows"/> using the
    /// same UTF-8-key + stored-value-length formula the leaf surface uses
    /// for <see cref="LeafStats.StateBytes"/>. Populated once at capture
    /// time by <see cref="Grains.ILeafSnapshotStorageGrain.SaveAsync"/> so
    /// <see cref="Grains.ILeafSnapshotStorageGrain.GetSnapshotByteSizeAsync"/>
    /// is a constant-time field read. Wire-compatible: legacy persisted
    /// blobs without this field decode to <c>0</c>, and the storage grain
    /// lazily back-fills the slot on the first byte-size read so the figure
    /// converges to the correct value without forcing a re-capture.
    /// </summary>
    [Id(3)] public long SnapshotBytes { get; set; }

    /// <summary>
    /// Per-partition WAL offset the projection in <see cref="Rows"/> is
    /// consistent through, under the same "applied through offset N
    /// inclusive" semantics as the scalar <see cref="SnapshotOffset"/>.
    /// Slot <c>p</c> holds the checkpoint offset partition <c>p</c> was
    /// captured at; slot <c>0</c> mirrors <see cref="SnapshotOffset"/>.
    /// A partition that had never checkpointed at capture time holds the
    /// <c>-1</c> "nothing applied" sentinel.
    /// <para>
    /// This exists because under the default <c>WalPartitions = 8</c> the
    /// scalar <see cref="SnapshotOffset"/> only describes partition 0; the
    /// coverage-gated WAL-GC trim floor (see
    /// <c>BPlusLeafGrain.ResolveDurablePinForPartition</c>) needs the
    /// per-partition covered offset to authorise trimming each partition's
    /// checkpointed prefix, and the rehydrate path needs it to advance each
    /// partition's persisted checkpoint independently. Wire-compatible:
    /// legacy blobs captured before this field decode to <see langword="null"/>,
    /// which the readers treat as "only partition 0 is covered, at
    /// <see cref="SnapshotOffset"/>".
    /// </para>
    /// </summary>
    [Id(4)] public long[]? SnapshotOffsetsByPartition { get; set; }
}
