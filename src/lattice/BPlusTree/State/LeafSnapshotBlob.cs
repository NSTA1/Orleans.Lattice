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
}
