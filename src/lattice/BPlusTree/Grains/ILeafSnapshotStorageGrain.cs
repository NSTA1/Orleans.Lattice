using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-leaf snapshot storage grain. Holds at most one
/// <see cref="LeafSnapshotBlob"/> per leaf, captured by the
/// maintenance grain when the owning leaf's persisted
/// <c>ProjectionCheckpointOffset</c> approaches the WAL retention
/// boundary (the snapshot-on-fall-off trigger of the leaf-snapshot
/// safety net).
/// <para>
/// Grain key format: the <see cref="System.Guid"/> portion of the
/// owning <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain"/>'s
/// <see cref="GrainId"/>. One activation per leaf; the grain is
/// completely passive between capture and reactivation reads.
/// </para>
/// <para>
/// Decoupled from the leaf state row by design: the leaf row was
/// collapsed to a small fixed-shape envelope (topology, checkpoint,
/// and digest only), and a snapshot blob can be multi-MB on a leaf
/// with many live keys.
/// Persisting the blob to a separate grain row keeps the foreground
/// leaf state row small and lets the Orleans storage provider's
/// per-row limit apply only to the snapshot, not to the hot
/// foreground leaf state.
/// </para>
/// </summary>
[Alias(TypeAliases.ILeafSnapshotStorageGrain)]
internal interface ILeafSnapshotStorageGrain : IGrainWithGuidKey
{
    /// <summary>
    /// Offers <paramref name="blob"/> as the current snapshot for this leaf and
    /// reports whether the snapshot this grain holds once the call returns
    /// covers everything <paramref name="blob"/> covers. The call returns only
    /// after the underlying state provider has durably accepted any write it
    /// made.
    /// <para>
    /// The store keeps coverage monotone, so it does not always keep what it was
    /// offered: a capture whose coverage regresses is merged with the stored
    /// snapshot (element-wise maximum, which still covers the offer), or
    /// declined outright when the merge cannot be proved safe (a segmented
    /// stored snapshot, or a stored key the capture no longer carries), and an
    /// offered blob whose row payload does not read back is refused. A caller
    /// must record the offered coverage as durable only on
    /// <see cref="LeafSnapshotSaveOutcome.Kept"/>: recording an offer the store
    /// declined licenses a WAL trim past what any durable snapshot can
    /// reproduce (issue #3421).
    /// </para>
    /// </summary>
    /// <param name="blob">Snapshot payload. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the persist call.</param>
    /// <returns>
    /// <see cref="LeafSnapshotSaveOutcome.Kept"/> when the held snapshot covers
    /// the offer; <see cref="LeafSnapshotSaveOutcome.Declined"/> otherwise.
    /// </returns>
    Task<LeafSnapshotSaveOutcome> SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the most recently persisted snapshot for this leaf,
    /// or <see langword="null"/> when no snapshot has ever been
    /// captured (or after a successful <see cref="ClearAsync"/>).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the load.</param>
    Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Returns the encoded frame of segment <paramref name="index"/> of a
    /// segmented snapshot, or <see langword="null"/> when this leaf's snapshot
    /// is not segmented, the index is out of range, or the segment does not
    /// read back.
    /// <para>
    /// This is the read that makes the hydration allocation bounded. A
    /// segmented snapshot's payload is spread across one grain-state row per
    /// segment, so each call materialises at most one segment window rather
    /// than the whole snapshot, and the caller folds each segment's rows into
    /// its entry cache and releases the frame before requesting the next. The
    /// caller must treat a <see langword="null"/> for an in-range index as "the
    /// snapshot is not usable" and fall back to WAL replay, never as an empty
    /// segment - a segmented snapshot reads back whole or not at all.
    /// </para>
    /// </summary>
    /// <param name="index">Zero-based segment index, below <see cref="LeafSnapshotBlob.SegmentCount"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the load.</param>
    Task<byte[]?> LoadSegmentFrameAsync(int index, CancellationToken cancellationToken);

    /// <summary>
    /// Stages one encoded segment frame of a capture in progress, returning the
    /// zero-based index it was staged at. Frames must be staged in ascending
    /// key order, which is the order a capture produces them in.
    /// <para>
    /// This is the write that makes the <b>capture</b> allocation bounded, and
    /// it is the mirror of <see cref="LoadSegmentFrameAsync"/> on the read side.
    /// A capture previously encoded its whole row set into one contiguous frame
    /// and handed that to <see cref="SaveAsync"/>, so the largest leaf still
    /// demanded a single array of the entire snapshot before any segmentation
    /// happened - segmentation bounded the row write and the hydration read
    /// while leaving the capture peak exactly where it was. Staging frame by
    /// frame bounds the caller's allocation and this call's payload to one
    /// segment window.
    /// </para>
    /// <para>
    /// Staged frames are written into a generation no live manifest references,
    /// so the current snapshot stays authoritative and intact until
    /// <see cref="CommitStagedSnapshotAsync"/> writes the new manifest. A
    /// capture abandoned part-way leaves unreferenced frames behind; they are
    /// made inert by the next capture's
    /// <see cref="BeginStagedSnapshotAsync"/>, which retires them and restarts
    /// the run at index zero.
    /// </para>
    /// <para>
    /// A caller must open every staged run with
    /// <see cref="BeginStagedSnapshotAsync"/>. Staging without it appends to
    /// whatever run is already open, which is how a partially staged capture
    /// leaks into the next one's manifest.
    /// </para>
    /// </summary>
    /// <param name="frame">Encoded segment frame. Must be non-empty.</param>
    /// <param name="rowCount">Number of rows encoded in <paramref name="frame"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the write.</param>
    Task<int> StageSnapshotSegmentAsync(byte[] frame, int rowCount, CancellationToken cancellationToken);

    /// <summary>
    /// Opens a staged capture, discarding any run a previous capture abandoned
    /// part-way so this one starts at segment index zero.
    /// <para>
    /// This exists because the staging cursor is activation-scoped, and an
    /// abandoned run is only self-clearing when the abandonment takes the
    /// activation with it. A capture that merely <b>fails</b> - the storage
    /// fault or timeout that
    /// <c>orleans.lattice.leaf.snapshot.captures{outcome="failed"}</c> counts -
    /// leaves the grain activated with its cursor mid-run. Without this call
    /// the next capture resumes at that cursor, so its commit publishes a
    /// manifest whose leading segments belong to the abandoned capture. Those
    /// frames are stale rows, and because hydration folds segments in index
    /// order with later-wins-per-key, a key deleted between the two captures
    /// has nothing to overwrite it and comes back.
    /// </para>
    /// <para>
    /// Idempotent, and safe to call when no run is open. The retirement of an
    /// abandoned run is best-effort for the same reason
    /// <see cref="CommitStagedSnapshotAsync"/>'s is: an orphaned frame wastes a
    /// row but is unreachable once the cursor has been reset, so a failure to
    /// delete it must not fail the capture that is about to start.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the reset.</param>
    Task BeginStagedSnapshotAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Commits the frames staged by <see cref="StageSnapshotSegmentAsync"/> as
    /// this leaf's current snapshot, using <paramref name="manifest"/> for
    /// coverage and sizing. The manifest write is the commit point; the
    /// previous generation's frames are retired only after it lands.
    /// <para>
    /// Applies the same monotone-coverage rule as <see cref="SaveAsync"/>: a
    /// capture that would lower coverage for any partition is declined and the
    /// stored snapshot kept, because a manifest reporting coverage its rows
    /// cannot reproduce is what lets the coverage-gated WAL GC trim the last
    /// durable copy of a prefix. A declined capture retires its own staged
    /// frames rather than leaving them to accumulate.
    /// </para>
    /// <para>
    /// Declining must stay rare on the ordinary path, because it leaves the
    /// leaf's durable-materialiser pin unadvanced, which keeps the tree's WAL
    /// trim floor down, which feeds
    /// <see cref="Orleans.Lattice.IWalSaturationSignal"/> - the signal atomic
    /// writes and the replication receiver both gate on. Over-trimming loses
    /// data and under-trimming stalls the cluster, so both directions matter.
    /// </para>
    /// </summary>
    /// <param name="manifest">Coverage and sizing for the staged snapshot. Must carry no inline rows.</param>
    /// <param name="cancellationToken">Cancellation token observed before the write.</param>
    /// <returns>
    /// <see langword="true"/> when the staged snapshot became current;
    /// <see langword="false"/> when the commit was declined and the stored
    /// snapshot kept. As with <see cref="SaveAsync"/>, a caller must record the
    /// manifest's coverage as durable only on <see langword="true"/>
    /// (issue #3421).
    /// </returns>
    Task<bool> CommitStagedSnapshotAsync(LeafSnapshotBlob manifest, CancellationToken cancellationToken);

    /// <summary>
    /// Returns the approximate persisted snapshot footprint for this
    /// leaf in bytes - the summed key-plus-value lengths of every row in
    /// the captured <see cref="LeafSnapshotBlob"/> - or <c>0</c> when no
    /// snapshot has been captured. Used by the byte-accurate storage-usage
    /// aggregator (<see cref="ILattice.GetStorageUsageAsync"/>) to report a
    /// tree's snapshot footprint without forcing a full blob load on the
    /// caller's behalf beyond the already-activated state row.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the read.</param>
    Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Drops the persisted snapshot. Idempotent: clearing a leaf
    /// that has no snapshot is a no-op. Used by the operator-driven
    /// projection rebuild seam so a forced rebuild does not silently
    /// rehydrate from a stale snapshot.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the clear.</param>
    Task ClearAsync(CancellationToken cancellationToken);
}
