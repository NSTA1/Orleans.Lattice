namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Holds one bounded slice of a segmented leaf snapshot. One activation per
/// segment, and therefore one grain-state row per segment - which is the whole
/// point of the type, since a row is the unit the storage provider reads
/// contiguously (see <see cref="Orleans.Lattice.BPlusTree.State.LeafSnapshotSegment"/>).
/// <para>
/// Grain key format: <c>{leafKey}/{segmentIndex}</c>. The leaf's own grain
/// key is the prefix so a segment is addressable from the leaf without a
/// lookup, and the index is the suffix so segments of one leaf sort together
/// in any store that orders by key.
/// </para>
/// <para>
/// The grain is deliberately dumb: it stores and returns a frame and knows
/// nothing about coverage, merging, or snapshot semantics. Everything that
/// decides whether a snapshot is usable stays in
/// <see cref="ILeafSnapshotStorageGrain"/>, which owns the manifest row that
/// commits a segmented snapshot. Splitting that judgement across the segments
/// would make a torn write indistinguishable from a complete one.
/// </para>
/// </summary>
[Alias(TypeAliases.ILeafSnapshotSegmentGrain)]
internal interface ILeafSnapshotSegmentGrain : IGrainWithStringKey
{
    /// <summary>
    /// Persists <paramref name="frame"/> as this segment's payload,
    /// overwriting any previous frame. Returns only once the state provider
    /// has durably accepted the write, because the manifest that commits the
    /// snapshot is written afterwards and must not be able to reference a
    /// segment that has not landed.
    /// </summary>
    /// <param name="frame">Encoded segment frame. Must not be <see langword="null"/> or empty.</param>
    /// <param name="rowCount">Number of rows encoded in <paramref name="frame"/>.</param>
    /// <param name="cancellationToken">Cancellation token observed before the persist call.</param>
    Task SaveAsync(byte[] frame, int rowCount, CancellationToken cancellationToken);

    /// <summary>
    /// Returns this segment's frame, or <see langword="null"/> when no segment
    /// has been persisted at this index or the persisted frame does not
    /// validate. A frame that fails validation is reported as absent rather
    /// than returned partially: the caller's contract is that a segmented
    /// snapshot reads back whole or not at all, and a half-decoded segment
    /// would present as a snapshot with fewer rows - which is the one outcome
    /// the coverage-gated WAL GC must never see.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the load.</param>
    Task<byte[]?> LoadFrameAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Drops this segment's frame. Idempotent: clearing a segment that holds
    /// nothing is a no-op and performs no I/O.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token observed before the clear.</param>
    Task ClearAsync(CancellationToken cancellationToken);
}
