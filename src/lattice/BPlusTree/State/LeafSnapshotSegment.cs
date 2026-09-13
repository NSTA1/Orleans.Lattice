namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// One bounded slice of a segmented leaf snapshot: a standalone
/// <see cref="LeafSnapshotCodec"/> frame over a contiguous run of the
/// snapshot's rows, persisted in its own grain-state row.
/// <para>
/// The type exists because of where the failure in issue #2844 actually
/// occurs. A snapshot's row payload is persisted as a single BLOB column, and
/// the Orleans storage provider materialises a column as one contiguous
/// <see cref="byte"/> array before any lattice code runs - the observed
/// <see cref="OutOfMemoryException"/> is raised inside the provider's own
/// value reader, not in the decode that follows it. No restructuring of the
/// object graph inside that column can change the size of that allocation,
/// because the allocation is sized by the column and made before the graph is
/// seen. Bounding it therefore requires the column itself to be smaller, and a
/// grain-state row is addressed by grain id, so the payload has to be spread
/// across several grains. That is what a segment is.
/// </para>
/// <para>
/// A segment is a <b>whole</b> frame, not a byte range of a larger one. The
/// codec's <c>Encode</c> emits a self-describing frame over whatever rows it
/// is given, so a row-aligned run encodes to a frame that validates,
/// enumerates, and reports its own row count with no reference to its
/// siblings. That is what lets hydration decode one segment, fold its rows
/// into the entry cache, and release the frame before reading the next: the
/// peak contiguous allocation becomes the segment window rather than the whole
/// snapshot. Splitting by byte range instead would require every part to be
/// resident simultaneously to reassemble a parseable frame, which would move
/// the large allocation out of the provider and into lattice code while
/// leaving the peak exactly where it was.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafSnapshotSegment)]
internal sealed class LeafSnapshotSegment : ILatticeBinaryPersistedState
{
    /// <summary>
    /// The segment's encoded frame, or <see langword="null"/> when no segment
    /// has been persisted at this index (the "absent" reading, which is also
    /// the post-clear shape).
    /// <para>
    /// Marked <c>[Immutable]</c> so a same-silo grain call shares the array
    /// rather than deep-copying it. The copy is precisely the second
    /// contiguous allocation this type exists to avoid, so the annotation is
    /// load-bearing rather than an optimisation: without it a segment read
    /// would peak at twice the window.
    /// </para>
    /// </summary>
    [Id(0)][Immutable] public byte[]? Frame { get; set; }

    /// <summary>
    /// Number of rows encoded in <see cref="Frame"/>. Persisted alongside the
    /// frame so a reader can cross-check the decoded row count against what
    /// the writer intended, which is what turns a silently truncated segment
    /// into a detected one.
    /// </summary>
    [Id(1)] public int RowCount { get; set; }
}
