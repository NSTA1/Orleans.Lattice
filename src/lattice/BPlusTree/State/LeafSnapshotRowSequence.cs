namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Encoding-agnostic view over the rows of a <see cref="LeafSnapshotBlob"/>.
/// Yields the same <see cref="LeafSnapshotRow"/> sequence whether the blob
/// carries a legacy row list or a <see cref="LeafSnapshotCodec"/> binary
/// frame, so consumers never branch on the encoding.
/// <para>
/// Both the sequence and its enumerator are structs and
/// <c>GetEnumerator</c> is resolved by pattern rather than through
/// <see cref="IEnumerable{T}"/>, so a <c>foreach</c> over either encoding
/// boxes nothing and allocates nothing beyond the rows it decodes.
/// </para>
/// <para>
/// A binary-backed sequence assumes its frame has already passed
/// <see cref="LeafSnapshotCodec.Validate"/>: enumeration of a frame that has
/// not been validated throws <see cref="InvalidDataException"/> on the row
/// that fails to parse. That is deliberate. Silently stopping early would
/// hand the caller a short row set that looks like a complete snapshot, and a
/// snapshot that under-reports its contents while still claiming coverage is
/// exactly the shape that loses data once the WAL GC has trimmed the prefix it
/// claims to cover.
/// </para>
/// </summary>
internal readonly struct LeafSnapshotRowSequence
{
    private readonly IReadOnlyList<LeafSnapshotRow>? _legacyRows;
    private readonly byte[]? _frame;
    private readonly int _rowRegionEnd;
    private readonly int _count;

    private LeafSnapshotRowSequence(IReadOnlyList<LeafSnapshotRow>? legacyRows, byte[]? frame, int rowRegionEnd, int count)
    {
        _legacyRows = legacyRows;
        _frame = frame;
        _rowRegionEnd = rowRegionEnd;
        _count = count;
    }

    /// <summary>An empty sequence, used for a blob with no rows in either encoding.</summary>
    internal static LeafSnapshotRowSequence Empty => new(null, null, 0, 0);

    /// <summary>
    /// Creates a sequence over a legacy row list. A <see langword="null"/>
    /// list is treated as empty, matching the "never null, but the setter is
    /// public" defensiveness the rehydrate path has always applied.
    /// </summary>
    /// <param name="rows">Legacy row list, or <see langword="null"/>.</param>
    internal static LeafSnapshotRowSequence FromLegacyRows(IReadOnlyList<LeafSnapshotRow>? rows)
        => rows is null || rows.Count == 0 ? Empty : new(rows, null, 0, rows.Count);

    /// <summary>
    /// Creates a sequence over a binary frame. Returns an empty sequence when
    /// the frame header is unreadable; callers that must distinguish "empty"
    /// from "corrupt" run <see cref="LeafSnapshotCodec.Validate"/> first.
    /// </summary>
    /// <param name="frame">Encoded frame bytes.</param>
    internal static LeafSnapshotRowSequence FromFrame(byte[] frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        return LeafSnapshotCodec.TryReadHeader(frame, out var rowCount, out var indexOffset)
            ? new LeafSnapshotRowSequence(null, frame, indexOffset, rowCount)
            : Empty;
    }

    /// <summary>Number of rows the sequence yields.</summary>
    internal int Count => _count;

    /// <summary>Returns a struct enumerator over the rows, in stored order.</summary>
    public Enumerator GetEnumerator() => new(_legacyRows, _frame, _rowRegionEnd, _count);

    /// <summary>
    /// Struct enumerator over a <see cref="LeafSnapshotRowSequence"/>. Decodes
    /// each binary row on demand so no intermediate row collection is ever
    /// materialised.
    /// </summary>
    public struct Enumerator
    {
        private readonly IReadOnlyList<LeafSnapshotRow>? _legacyRows;
        private readonly byte[]? _frame;
        private readonly int _rowRegionEnd;
        private readonly int _count;
        private int _index;
        private int _position;

        internal Enumerator(IReadOnlyList<LeafSnapshotRow>? legacyRows, byte[]? frame, int rowRegionEnd, int count)
        {
            _legacyRows = legacyRows;
            _frame = frame;
            _rowRegionEnd = rowRegionEnd;
            _count = count;
            _index = 0;
            _position = LeafSnapshotCodec.HeaderLength;
            Current = default;
        }

        /// <summary>The row most recently yielded by <see cref="MoveNext"/>.</summary>
        public LeafSnapshotRow Current { get; private set; }

        /// <summary>
        /// Advances to the next row, returning <see langword="false"/> once the
        /// sequence is exhausted.
        /// </summary>
        /// <exception cref="InvalidDataException">A binary row failed to parse, meaning the frame was enumerated without being validated.</exception>
        public bool MoveNext()
        {
            if (_index >= _count)
            {
                return false;
            }

            if (_frame is null)
            {
                Current = _legacyRows![_index];
                _index++;
                return true;
            }

            if (!LeafSnapshotCodec.TryReadRow(_frame, _rowRegionEnd, ref _position, out var row))
            {
                throw new InvalidDataException(
                    "Leaf snapshot frame is malformed; it must be validated with LeafSnapshotCodec.Validate before enumeration.");
            }

            Current = row;
            _index++;
            return true;
        }
    }
}
