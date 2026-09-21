using System.Text;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The arithmetic that decides how many rows fit in one leaf-snapshot segment.
/// <para>
/// Hoisted out of <see cref="LeafSnapshotStorageGrain"/> because two sides now
/// depend on it and they must agree exactly. The capture path plans runs before
/// encoding anything, so that it never materialises a whole-leaf frame; the
/// persist path plans runs from an already-encoded frame, for blobs that arrive
/// inline. If the two used different budgets a capture could stage frames the
/// hydration admission gate refuses to read back, which is a snapshot reporting
/// coverage nothing can reproduce - the exact shape the coverage-gated WAL GC
/// must never see.
/// </para>
/// </summary>
internal static class LeafSnapshotSegmentPlan
{
    /// <summary>
    /// Per-row allowance covering the frame's index entry and the row's own
    /// length and discriminator fields. Deliberately generous: over-reserving
    /// costs one extra segment, while under-reserving lets a run encode above
    /// the window, which is the failure the window exists to prevent.
    /// </summary>
    internal const long PerRowOverheadBytes = 64;

    /// <summary>
    /// Effective segment window for <paramref name="configured"/>, clamped to
    /// <see cref="LatticeOptions.MinimumLeafSnapshotSegmentBytes"/>.
    /// </summary>
    internal static long Window(long configured)
        => Math.Max(LatticeOptions.MinimumLeafSnapshotSegmentBytes, configured);

    /// <summary>
    /// Row-cost budget for one run inside <paramref name="window"/>, leaving
    /// headroom for the frame header and index table, which scale with the run
    /// rather than with any one row.
    /// </summary>
    internal static long Budget(long window)
        => Math.Max(window - (window / 8), LatticeOptions.MinimumLeafSnapshotSegmentBytes / 2);

    /// <summary>
    /// Encoded cost this row contributes to its run.
    /// </summary>
    internal static long RowCost(in LeafSnapshotRow row)
    {
        long keyBytes = Encoding.UTF8.GetByteCount(row.Key);
        long valueBytes = row.Value.IsTombstone ? 0 : row.Value.Value?.Length ?? 0;
        return keyBytes + valueBytes + PerRowOverheadBytes;
    }

    /// <summary>
    /// Whether a run holding <paramref name="currentCost"/> across
    /// <paramref name="currentCount"/> rows must be closed before admitting a
    /// row costing <paramref name="rowCost"/>.
    /// <para>
    /// A run always admits its first row, however large: splitting one row
    /// across segments would leave a partial row that is not independently
    /// decodable, which is the property the whole design rests on. The row wins
    /// and the window yields - the one honest residual in the bound.
    /// </para>
    /// </summary>
    internal static bool MustCloseRun(int currentCount, long currentCost, long rowCost, long budget)
        => currentCount > 0 && currentCost + rowCost > budget;
}
