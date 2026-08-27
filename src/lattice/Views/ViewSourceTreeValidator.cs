using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Views;

/// <summary>
/// Validates that a materialised view's source tree is a directly-writable data
/// tree, not another view tree. A view derives from a source tree by tailing that
/// tree's write-ahead log; chaining a view onto another view (so its source is a
/// <c>view-*</c> tree) is unsupported - it compounds apply lag at every hop,
/// stacks source-WAL cursor pins, and cascades rebuilds - so it is rejected at the
/// point of creation rather than silently half-working.
/// </summary>
internal static class ViewSourceTreeValidator
{
    /// <summary>
    /// Throws <see cref="InvalidOperationException"/> when <paramref name="sourceTreeId"/>
    /// names a materialised-view tree, whether or not the id has been
    /// tenant-composed (see <see cref="LatticeViewTrees.IsViewTree"/> - testing
    /// the leading prefix alone would let a composed
    /// <c>t/{tenant}/view-x</c> through and silently retire this guard).
    /// </summary>
    /// <param name="sourceTreeId">The candidate source tree id.</param>
    public static void ThrowIfViewTree(string sourceTreeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        if (LatticeViewTrees.IsViewTree(sourceTreeId))
        {
            throw new InvalidOperationException(
                $"Source tree '{sourceTreeId}' is itself a materialised view (the reserved '{LatticeConstants.ViewTreePrefix}' prefix); a view cannot derive from another view. Point the view at a directly-writable source tree instead.");
        }
    }
}
