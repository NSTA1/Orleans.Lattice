using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The receive-fence epoch under which a replication apply was admitted (issue
/// #4593), and the tree whose fence admitted it, carried from the replication
/// applier to the tree's apply seam in the <see cref="RequestContext"/>.
/// <para>
/// A tree's receive fence bumps its epoch on every pause. A coordinated restore
/// closes its restored copy with the epoch of the pause it took, as the copy's
/// minimum admission epoch. An apply admitted by that tree's fence under an older
/// epoch was admitted before that pause, so it carries a pre-cutover write and
/// must never land on the restored copy - even after the copy opens. The seam
/// refuses it.
/// </para>
/// <para>
/// Set it only from inside an <see langword="async"/> method: the request context
/// is flow-scoped, so the value then reaches the calls that method makes and is
/// discarded when it returns.
/// </para>
/// </summary>
internal static class ReplicationAdmissionEpoch
{
    /// <summary>The request-context key that carries the admitting tree's id.</summary>
    internal const string TreeRequestContextKey = "lattice.replication.admission-tree";

    /// <summary>The request-context key that carries the admission epoch.</summary>
    internal const string EpochRequestContextKey = "lattice.replication.admission-epoch";

    /// <summary>Stamps the current flow with the tree and epoch its apply was admitted under.</summary>
    /// <param name="treeId">The tree whose receive fence admitted the apply.</param>
    /// <param name="epoch">The receive-fence epoch the admission observed.</param>
    internal static void Stamp(string treeId, long epoch)
    {
        RequestContext.Set(TreeRequestContextKey, treeId);
        RequestContext.Set(EpochRequestContextKey, epoch);
    }

    /// <summary>Reads the current flow's admission stamp.</summary>
    /// <param name="treeId">The tree whose receive fence admitted the apply.</param>
    /// <param name="epoch">The receive-fence epoch the admission observed.</param>
    /// <returns><see langword="true"/> when the flow carries a stamp.</returns>
    internal static bool TryGet(out string treeId, out long epoch)
    {
        if (RequestContext.Get(TreeRequestContextKey) is string tree
            && RequestContext.Get(EpochRequestContextKey) is long observed)
        {
            treeId = tree;
            epoch = observed;
            return true;
        }

        treeId = string.Empty;
        epoch = 0;
        return false;
    }
}