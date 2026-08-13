namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Thrown when a bulk-load (tree creation) session is opened against a tree that
/// is not empty. Bulk-load is a bottom-up <b>tree creation / initial seed</b>
/// primitive; it requires every shard to start empty so the caller can
/// distinguish "the tree already exists / has data" from a transient fault and
/// choose to append through the data plane, target a fresh tree, or abort. A
/// transport binding surfaces this as a distinct, typed <c>TreeNotEmpty</c>
/// outcome rather than a generic failure or a silent no-op.
/// </summary>
public sealed class TreeNotEmptyException : Exception
{
    /// <summary>Initialises the exception for <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The non-empty tree the bulk-load was rejected for.</param>
    public TreeNotEmptyException(string treeId)
        : base($"Tree '{treeId}' is not empty. Bulk-load requires an empty tree (tree creation / initial seed). "
            + "Target a fresh tree, or append into the existing tree through the data plane instead.")
        => TreeId = treeId;

    /// <summary>Initialises the exception with a custom <paramref name="message"/>.</summary>
    public TreeNotEmptyException(string treeId, string message)
        : base(message)
        => TreeId = treeId;

    /// <summary>The non-empty tree the bulk-load was rejected for.</summary>
    public string TreeId { get; }
}
