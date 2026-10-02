namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Read-only WAL reclamation diagnostics for a tree: which durable pin holds the
/// tree's write-ahead-log floor and whether it has wedged reclamation (issue
/// #4195). A separate interface so that adding it changes no released facade.
/// </summary>
public interface ILatticeWalReclamation
{
    /// <summary>
    /// Reads which durable materialiser pin holds <paramref name="treeId"/>'s WAL
    /// floor, the leaf behind it and that leaf's persisted checkpoint, and whether
    /// the floor is wedged. Probes the durable pin store and one leaf's durable
    /// state on demand, without activating the leaf, after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> fail-closed. A pure read with no side
    /// effects.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty. An aliased tree is resolved to its physical tree.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The reclamation report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeWalReclamationReport> GetWalReclamationAsync(string treeId, CancellationToken cancellationToken = default);
}
