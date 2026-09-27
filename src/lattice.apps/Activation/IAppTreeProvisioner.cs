namespace Orleans.Lattice.Apps;

/// <summary>
/// Creates and retires an app's structural trees through the core tree registry and
/// soft-delete seams.
/// </summary>
internal interface IAppTreeProvisioner
{
    /// <summary>
    /// Ensures <paramref name="treeId"/> exists with the declared shape. Idempotent: an existing
    /// tree keeps its pinned structure (a virtual shard count is never changed), and a
    /// soft-deleted tree is recovered.
    /// </summary>
    Task EnsureAsync(string treeId, AppTreeDeclaration declaration, CancellationToken cancellationToken);

    /// <summary>
    /// Soft-deletes <paramref name="treeId"/>, honouring its soft-delete window; never purges.
    /// A tree that does not exist is a no-op.
    /// </summary>
    Task SoftDeleteAsync(string treeId, CancellationToken cancellationToken);
}
