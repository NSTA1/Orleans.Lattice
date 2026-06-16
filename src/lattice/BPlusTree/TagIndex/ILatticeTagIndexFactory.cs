namespace Orleans.Lattice;

/// <summary>
/// Resolves <see cref="ILatticeTagIndex"/> and <see cref="ILatticeMultiTreeTagIndex"/>
/// instances that are pre-wired to the host's replication configuration.
/// <para>
/// This is the entry point for opening a tag index. The factory captures the
/// injectable <see cref="ILatticeReplicationContext"/> seam, so the membership
/// convergence mode and the dot-authoring local replica id are sourced from
/// server configuration (a single source of truth) rather than threaded through
/// every call site. Under a single cluster the captured seam reports replication
/// disabled and the index uses the plain last-writer-wins membership path -
/// correct and lossless for single-writer-per-key indexes; under active-active
/// (multi-writer) replication it authors flag-CRDT membership so the index
/// converges.
/// </para>
/// <para>
/// Registered as a singleton by <c>ISiloBuilder.AddLattice(...)</c>; the
/// <c>Orleans.Lattice.Replication</c> package supplies the configured seam the
/// factory consumes when <c>AddLatticeReplication</c> is present.
/// </para>
/// </summary>
public interface ILatticeTagIndexFactory
{
    /// <summary>
    /// Opens the single-tree tag index named <paramref name="indexName"/> bound
    /// to the subject tree <paramref name="tree"/>, pre-wired to the host's
    /// replication configuration.
    /// </summary>
    /// <param name="tree">The subject tree whose keys are tagged.</param>
    /// <param name="indexName">The logical index name; the index tree is resolved as <c>tag-{indexName}</c>.</param>
    ILatticeTagIndex Create(ILattice tree, string indexName);

    /// <summary>
    /// Opens the multi-tree view of the tag index named
    /// <paramref name="indexName"/>, pre-wired to the host's replication
    /// configuration, without pre-binding a subject tree.
    /// </summary>
    /// <param name="indexName">The logical index name; the index tree is resolved as <c>tag-{indexName}</c>.</param>
    /// <param name="allowedTrees">Optional closed allowlist of subject tree ids that may be tagged.</param>
    ILatticeMultiTreeTagIndex CreateMultiTree(string indexName, IReadOnlyCollection<string>? allowedTrees = null);
}
