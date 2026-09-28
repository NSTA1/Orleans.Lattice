namespace Orleans.Lattice.Apps;

/// <summary>
/// The core tree-registry facts the tree ownership ledger consults before a fresh claim: whether a
/// tree is registered, what it was derived from, where it resolves, and which logical trees alias
/// onto a physical tree. The production implementation is <see cref="LatticeAppTreeFacts"/>; the
/// seam exists so the ledger's decisions can be driven deterministically in unit tests.
/// </summary>
internal interface IAppTreeFacts
{
    /// <summary>Whether <paramref name="treeId"/> is registered (live or soft-deleted).</summary>
    /// <param name="treeId">The effective tree id.</param>
    /// <returns><c>true</c> when registered.</returns>
    Task<bool> ExistsAsync(string treeId);

    /// <summary>The logical tree <paramref name="treeId"/> was derived from, or <c>null</c> for an independent tree.</summary>
    /// <param name="treeId">The effective tree id.</param>
    /// <returns>The recorded derivation.</returns>
    Task<string?> GetDerivedFromAsync(string treeId);

    /// <summary>The physical tree <paramref name="treeId"/> resolves to; itself when it is not aliased.</summary>
    /// <param name="treeId">The effective tree id.</param>
    /// <returns>The physical tree id.</returns>
    Task<string> ResolveAsync(string treeId);

    /// <summary>The logical trees whose alias targets <paramref name="physicalTreeId"/>.</summary>
    /// <param name="physicalTreeId">The physical tree id.</param>
    /// <returns>The aliasing logical tree ids; empty when none.</returns>
    Task<IReadOnlyList<string>> GetAliasesTargetingAsync(string physicalTreeId);
}
