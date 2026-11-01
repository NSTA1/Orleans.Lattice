namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The one predicate for "a tree only the library itself can put content into":
/// the <see cref="LatticeConstants.SystemTreePrefix"/> control namespace and the
/// <see cref="LatticeConstants.SystemDataTreePrefix"/> system-data namespace.
/// </summary>
/// <remarks>
/// <para>
/// Both namespaces are refused to user-origin content on every path that can
/// materialise rows. A <c>_lattice_</c> tree cannot be addressed through the
/// public <see cref="ILattice"/> surface at all (<c>LatticeGrain.ThrowIfSystemTree</c>).
/// A <c>sys-</c> tree refuses user-origin writes, deletes and CRDT applies
/// (<c>LatticeGrain.ThrowIfUserOriginSystemDataTree</c>), bulk load and snapshot
/// destinations (<c>LatticeGrain.ThrowIfReservedSnapshotDestination</c>, which
/// uses this predicate), merge sources (<c>TreeMergeGrain</c>), alias targets
/// (<c>LatticeRegistryGrain</c>), and tenant addressing
/// (<c>LatticeTenantResolution</c>). Only a system-origin scope - a first-party
/// add-on writing its own store - passes those guards.
/// </para>
/// <para>
/// That is why it is safe to give these trees privileges that would be abusable
/// on a user tree, such as the reserved WAL replay lane (issue #5569). A replay's
/// cost is the WAL content it must read, and no caller outside the library can
/// put WAL content into a tree this predicate accepts. A tree-administration
/// caller can register an empty <c>sys-</c> tree, but it cannot write to it, so
/// the lane it would reach has nothing to replay.
/// </para>
/// <para>
/// A materialised view over a system tree (<c>view-sys-*</c>) is deliberately
/// <b>not</b> accepted. A view's name is caller-chosen, so a user can create a
/// view named <c>sys-anything</c> over their own tree, and a name test cannot
/// tell the two apart.
/// </para>
/// </remarks>
internal static class SystemTreeNamespace
{
    /// <summary>
    /// Whether <paramref name="treeId"/> lies in a namespace that refuses
    /// user-origin content.
    /// </summary>
    /// <param name="treeId">The physical tree id. <see langword="null"/> or empty is not system-owned.</param>
    /// <returns><see langword="true"/> for a <c>_lattice_</c> or <c>sys-</c> tree.</returns>
    public static bool IsSystemOwned(string? treeId)
        => !string.IsNullOrEmpty(treeId)
           && (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
               || treeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal));
}
