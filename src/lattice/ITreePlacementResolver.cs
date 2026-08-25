namespace Orleans.Lattice;

/// <summary>
/// The per-silo seam that resolves the physical <see cref="TreePhysicalPlacement"/>
/// for a tree at the moment it is first registered - the
/// <see cref="IWalStorageProviderCatalog"/> key its WAL partitions should be pinned
/// to, and an optional silo placement filter. The interface lives in core so the
/// tree registry can seed a tree's immutable WAL placement without depending on the
/// tenancy add-on; core ships only the <see cref="NullTreePlacementResolver"/>
/// fallback, which resolves every tree to <see cref="TreePhysicalPlacement.Default"/>
/// so core behaves byte-for-byte as it did before per-tenant placement existed. The
/// tenancy package contributes the active implementation that pins a tenant's trees
/// to the dedicated WAL provider named on the tenant's placement binding.
/// </summary>
/// <remarks>
/// The resolver is consulted only at first registration of a tree (the single
/// non-hot control-plane path), never on any per-operation read or write path. A
/// tree's WAL placement is immutable for its lifetime once seeded: re-registration
/// is idempotent and never re-resolves, and a later change to the tenant's placement
/// does not re-place trees that already exist (a migration would require data
/// movement and is out of scope for v1).
/// </remarks>
public interface ITreePlacementResolver
{
    /// <summary>
    /// Attempts to resolve the placement <em>synchronously</em>, with no I/O. Lets
    /// the registry skip the asynchronous path when the answer is known without a
    /// lookup - the null seam always is, and the active resolver is for any
    /// non-tenant (platform, legacy, or system) tree.
    /// </summary>
    /// <param name="treeId">The tree being registered.</param>
    /// <param name="placement">
    /// The resolved placement when this returns <see langword="true"/>; otherwise
    /// <see cref="TreePhysicalPlacement.Default"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the placement was resolved synchronously;
    /// <see langword="false"/> when an asynchronous resolution via
    /// <see cref="ResolveForRegistrationAsync"/> is required. The default
    /// implementation returns <see langword="false"/>, so a resolver that cannot
    /// resolve synchronously safely falls back to the async path.
    /// </returns>
    bool TryResolveForRegistration(string treeId, out TreePhysicalPlacement placement)
    {
        placement = TreePhysicalPlacement.Default;
        return false;
    }

    /// <summary>
    /// Resolves the placement for <paramref name="treeId"/> at registration.
    /// Returns <see cref="TreePhysicalPlacement.Default"/> for any tree with no
    /// dedicated placement - which is every tree when the tenancy add-on is not
    /// registered.
    /// </summary>
    /// <param name="treeId">The tree being registered.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The resolved placement, or <see cref="TreePhysicalPlacement.Default"/>.</returns>
    ValueTask<TreePhysicalPlacement> ResolveForRegistrationAsync(
        string treeId, CancellationToken cancellationToken = default);
}
