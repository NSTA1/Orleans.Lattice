using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantTreeCascade"/>: enumerates the target
/// tenant's registered trees straight from the tree registry and soft-deletes
/// them.
/// </summary>
/// <remarks>
/// <para>
/// The enumeration is scoped to the tenant's own <c>t/{tenant}/</c> prefix.
/// Because the registry is an ordinally-sorted Lattice tree, that prefix bounds
/// the tenant's trees into a single contiguous key range, so this is a bounded
/// range scan rather than a read of the whole cluster catalog whose other-tenant
/// ids would then be discarded. The prefix is a performance hint only: ownership
/// is still confirmed per id via <see cref="LatticeTenantTrees.TryGetTenant"/>,
/// which is what decides whether a tree is cascaded.
/// </para>
/// <para>
/// Enumeration runs under a <see cref="LatticeSystemOrigin.Enter"/> scope so the
/// infrastructure registry read is not itself denied by the access gate (the
/// caller was already authorized at the facade seam), and under an explicit
/// cleared active-tenant scope (<see cref="LatticeActiveTenantContext.With"/> with
/// <c>null</c>) so the enumeration is not additionally pruned to whichever tenant
/// happens to be ambient, which need not be the delete target.
/// </para>
/// <para>
/// Each owned tree is soft-deleted under its own system-origin scope so the
/// delete is admitted as trusted infrastructure past the tenant-namespace
/// user-write guard.
/// </para>
/// </remarks>
internal sealed class GrainTenantTreeCascade(IGrainFactory grainFactory) : ITenantTreeCascade
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<int> DeleteTenantTreesAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        // Enumerate only the tenant's own namespace. The registry is ordinally
        // sorted, so t/{tenant}/ bounds its trees into one contiguous key range:
        // this is a bounded range scan rather than a read of the whole cluster
        // catalog that then discards every other tenant's ids. Dialing the
        // registry directly also removes the previous probe-tree-id indirection
        // through the public ILattice surface.
        IReadOnlyList<string> tenantTreeIds;
        using (LatticeSystemOrigin.Enter())
        using (LatticeActiveTenantContext.With(null))
        {
            tenantTreeIds = await _grainFactory
                .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
                .GetAllTreeIdsAsync(LatticeTenantTrees.ComposePrefix(tenant))
                .ConfigureAwait(false);
        }

        var deleted = 0;
        foreach (var treeId in tenantTreeIds)
        {
            // The prefix scan already bounds the range, but ownership is still
            // confirmed structurally: the prefix is a performance hint, and the
            // owner check is what decides whether a tree is actually cascaded.
            if (!LatticeTenantTrees.TryGetTenant(treeId, out var owner) || !owner.Equals(tenant))
            {
                continue;
            }

            using (LatticeSystemOrigin.Enter())
            {
                await _grainFactory
                    .GetGrain<ILattice>(treeId)
                    .DeleteTreeAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            deleted++;
        }

        return deleted;
    }
}
