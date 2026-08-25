using Orleans.Lattice;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantTreeCascade"/>: enumerates the cluster's
/// registered trees through the public <see cref="ILattice"/> grain surface and
/// soft-deletes those owned by the target tenant.
/// </summary>
/// <remarks>
/// <para>
/// Enumeration runs under a <see cref="LatticeSystemOrigin.Enter"/> scope so the
/// infrastructure registry read is not itself denied by the access gate (the
/// caller was already authorized at the facade seam), and under an explicit
/// cleared active-tenant scope (<see cref="LatticeActiveTenantContext.With"/> with
/// <c>null</c>) so the enumeration returns the <b>unfiltered</b> global tree list;
/// the cascade then selects exactly the target tenant's trees itself via
/// <see cref="LatticeTenantTrees.TryGetTenant"/>, rather than depending on the
/// ambient active tenant matching the delete target.
/// </para>
/// <para>
/// Each owned tree is soft-deleted under its own system-origin scope so the
/// delete is admitted as trusted infrastructure past the tenant-namespace
/// user-write guard.
/// </para>
/// </remarks>
internal sealed class GrainTenantTreeCascade(IGrainFactory grainFactory) : ITenantTreeCascade
{
    // An arbitrary, grammar-valid, read-only probe id used only to reach the
    // ILattice surface whose GetAllTreeIdsAsync reads the shared global tree
    // registry (the result is independent of the probe id). Getting the grain
    // reference and issuing a read never registers a tree.
    private const string EnumerationProbeTreeId = "lattice-tenant-admin-probe";

    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<int> DeleteTenantTreesAsync(TenantId tenant, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> allTreeIds;
        using (LatticeSystemOrigin.Enter())
        using (LatticeActiveTenantContext.With(null))
        {
            allTreeIds = await _grainFactory
                .GetGrain<ILattice>(EnumerationProbeTreeId)
                .GetAllTreeIdsAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var deleted = 0;
        foreach (var treeId in allTreeIds)
        {
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
