namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The active <see cref="ITreePlacementResolver"/> contributed by the tenancy
/// add-on. At tree registration it derives the tree's tenant from its id
/// (<see cref="LatticeTenantTrees.TryGetTenant(string, out TenantId)"/>), reads the
/// tenant's <see cref="TenantPlacement"/> from the in-memory
/// <see cref="TenantPlacementSnapshotMaintainer"/>, and pins the tree to the
/// tenant's dedicated WAL provider when one is bound - otherwise it resolves to
/// <see cref="TreePhysicalPlacement.Default"/> so routing is unchanged.
/// </summary>
/// <remarks>
/// <para>
/// Resolution is a <b>pure, synchronous, in-memory lookup</b>: it reads the current
/// placement snapshot and never touches a grain. This is load-bearing, not an
/// optimisation - the resolver is invoked from inside the singleton, non-reentrant
/// registry grain's <c>RegisterAsync</c> turn, so a live registry read here would
/// re-enter the same grain and self-deadlock. The change-feed-maintained snapshot
/// moves that read off the registration turn entirely.
/// </para>
/// <para>
/// A non-tenant (platform, legacy, or system) tree is resolved with no snapshot
/// read and always maps to the baseline placement, so enabling tenancy leaves
/// legacy and system trees byte-for-byte unchanged. A tenant-scoped
/// <c>t/{tenant}/{name}</c> tree consults the snapshot; a tenant not yet present in
/// it (a tree registered before its tenant record is observed) resolves to the
/// baseline placement, which is fail-safe.
/// </para>
/// <para>
/// The binding is honoured only when the tenant explicitly requires a dedicated WAL
/// (<see cref="TenantPlacement.DedicatedWal"/>) and names a provider
/// (<see cref="TenantPlacement.WalProviderName"/>); a shared binding, or a dedicated
/// flag with no named provider, resolves to the baseline key. Once a tenant's trees
/// are placed the physical binding is immutable in v1: the registry grain seeds the
/// pin only for a tree with no existing placement, so a later placement change does
/// not migrate trees that already exist.
/// </para>
/// </remarks>
internal sealed class TenantWalPlacementResolver(TenantPlacementSnapshotMaintainer snapshots)
    : ITreePlacementResolver
{
    /// <inheritdoc />
    public bool TryResolveForRegistration(string treeId, out TreePhysicalPlacement placement)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // The resolution is a synchronous snapshot read, so the fast (Try) path
        // always succeeds and the registry grain never awaits the async fallback -
        // that is what keeps registration free of a re-entrant grain hop.
        placement = Resolve(treeId);
        return true;
    }

    /// <inheritdoc />
    public ValueTask<TreePhysicalPlacement> ResolveForRegistrationAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // Synchronously completed: the snapshot read never blocks and never hops a
        // grain, so there is no async state machine on this path.
        return new ValueTask<TreePhysicalPlacement>(Resolve(treeId));
    }

    private TreePhysicalPlacement Resolve(string treeId)
    {
        // A non-tenant tree is never pinned to a dedicated placement - resolved with
        // no snapshot read.
        if (!LatticeTenantTrees.TryGetTenant(treeId, out var tenant))
        {
            return TreePhysicalPlacement.Default;
        }

        // Pure in-memory snapshot read: no grain hop, so this is safe to call from
        // inside the registry grain's RegisterAsync turn. A tenant not yet in the
        // snapshot resolves to baseline (fail-safe).
        if (!snapshots.Current.TryGetPlacement(tenant, out var placement))
        {
            return TreePhysicalPlacement.Default;
        }

        // Pin only when the tenant explicitly requires a dedicated WAL and names a
        // provider; otherwise fall back to the baseline key so routing is unchanged.
        if (!placement.DedicatedWal || string.IsNullOrEmpty(placement.WalProviderName))
        {
            return TreePhysicalPlacement.Default;
        }

        return new TreePhysicalPlacement
        {
            WalProviderKey = placement.WalProviderName,
            PlacementFilter = placement.PlacementFilter,
        };
    }
}
