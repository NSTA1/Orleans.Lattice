using Microsoft.Extensions.Options;

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
/// <c>t/{tenant}/{name}</c> tree consults the snapshot, and only while it is
/// authoritative (<see cref="TenantPlacementSnapshotMaintainer.IsSnapshotAuthoritative"/>);
/// a tenant absent from an authoritative snapshot is unregistered and resolves to
/// the baseline placement.
/// </para>
/// <para>
/// <b>Fail closed on a stale snapshot (issue #4052).</b> A seeded WAL pin is
/// immutable, so a placement resolved from a stale snapshot - on a silo that has not
/// yet learned a tenant was moved to a dedicated WAL - would put that tenant's tree on
/// the shared WAL permanently and silently. And the resolver cannot confirm against
/// the tenant registry instead: it runs inside the non-reentrant tree registry
/// grain's turn, and a registry read could re-enter that grain. So while the snapshot
/// is not authoritative the synchronous path declines, and
/// <see cref="ResolveForRegistrationAsync"/> waits a bounded time (a fifth of
/// <see cref="LatticeTenancyOptions.PolicySnapshotLeaseDuration"/>) for the rebuild
/// that restores authority - it runs on its own call chain - and then
/// <b>refuses</b> the registration with a retryable <see cref="TimeoutException"/>
/// rather than seed a placement that may be wrong. A refused registration is loud
/// and retryable; a wrong placement is neither. The short wait keeps the common
/// create-a-tenant-then-its-trees flow working without a retry.
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
internal sealed class TenantWalPlacementResolver(
    TenantPlacementSnapshotMaintainer snapshots,
    IOptions<LatticeTenancyOptions> options)
    : ITreePlacementResolver
{
    private readonly TimeSpan _authorityWait = options.Value.PolicySnapshotLeaseDuration / 5;

    /// <inheritdoc />
    /// <remarks>
    /// Resolves synchronously for a non-tenant tree, and for a tenant tree while the
    /// placement snapshot is authoritative - the steady state, so the registry grain
    /// normally never awaits the async path. Declines (returns <see langword="false"/>)
    /// for a tenant tree while the snapshot is not authoritative.
    /// </remarks>
    public bool TryResolveForRegistration(string treeId, out TreePhysicalPlacement placement)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        if (!LatticeTenantTrees.TryGetTenant(treeId, out var tenant))
        {
            placement = TreePhysicalPlacement.Default;
            return true;
        }

        if (!snapshots.IsSnapshotAuthoritative)
        {
            placement = TreePhysicalPlacement.Default;
            return false;
        }

        placement = Resolve(tenant);
        return true;
    }

    /// <inheritdoc />
    /// <remarks>
    /// While the placement snapshot is not authoritative, waits a bounded time for it
    /// to become so and then throws <see cref="TimeoutException"/> (retryable) rather
    /// than resolve a tenant tree from a snapshot that may be stale.
    /// </remarks>
    /// <exception cref="TimeoutException">The placement snapshot did not become authoritative within the bound.</exception>
    public ValueTask<TreePhysicalPlacement> ResolveForRegistrationAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        return TryResolveForRegistration(treeId, out var placement)
            ? new ValueTask<TreePhysicalPlacement>(placement)
            : WaitThenResolveAsync(treeId, cancellationToken);
    }

    private async ValueTask<TreePhysicalPlacement> WaitThenResolveAsync(
        string treeId, CancellationToken cancellationToken)
    {
        if (!await snapshots.WaitUntilAuthoritativeAsync(_authorityWait, cancellationToken).ConfigureAwait(false))
        {
            throw new TimeoutException(
                $"Tree '{treeId}' was not registered: this silo's tenant-placement snapshot could not be confirmed "
                + "current (a tenant-registry change is still being applied, or the silo cannot reach the "
                + "tenant-policy epoch), so its WAL placement cannot be resolved safely. Retry the registration.");
        }

        LatticeTenantTrees.TryGetTenant(treeId, out var tenant);
        return Resolve(tenant);
    }

    private TreePhysicalPlacement Resolve(TenantId tenant)
    {
        // Pure in-memory read of an authoritative snapshot: no grain hop, so this is
        // safe inside the registry grain's RegisterAsync turn. A tenant absent from
        // it is unregistered and resolves to baseline.
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
