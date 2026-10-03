using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The production <see cref="ITenantMembershipUsage"/>: counts a tenant's groups and
/// edges through the membership directory's internal tenant-scoped store. When the
/// host replaced the shipped directory with one that has no tenant-scoped store, the
/// counts are reported unmeasured rather than failing the posture read.
/// </summary>
internal sealed class ScopedStoreTenantMembershipUsage : ITenantMembershipUsage
{
    private readonly ITenantScopedMembershipStore? _store;

    /// <summary>Initializes a new <see cref="ScopedStoreTenantMembershipUsage"/>.</summary>
    /// <param name="directory">The registered membership directory, or <see langword="null"/> when none is registered.</param>
    public ScopedStoreTenantMembershipUsage(ILatticeMembershipDirectory? directory) =>
        _store = directory as ITenantScopedMembershipStore;

    /// <inheritdoc />
    public async Task<long?> CountGroupsAsync(TenantId tenant, CancellationToken cancellationToken) =>
        _store is null ? null : await _store.CountTenantGroupsAsync(tenant, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<long?> CountEdgesAsync(TenantId tenant, CancellationToken cancellationToken) =>
        _store is null ? null : await _store.CountTenantEdgesAsync(tenant, cancellationToken).ConfigureAwait(false);
}
