using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host <see cref="ITenantRegionVisibilityResolver"/>: resolves the calling
/// tenant's per-region standing by asking the remote cluster's region-residency
/// facade over gRPC, so a split (remote) MCP head can scope
/// <c>lattice_list_regions</c> to the tenant exactly as a co-hosted head does. A
/// co-hosted head reads the tenancy registry in-process instead; this adapter is
/// the wire equivalent of that read.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed.</b> Any outcome that leaves the standing unknown - the call is
/// denied, the tenant is unknown, or the remote endpoint is unreachable - resolves
/// to <see cref="TenantRegionVisibilityMap.Unresolved"/>, so the catalog degrades
/// to the current region alone rather than disclosing the full topology. A
/// cancellation is deliberately not swallowed: it is the caller's own signal.
/// </para>
/// <para>
/// <b>Cost.</b> Registered only when the head is configured with a tenant-admin
/// endpoint, and consulted only when a call asserts a non-default tenant, so an
/// operator call and a tenancy-off head never pay the round trip.
/// </para>
/// </remarks>
internal sealed class GrpcTenantRegionVisibilityResolver : ITenantRegionVisibilityResolver
{
    private readonly ILatticeTenantRegionAdmin _regionAdmin;

    /// <summary>Initialises the resolver over the remote region-residency facade.</summary>
    /// <param name="regionAdmin">The region-residency facade. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="regionAdmin"/> is <c>null</c>.</exception>
    public GrpcTenantRegionVisibilityResolver(ILatticeTenantRegionAdmin regionAdmin)
    {
        ArgumentNullException.ThrowIfNull(regionAdmin);
        _regionAdmin = regionAdmin;
    }

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public async ValueTask<TenantRegionVisibilityMap> ResolveAsync(
        TenantId tenant, CancellationToken cancellationToken = default)
    {
        if (tenant.Value is null)
        {
            return TenantRegionVisibilityMap.Unresolved;
        }

        TenantRegionStatusReport report;
        try
        {
            report = await _regionAdmin
                .GetTenantRegionStatusAsync(tenant.Value, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return TenantRegionVisibilityMap.Unresolved;
        }

        var regions = report.Regions;
        var map = new KeyValuePair<string, TenantRegionVisibility>[regions.Count];
        for (var i = 0; i < regions.Count; i++)
        {
            var region = regions[i];
            map[i] = new KeyValuePair<string, TenantRegionVisibility>(
                region.RegionId,
                new TenantRegionVisibility(region.IsAllowed, MapStatus(region.Status)));
        }

        return TenantRegionVisibilityMap.Create(map);
    }

    private static TenantRegionResidencyStatus MapStatus(TenantRegionLifecycleStatus status) => status switch
    {
        TenantRegionLifecycleStatus.Provisioning => TenantRegionResidencyStatus.Provisioning,
        TenantRegionLifecycleStatus.Backfilling => TenantRegionResidencyStatus.Backfilling,
        TenantRegionLifecycleStatus.Online => TenantRegionResidencyStatus.Online,
        TenantRegionLifecycleStatus.Draining => TenantRegionResidencyStatus.Draining,
        TenantRegionLifecycleStatus.Offline => TenantRegionResidencyStatus.Offline,
        TenantRegionLifecycleStatus.Removed => TenantRegionResidencyStatus.Removed,
        _ => TenantRegionResidencyStatus.None,
    };
}
