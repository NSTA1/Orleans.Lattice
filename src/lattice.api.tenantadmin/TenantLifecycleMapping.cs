using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Projects the tenancy package's stored tenant and region states onto the
/// transport-agnostic lifecycle statuses the tenant administration facades
/// report, so every facade describes the same record identically.
/// </summary>
internal static class TenantLifecycleMapping
{
    /// <summary>Maps a stored tenant status to its reported lifecycle status.</summary>
    /// <param name="status">The stored status.</param>
    internal static TenantLifecycleStatus ToLifecycleStatus(TenantStatus status) => status switch
    {
        TenantStatus.Active => TenantLifecycleStatus.Active,
        TenantStatus.Suspended => TenantLifecycleStatus.Suspended,
        _ => TenantLifecycleStatus.Active,
    };

    /// <summary>Maps a stored tenant-region status to its reported lifecycle status.</summary>
    /// <param name="status">The stored status.</param>
    internal static TenantRegionLifecycleStatus ToLifecycleStatus(TenantRegionStatus status) => status switch
    {
        TenantRegionStatus.Provisioning => TenantRegionLifecycleStatus.Provisioning,
        TenantRegionStatus.Backfilling => TenantRegionLifecycleStatus.Backfilling,
        TenantRegionStatus.Online => TenantRegionLifecycleStatus.Online,
        TenantRegionStatus.Draining => TenantRegionLifecycleStatus.Draining,
        TenantRegionStatus.Offline => TenantRegionLifecycleStatus.Offline,
        TenantRegionStatus.Removed => TenantRegionLifecycleStatus.Removed,
        _ => TenantRegionLifecycleStatus.None,
    };

    /// <summary>
    /// Describes every region a tenant record names - allowed, or carrying a
    /// lifecycle status - in ordinal region-id order.
    /// </summary>
    /// <param name="record">The tenant record.</param>
    internal static IReadOnlyList<TenantRegionStatusDescriptor> DescribeRegions(TenantRecord record)
    {
        var regionIds = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var regionId in record.AllowedRegionIds)
        {
            regionIds.Add(regionId);
        }

        foreach (var entry in record.RegionStatusEntries)
        {
            regionIds.Add(entry.Key);
        }

        var descriptors = new List<TenantRegionStatusDescriptor>(regionIds.Count);
        foreach (var regionId in regionIds)
        {
            descriptors.Add(new TenantRegionStatusDescriptor
            {
                RegionId = regionId,
                Status = ToLifecycleStatus(record.GetRegionStatus(regionId)),
                IsAllowed = record.IsRegionAllowed(regionId),
            });
        }

        return descriptors;
    }
}
