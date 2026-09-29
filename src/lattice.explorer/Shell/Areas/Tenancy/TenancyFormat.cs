using System.Globalization;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Areas.Tenancy;

/// <summary>
/// The Tenancy area's words and numbers: state labels and their state roles,
/// grant access, quota figures, and region lists. Every figure is formatted with
/// the invariant culture, so a table reads the same on every machine.
/// </summary>
internal static class TenancyFormat
{
    private static readonly string[] ByteUnits = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

    /// <summary>Every quota dimension, in the order the area lists them.</summary>
    public static IReadOnlyList<TenancyQuotaDimension> Dimensions { get; } =
    [
        TenancyQuotaDimension.Bytes,
        TenancyQuotaDimension.Keys,
        TenancyQuotaDimension.MemoryBytes,
        TenancyQuotaDimension.TreeCount,
        TenancyQuotaDimension.OpsPerSecond,
    ];

    /// <summary>A tenant's lifecycle state, in words.</summary>
    /// <param name="status">The state.</param>
    public static string TenantStateLabel(TenantLifecycleStatus status) => status switch
    {
        TenantLifecycleStatus.Active => "Active",
        TenantLifecycleStatus.Suspended => "Suspended",
        _ => "Unknown",
    };

    /// <summary>The state role a tenant's lifecycle state is drawn with.</summary>
    /// <param name="status">The state.</param>
    public static LtStateRole TenantStateRole(TenantLifecycleStatus status) => status switch
    {
        TenantLifecycleStatus.Active => LtStateRole.Enabled,
        TenantLifecycleStatus.Suspended => LtStateRole.Disabled,
        _ => LtStateRole.Unknown,
    };

    /// <summary>A grant's lifecycle state, in words.</summary>
    /// <param name="state">The state.</param>
    public static string GrantStateLabel(TenantGrantLifecycleState state) => state switch
    {
        TenantGrantLifecycleState.Active => "Active",
        TenantGrantLifecycleState.Pending => "Pending",
        TenantGrantLifecycleState.Rejected => "Rejected",
        TenantGrantLifecycleState.Revoked => "Revoked",
        _ => "Unknown",
    };

    /// <summary>The state role a grant's lifecycle state is drawn with.</summary>
    /// <param name="state">The state.</param>
    public static LtStateRole GrantStateRole(TenantGrantLifecycleState state) => state switch
    {
        TenantGrantLifecycleState.Active => LtStateRole.Enabled,
        TenantGrantLifecycleState.Pending => LtStateRole.Drift,
        TenantGrantLifecycleState.Rejected => LtStateRole.Disabled,
        TenantGrantLifecycleState.Revoked => LtStateRole.Uninstalled,
        _ => LtStateRole.Unknown,
    };

    /// <summary>What a grant lets its grantee do, in words.</summary>
    /// <param name="access">The granted operations.</param>
    public static string AccessLabel(TenantGrantAccess access) => access switch
    {
        TenantGrantAccess.Read => "Read",
        TenantGrantAccess.Write => "Write",
        TenantGrantAccess.ReadWrite => "Read and write",
        _ => "Nothing",
    };

    /// <summary>A region's residency lifecycle, in words.</summary>
    /// <param name="status">The region status.</param>
    public static string RegionStatusLabel(TenantRegionLifecycleStatus status) => status switch
    {
        TenantRegionLifecycleStatus.Provisioning => "Provisioning",
        TenantRegionLifecycleStatus.Backfilling => "Backfilling",
        TenantRegionLifecycleStatus.Online => "Online",
        TenantRegionLifecycleStatus.Draining => "Draining",
        TenantRegionLifecycleStatus.Offline => "Offline",
        TenantRegionLifecycleStatus.Removed => "Removed",
        _ => "Not resident",
    };

    /// <summary>The state role a region's residency lifecycle is drawn with.</summary>
    /// <param name="status">The region status.</param>
    public static LtStateRole RegionStatusRole(TenantRegionLifecycleStatus status) => status switch
    {
        TenantRegionLifecycleStatus.Online => LtStateRole.Healthy,
        TenantRegionLifecycleStatus.Provisioning or TenantRegionLifecycleStatus.Backfilling or TenantRegionLifecycleStatus.Draining => LtStateRole.Lagging,
        TenantRegionLifecycleStatus.Offline => LtStateRole.Stalled,
        TenantRegionLifecycleStatus.Removed => LtStateRole.Uninstalled,
        _ => LtStateRole.Disabled,
    };

    /// <summary>
    /// Whether a region in <paramref name="status"/> is in the tenant's residency
    /// set: one being added or held. A draining region is leaving it.
    /// </summary>
    /// <param name="status">The region status.</param>
    public static bool IsResident(TenantRegionLifecycleStatus status) =>
        status is TenantRegionLifecycleStatus.Provisioning or TenantRegionLifecycleStatus.Backfilling or TenantRegionLifecycleStatus.Online;

    /// <summary>The ids of the regions in the residency set, in the order reported.</summary>
    /// <param name="regions">The per-region status.</param>
    public static IReadOnlyList<string> ResidentRegions(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        return [.. regions.Where(region => IsResident(region.Status)).Select(region => region.RegionId)];
    }

    /// <summary>The ids of the allowed regions, in the order reported.</summary>
    /// <param name="regions">The per-region status.</param>
    public static IReadOnlyList<string> AllowedRegions(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        return [.. regions.Where(region => region.IsAllowed).Select(region => region.RegionId)];
    }

    /// <summary>A list of region ids as one line, or <paramref name="none"/> when empty.</summary>
    /// <param name="regions">The region ids.</param>
    /// <param name="none">The text for an empty list.</param>
    public static string RegionList(IReadOnlyList<string> regions, string none = "None")
    {
        ArgumentNullException.ThrowIfNull(regions);
        return regions.Count == 0 ? none : string.Join(", ", regions);
    }

    /// <summary>How the quotas are enforced, in words.</summary>
    /// <param name="scope">The enforcement scope.</param>
    public static string EnforcementLabel(TenantQuotaEnforcementScope scope) => scope switch
    {
        TenantQuotaEnforcementScope.GlobalConverged => "Across every region (converged)",
        TenantQuotaEnforcementScope.PerCluster => "Per cluster",
        _ => "Unknown",
    };

    /// <summary>A quota dimension's name.</summary>
    /// <param name="dimension">The dimension.</param>
    public static string DimensionLabel(TenancyQuotaDimension dimension) => dimension switch
    {
        TenancyQuotaDimension.Bytes => "Stored bytes",
        TenancyQuotaDimension.Keys => "Keys",
        TenancyQuotaDimension.MemoryBytes => "Memory",
        TenancyQuotaDimension.TreeCount => "Trees",
        TenancyQuotaDimension.OpsPerSecond => "Operations per second",
        _ => dimension.ToString(),
    };

    /// <summary>A figure in <paramref name="dimension"/>'s unit.</summary>
    /// <param name="dimension">The dimension the figure measures.</param>
    /// <param name="value">The figure.</param>
    public static string Figure(TenancyQuotaDimension dimension, long value) =>
        dimension is TenancyQuotaDimension.Bytes or TenancyQuotaDimension.MemoryBytes ? Bytes(value) : Count(value);

    /// <summary>A whole number with thousands separators.</summary>
    /// <param name="value">The number.</param>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>A byte count in the largest binary unit that keeps it at or above one.</summary>
    /// <param name="value">The byte count.</param>
    public static string Bytes(long value)
    {
        if (value < 1024)
        {
            return value == 1 ? "1 byte" : Count(value) + " bytes";
        }

        double scaled = value;
        var unit = 0;
        while (scaled >= 1024 && unit < ByteUnits.Length - 1)
        {
            scaled /= 1024;
            unit++;
        }

        return scaled.ToString(scaled < 10 ? "0.#" : "0", CultureInfo.InvariantCulture) + " " + ByteUnits[unit];
    }

    /// <summary>A percentage, such as <c>42%</c>.</summary>
    /// <param name="percent">The whole percentage.</param>
    public static string Percent(int percent) => percent.ToString(CultureInfo.InvariantCulture) + "%";

    /// <summary>
    /// A one-phrase summary of a tenant's use against its quota: the dimension
    /// nearest its ceiling, or whether nothing is capped or measured.
    /// </summary>
    /// <param name="report">The usage report.</param>
    public static string QuotaHeadline(TenantQuotaUsageReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        if (report.IsDefault)
        {
            return "Unbounded";
        }

        TenancyQuotaGauge? nearest = null;
        var bounded = false;
        foreach (var gauge in TenancyQuotaGauge.All(report))
        {
            bounded |= gauge.IsBounded;
            if (gauge.Percent is not { } percent)
            {
                continue;
            }

            if (gauge.IsOverLimit)
            {
                return DimensionLabel(gauge.Dimension) + " over limit";
            }

            if (nearest?.Percent is not { } best || percent > best)
            {
                nearest = gauge;
            }
        }

        if (nearest is { Percent: { } top } found)
        {
            return DimensionLabel(found.Dimension) + " " + Percent(top);
        }

        return bounded ? "Not measured" : "Unbounded";
    }
}
