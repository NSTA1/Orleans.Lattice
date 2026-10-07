using System.Globalization;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

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
    /// <param name="hasResidency">
    /// Whether the tenant has residency set. A region with no status reads as
    /// outside the residency when it has, and as serving the tenant, with every
    /// other region, when it has not.
    /// </param>
    public static string RegionStatusLabel(TenantRegionLifecycleStatus status, bool hasResidency = true) => status switch
    {
        TenantRegionLifecycleStatus.Provisioning => "Provisioning",
        TenantRegionLifecycleStatus.Backfilling => "Backfilling",
        TenantRegionLifecycleStatus.Online => "Online",
        TenantRegionLifecycleStatus.Draining => "Draining",
        TenantRegionLifecycleStatus.Offline => "Offline",
        TenantRegionLifecycleStatus.Removed => "Removed",
        _ => hasResidency ? "Not in residency" : "No residency set",
    };

    /// <summary>What a region in the Provisioning state is waiting for, and what it means for the tenant.</summary>
    public const string ProvisioningMeaning =
        "Not served here until it is Online. Nothing in Lattice advances an added region: a platform operator of the hosting deployment promotes it once the tenant's data is in place.";

    /// <summary>What a region in the Backfilling state means for the tenant.</summary>
    public const string BackfillingMeaning =
        "Not served here until it is Online. Lattice copies no data into an added region; the hosting deployment fills it in, and a platform operator promotes it.";

    /// <summary>What a region in the Draining state means for the tenant, and what it means when it stays there.</summary>
    public const string DrainingMeaning =
        "Awaiting confirmation from this region: its silos complete the drain after observing the change in sys-tenant-registry. If it stays Draining, check that the region is running and registry replication works in both directions. This view cannot confirm whether the remote region has observed the change; it may still serve the tenant until it does.";

    /// <summary>What a region means for a tenant with no residency set.</summary>
    public const string NoResidencyMeaning = "No residency is set, so this region serves the tenant, as every region does.";

    /// <summary>How a tenant with no residency set is described: it is served in every region.</summary>
    public const string NoResidency = "Not set: served in every region";

    /// <summary>
    /// How a tenant is described whose residency was set and has no region left
    /// in it (each region is Draining, Offline or Removed): once set, residency
    /// stays set, so the tenant is served in no region.
    /// </summary>
    public const string NoResidentRegion = "None: served in no region";

    /// <summary>Whether a region serves the tenant, in words.</summary>
    public const string ServedLabel = "Served";

    /// <summary>Whether a region does not serve the tenant, in words.</summary>
    public const string NotServedLabel = "Not served";

    /// <summary>
    /// Where a platform operator of the hosting deployment learns how to promote a
    /// tenant's region to Online. The facade has no promotion call, so the
    /// Explorer cannot offer the action itself.
    /// </summary>
    public const string PromotionHelpUrl = "https://nsta1.github.io/Orleans.Lattice/docs/lattice.tenancy/README.html#lifecycle-states";

    /// <summary>
    /// What a region's residency lifecycle means for the tenant, following the
    /// tenancy engine: an added region stays Provisioning, and then Backfilling,
    /// until a platform operator of the hosting deployment promotes it, because
    /// nothing in Lattice copies the tenant's data in; a removed region's own silos
    /// step it from Draining to Offline to Removed on their own; and once a tenant
    /// has residency only an Online region serves it. With none set, every region
    /// does.
    /// </summary>
    /// <param name="status">The region status.</param>
    /// <param name="hasResidency">Whether the tenant has residency set.</param>
    public static string RegionStatusMeaning(TenantRegionLifecycleStatus status, bool hasResidency = true) => status switch
    {
        TenantRegionLifecycleStatus.Provisioning => ProvisioningMeaning,
        TenantRegionLifecycleStatus.Backfilling => BackfillingMeaning,
        TenantRegionLifecycleStatus.Online => "Serves this tenant.",
        TenantRegionLifecycleStatus.Draining => DrainingMeaning,
        TenantRegionLifecycleStatus.Offline => "Drained; no longer serves this tenant.",
        TenantRegionLifecycleStatus.Removed => "Left the tenant's residency; does not serve this tenant.",
        _ => hasResidency ? "Outside the tenant's residency, so it does not serve this tenant." : NoResidencyMeaning,
    };

    /// <summary>
    /// A tenant's resident regions as one line; <see cref="NoResidency"/> when no
    /// residency is set; or <see cref="NoResidentRegion"/> when residency is set
    /// and no region is left in it.
    /// </summary>
    /// <param name="regions">The per-region status.</param>
    public static string ResidencyText(IReadOnlyList<TenantRegionStatusDescriptor> regions) =>
        RegionList(ResidentRegions(regions), HasResidency(regions) ? NoResidentRegion : NoResidency);

    /// <summary>
    /// Why a tenant that <see cref="IsServedNowhere"/> is served nowhere, as one
    /// sentence after "This tenant is not served anywhere:", and what serves it
    /// again.
    /// </summary>
    /// <param name="regions">The per-region status.</param>
    public static string ServedNowhereReason(IReadOnlyList<TenantRegionStatusDescriptor> regions) =>
        ResidentRegions(regions).Count > 0
            ? "it has residency set and none of its regions is Online yet. It is served again once a platform operator of the hosting deployment promotes one to Online."
            : "it has residency set and every region has left it. It is served again once a region is added to its residency and a platform operator of the hosting deployment promotes it to Online.";

    /// <summary>
    /// Whether a tenant has residency set: any region carries a lifecycle status,
    /// as the tenancy engine counts it (<c>TenantRecord.HasResidencyConfiguration</c>),
    /// including a region that is Offline or Removed. Without one the tenant is
    /// served in every region; with one it is served only where it is Online, so a
    /// tenant whose regions are all Offline or Removed is served nowhere.
    /// </summary>
    /// <param name="regions">The per-region status.</param>
    public static bool HasResidency(IReadOnlyList<TenantRegionStatusDescriptor> regions)
    {
        ArgumentNullException.ThrowIfNull(regions);
        return regions.Any(region => region.Status != TenantRegionLifecycleStatus.None);
    }

    /// <summary>
    /// Whether a region in <paramref name="status"/> serves the tenant: every
    /// region does while the tenant has no residency, and only an Online one once
    /// it has.
    /// </summary>
    /// <param name="status">The region status.</param>
    /// <param name="hasResidency">Whether the tenant has residency set.</param>
    public static bool IsServedIn(TenantRegionLifecycleStatus status, bool hasResidency) =>
        !hasResidency || status == TenantRegionLifecycleStatus.Online;

    /// <summary>
    /// Whether a tenant has residency set yet none of its regions is Online, so it
    /// is served nowhere: once a tenant has any residency it is served only in a
    /// region that reports Online.
    /// </summary>
    /// <param name="regions">The per-region status.</param>
    public static bool IsServedNowhere(IReadOnlyList<TenantRegionStatusDescriptor> regions) =>
        HasResidency(regions) && !regions.Any(region => region.Status == TenantRegionLifecycleStatus.Online);

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

    /// <summary>A byte count in the largest binary unit that keeps it at or above one, and below 1024 as written.</summary>
    /// <param name="value">The byte count.</param>
    public static string Bytes(long value)
    {
        if (value < 1024)
        {
            return value == 1 ? "1 byte" : Count(value) + " bytes";
        }

        double scaled = value;
        var unit = 0;

        // The unit is chosen on the figure as written, so a size that rounds up to
        // 1024 moves to the next unit rather than reading "1024 KiB" (#4355).
        while (Math.Round(scaled, scaled < 10 ? 1 : 0, MidpointRounding.AwayFromZero) >= 1024 && unit < ByteUnits.Length - 1)
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
