using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>One region of a residency plan, as the regions table draws it.</summary>
/// <param name="RegionId">The region id.</param>
/// <param name="Status">The region's residency lifecycle as the cluster reported it.</param>
/// <param name="IsAllowed">Whether an operator has allowed the tenant this region.</param>
/// <param name="IsResident">Whether the region is in the committed residency set.</param>
/// <param name="IsPlanned">Whether the plan keeps (or adds) the region.</param>
/// <param name="Refusal">Why the region's residency cannot be toggled, or <see langword="null"/> when it can.</param>
/// <param name="IsServed">Whether the region serves the tenant now: every region does while the tenant has no residency, and only an Online one once it has.</param>
/// <param name="BackfillProgress">Receiver-side progress while this region is Backfilling.</param>
internal sealed record TenancyRegionRow(
    string RegionId,
    TenantRegionLifecycleStatus Status,
    bool IsAllowed,
    bool IsResident,
    bool IsPlanned,
    string? Refusal,
    bool IsServed,
    TenantRegionBackfillProgress? BackfillProgress);
