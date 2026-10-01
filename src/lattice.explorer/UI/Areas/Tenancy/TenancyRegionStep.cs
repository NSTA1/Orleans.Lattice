using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// Where a region in a transitional residency stage is on its path, as the
/// regions table draws it: the add path is Provisioning, Backfilling, Online and
/// the remove path is Draining, Offline, Removed, so a region reports the step it
/// has reached out of three, the stage it is in, and what comes next and who
/// takes it.
/// </summary>
/// <remarks>
/// <c>ILatticeTenantRegionAdmin.GetTenantRegionStatusAsync</c> reports a stage
/// per region and nothing finer - no fraction, count or time - so progress is a
/// step on a named path and never a percentage within a stage. The remove path's
/// steps are taken automatically by the region's own silos; the add path's are an
/// operator step of the hosting deployment, because nothing in Lattice copies a
/// tenant's data into an added region.
/// </remarks>
/// <param name="Step">The step the region has reached, from 1.</param>
/// <param name="Stage">The stage the region is in, such as <c>Draining</c>.</param>
/// <param name="IsRemoving">Whether the region is on the remove path rather than the add path.</param>
/// <param name="Next">What comes next and who takes it, as one sentence.</param>
internal sealed record TenancyRegionStep(int Step, string Stage, bool IsRemoving, string Next)
{
    /// <summary>The steps on either path.</summary>
    public const int Steps = 3;

    /// <summary>The next step on the add path, which a platform operator of the hosting deployment takes.</summary>
    public const string ByOperator = "a platform operator of the hosting deployment promotes it";

    /// <summary>The next step on the remove path, which the region's own silos take.</summary>
    public const string Automatically = "taken automatically by the region's own silos";

    /// <summary>The path's name, as the bar's phase reads it: "Adding" or "Removing".</summary>
    public string Path => IsRemoving ? "Removing" : "Adding";

    /// <summary>The bar's phase: the path and the stage, such as "Removing: Draining".</summary>
    public string Phase => Path + ": " + Stage;

    /// <summary>
    /// The step a region in <paramref name="status"/> has reached, or
    /// <see langword="null"/> for a steady stage (Online, Removed, or none),
    /// which has no step to show.
    /// </summary>
    /// <param name="status">The region's residency lifecycle stage.</param>
    public static TenancyRegionStep? For(TenantRegionLifecycleStatus status) => status switch
    {
        TenantRegionLifecycleStatus.Provisioning => new(1, "Provisioning", false, $"Next: Backfilling, when {ByOperator}."),
        TenantRegionLifecycleStatus.Backfilling => new(2, "Backfilling", false, $"Next: Online, when {ByOperator}."),
        TenantRegionLifecycleStatus.Draining => new(1, "Draining", true, $"Next: Offline, {Automatically}."),
        TenantRegionLifecycleStatus.Offline => new(2, "Offline", true, $"Next: Removed, {Automatically}."),
        _ => null,
    };

    /// <summary>Whether a region in <paramref name="status"/> is part-way along a path, so the page follows it.</summary>
    /// <param name="status">The region's residency lifecycle stage.</param>
    public static bool IsTransitional(TenantRegionLifecycleStatus status) => For(status) is not null;

    /// <summary>The bar's accessible name for <paramref name="regionId"/>, such as "Residency change in us-east".</summary>
    /// <param name="regionId">The region id.</param>
    public static string Label(string regionId) => "Residency change in " + regionId;

    /// <summary>The step as a short phrase for a compact row, such as "removing, step 1 of 3".</summary>
    public string Short => $"{Path.ToLowerInvariant()}, step {Step} of {Steps}";
}
