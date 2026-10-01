namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>What one read of a tenant's regions found, for <see cref="TenancyRegionFollower"/>.</summary>
internal enum TenancyRegionFollowOutcome
{
    /// <summary>A region changed stage, and some region is still part-way along a path: read again soon.</summary>
    Changed,

    /// <summary>Nothing changed, or the read failed, and some region is still part-way along a path: wait longer.</summary>
    Unchanged,

    /// <summary>Every region is steady, or the page can no longer read them: stop.</summary>
    Steady,
}
