using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// One tenant in the directory: its descriptor at once, and its regions, quota
/// use and installed-app count as they arrive, each <see langword="null"/> until
/// read and staying so when the read fails.
/// </summary>
/// <param name="tenant">The tenant's descriptor from the accessible-tenant list.</param>
internal sealed class TenancyDirectoryRow(TenantDescriptor tenant)
{
    /// <summary>The tenant's descriptor.</summary>
    public TenantDescriptor Tenant { get; } = tenant ?? throw new ArgumentNullException(nameof(tenant));

    /// <summary>The tenant id.</summary>
    public string TenantId => Tenant.TenantId;

    /// <summary>The resident regions, or <see langword="null"/> until read.</summary>
    public IReadOnlyList<string>? Regions { get; set; }

    /// <summary>
    /// What the <b>Resident in</b> column reads when <see cref="Regions"/> is
    /// empty: no residency set (served in every region), or a residency with no
    /// region left in it (served in none).
    /// </summary>
    public string NoRegionText { get; set; } = TenancyFormat.NoResidency;

    /// <summary>The one-phrase quota use, or <see langword="null"/> until read.</summary>
    public string? Quota { get; set; }

    /// <summary>The number of installed apps, or <see langword="null"/> when it cannot be read here.</summary>
    public int? Apps { get; set; }

    /// <summary>Whether the per-tenant reads have finished, successfully or not.</summary>
    public bool IsSettled { get; set; }
}
