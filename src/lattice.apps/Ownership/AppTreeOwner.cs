namespace Orleans.Lattice.Apps;

/// <summary>
/// The identity that owns app trees: the install's tenant, slug and publisher. The publisher comes
/// from the provenance the registry records for the install (the provenance the app source vouches
/// for), never from the manifest's self-declared provenance, so a different publisher reusing a slug
/// is a different owner.
/// </summary>
/// <param name="Tenant">The owning tenant.</param>
/// <param name="Slug">The owning app slug.</param>
/// <param name="Publisher">The owning publisher.</param>
internal readonly record struct AppTreeOwner(TenantId Tenant, AppSlug Slug, string Publisher)
{
    /// <summary>The owner identity of an install record.</summary>
    /// <param name="record">The install record.</param>
    /// <returns>The owner identity.</returns>
    public static AppTreeOwner Of(AppRegistryRecord record) =>
        new(record.Tenant, record.Slug, record.Provenance?.Publisher ?? string.Empty);
}
