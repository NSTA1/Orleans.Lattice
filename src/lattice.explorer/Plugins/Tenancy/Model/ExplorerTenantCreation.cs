namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The outcome of registering a new tenant: its id, the lifecycle state it was
/// created in, and the admin subjects seeded onto it.
/// <para>
/// Tenant visibility on the read-only self-service surface resolves from
/// admin-subject membership, so a tenant created with no admin subjects would be
/// invisible to the caller that created it. Seeding is therefore never empty:
/// the server seeds the calling subject when none is supplied, and
/// <see cref="AdminSubjects"/> reports what it actually seeded.
/// </para>
/// </summary>
public sealed record ExplorerTenantCreation
{
    /// <summary>The created tenant's id.</summary>
    public required string TenantId { get; init; }

    /// <summary>The lifecycle state the tenant was created in.</summary>
    public ExplorerTenantLifecycle Status { get; init; }

    /// <summary>
    /// The admin-subject ids seeded onto the tenant. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<string> AdminSubjects { get; init; }
}
