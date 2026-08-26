namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of an explicit tenant-creation request. A create either registers
/// a brand-new tenant (and this result reports its id and initial
/// <see cref="Status"/>) or fails closed with a
/// <see cref="TenantAlreadyExistsException"/>; unlike an idempotent upsert, a
/// create never silently reuses an existing tenant, so a returned result always
/// describes a tenant this call brought into existence.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantCreationResult)]
[Immutable]
public sealed record TenantCreationResult
{
    /// <summary>The tenant id that was created.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The lifecycle status the tenant was created in (always <see cref="TenantLifecycleStatus.Active"/>).</summary>
    [Id(1)] public TenantLifecycleStatus Status { get; init; }

    /// <summary>
    /// The tenant-admin subjects the create seeded onto the new tenant, in
    /// ordinal order. These are the subjects that can immediately see the tenant
    /// through the read-only self-service surface (list / get), which resolves
    /// visibility from admin-subject membership. Empty only when the create
    /// supplied no subjects <em>and</em> the calling subject could not be
    /// resolved (an anonymous or system-origin caller), in which case the new
    /// tenant is invisible to every caller until subjects are added.
    /// </summary>
    [Id(2)] public IReadOnlyList<string> AdminSubjects { get; init; } = [];
}
