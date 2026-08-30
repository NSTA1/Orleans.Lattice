namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The read-only projection of a tenant's live tenant-admin subject set: the
/// tenant id and the subject ids that currently hold tenant-admin authority over
/// it, in ordinal order. Membership of this set <em>is</em> the tenant-admin
/// capability, so a subject listed here can administer the tenant's own surfaces
/// (region residency, and this membership set itself).
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantAdminSubjectReport)]
[Immutable]
public sealed record TenantAdminSubjectReport
{
    /// <summary>The tenant id the report describes.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// The live tenant-admin subject ids, in ordinal order. Never <c>null</c>;
    /// empty when the tenant carries no admin subjects (a deliberately
    /// subject-less tenant only a platform operator can reach).
    /// </summary>
    [Id(1)] public required IReadOnlyList<string> Subjects { get; init; }
}
