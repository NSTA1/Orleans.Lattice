namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of adding or removing a tenant group member or a tenant member-set
/// entry: the tenant, the group (when the call targeted one), the subject the
/// call added or removed, and whether it actually wrote. Every such mutation is
/// idempotent, so <see cref="Changed"/> reports <see langword="false"/> when the
/// entry was already present (on add) or already absent (on remove).
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantMembershipChangeResult)]
[Immutable]
public sealed record TenantMembershipChangeResult
{
    /// <summary>The tenant id the call targeted.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// The local name of the group whose direct members the call changed, or
    /// <see langword="null"/> when the call changed the tenant's member set.
    /// </summary>
    [Id(1)] public string? GroupName { get; init; }

    /// <summary>The id of the subject the call added or removed.</summary>
    [Id(2)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(3)] public TenantSubjectKind SubjectKind { get; init; }

    /// <summary><see langword="true"/> when the call wrote; <see langword="false"/> for an idempotent no-op.</summary>
    [Id(4)] public bool Changed { get; init; }
}
