namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request carrying a tenant id and a single subject id, shared by the two
/// mutating tenant access-administration RPCs (<c>AddTenantAdminSubject</c> and
/// <c>RemoveTenantAdminSubject</c>). Both take the same shape because both are
/// single-subject membership deltas rather than declarative set replacements: a
/// tenant's admin-subject set is the tenant-admin capability itself, so granting
/// or revoking one subject must never be able to silently drop another that a
/// concurrent caller added.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminSubjectRequest)]
[Immutable]
public sealed record TenantAdminSubjectRequest
{
    /// <summary>The tenant id the call targets.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject id to grant or revoke tenant-admin authority for.</summary>
    [Id(1)] public required string SubjectId { get; init; }
}
