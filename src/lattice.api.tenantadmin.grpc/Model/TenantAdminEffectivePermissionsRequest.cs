namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the <c>GetTenantEffectivePermissions</c> RPC: the rules of both
/// layers that apply to a subject on a tenant's trees, optionally narrowed to one
/// tree.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminEffectivePermissionsRequest)]
[Immutable]
public sealed record TenantAdminEffectivePermissionsRequest
{
    /// <summary>The tenant id whose trees to resolve permissions on.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject to resolve permissions for, read per <see cref="SubjectKind"/>.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The tenant-local tree name to narrow to, or <see langword="null"/> for every tree of the tenant.</summary>
    [Id(2)] public string? TreeName { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(3)] public TenantSubjectKind SubjectKind { get; init; }
}
