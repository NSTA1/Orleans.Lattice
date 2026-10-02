namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request naming one subject of a tenant, shared by the
/// <c>AddTenantMember</c>, <c>RemoveTenantMember</c> and
/// <c>ResolveTenantSubject</c> RPCs.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminMemberRequest)]
[Immutable]
public sealed record TenantAdminMemberRequest
{
    /// <summary>The tenant id the call targets.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject's id, read per <see cref="SubjectKind"/>.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(2)] public TenantSubjectKind SubjectKind { get; init; }
}
