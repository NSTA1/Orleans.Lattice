namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the <c>ExplainTenantAccess</c> RPC: whether a subject may
/// perform an operation on one of a tenant's trees, or on one key of it.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminExplainRequest)]
[Immutable]
public sealed record TenantAdminExplainRequest
{
    /// <summary>The tenant id that owns the tree.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The subject to explain the decision for, read per <see cref="SubjectKind"/>.</summary>
    [Id(1)] public required string SubjectId { get; init; }

    /// <summary>The tenant-local tree name.</summary>
    [Id(2)] public required string TreeName { get; init; }

    /// <summary>The key to evaluate, or <see langword="null"/> to evaluate the whole tree.</summary>
    [Id(3)] public string? Key { get; init; }

    /// <summary>The operation to evaluate.</summary>
    [Id(4)] public LatticeOperation Operation { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(5)] public TenantSubjectKind SubjectKind { get; init; }
}
