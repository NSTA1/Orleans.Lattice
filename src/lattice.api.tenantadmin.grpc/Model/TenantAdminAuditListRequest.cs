using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the paged, read-only <c>ListTenantAudit</c> RPC, carrying the
/// tenant whose admin audit trail is read and the query to read it with.
/// </summary>
/// <remarks>
/// The query's <see cref="AdminAuditQuery.Scope"/> is ignored by the server's facade,
/// which always confines the read to <see cref="TenantId"/>'s own partition.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminAuditListRequest)]
[Immutable]
public sealed record TenantAdminAuditListRequest
{
    /// <summary>The tenant id whose admin audit trail is read.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The audit query (time window, filters, page size and continuation token).</summary>
    [Id(1)] public required AdminAuditQuery Query { get; init; }
}
