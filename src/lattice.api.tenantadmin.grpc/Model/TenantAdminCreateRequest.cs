namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the tenant-administration <c>CreateTenant</c> control-API RPC,
/// carrying the tenant id to create and the tenant-admin subjects to seed onto it.
/// Unlike the other lifecycle RPCs (which share
/// <see cref="TenantAdminTenantRequest"/>), create carries the admin-subject set
/// that decides who can subsequently see the new tenant, so it has its own request
/// message.
/// </summary>
/// <remarks>
/// An omitted or empty <see cref="AdminSubjects"/> asks the server to seed the
/// calling subject, so a create followed by a read-back works out of the box; a
/// non-empty set overrides that default outright.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminCreateRequest)]
[Immutable]
public sealed record TenantAdminCreateRequest
{
    /// <summary>The tenant id the call creates.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// The tenant-admin subject ids to seed onto the new tenant, or empty to seed
    /// the calling subject.
    /// </summary>
    [Id(1)] public IReadOnlyList<string> AdminSubjects { get; init; } = [];
}
