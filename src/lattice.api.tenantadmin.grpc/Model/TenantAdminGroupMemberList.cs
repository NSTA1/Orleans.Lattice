namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire response for the <c>ListTenantGroupMembers</c> RPC. A dedicated wrapper is
/// required because a bare <see cref="IReadOnlyList{T}"/> is not a reference-typed
/// gRPC message; the wrapper carries the group's direct members unchanged, in
/// ascending ordinal order of member id.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGroupMemberList)]
[Immutable]
public sealed record TenantAdminGroupMemberList
{
    /// <summary>The group's direct members; empty when it has none or does not exist.</summary>
    [Id(0)] public IReadOnlyList<TenantGroupMember> Members { get; init; } = Array.Empty<TenantGroupMember>();
}
