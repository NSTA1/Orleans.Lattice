namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request identifying one cross-tenant grant by the two tenants party to
/// it and the scope it covers, shared by the three RPCs that move a grant along
/// its lifecycle (<c>ApproveCrossTenantGrant</c>,
/// <c>RejectCrossTenantGrant</c> and <c>RevokeCrossTenantGrant</c>). They take
/// the same shape because all three address an existing agreement rather than
/// define one; the terms live on the grant already.
/// </summary>
/// <remarks>
/// The grant's own composite identity is deliberately <em>not</em> the wire key.
/// Naming the two tenants and the scope keeps the request self-describing and
/// lets the server derive the identity through the grant type's own rule, so a
/// client can never hand back a malformed or hand-assembled id.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGrantRequest)]
[Immutable]
public sealed record TenantAdminGrantRequest
{
    /// <summary>The tenant that offered the grant, and whose record holds it.</summary>
    [Id(0)] public required string GranterTenantId { get; init; }

    /// <summary>The tenant the grant was offered to.</summary>
    [Id(1)] public required string GranteeTenantId { get; init; }

    /// <summary>The scope of the granting tenant's data the grant covers.</summary>
    [Id(2)] public required string Scope { get; init; }
}
