namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the <c>OfferCrossTenantGrant</c> RPC: the two tenants party
/// to the agreement, the scope of the granting tenant's data it covers, and the
/// operations it will authorize once the grantee approves it. It carries the
/// terms, which is why it is a distinct message from the
/// <see cref="TenantAdminGrantRequest"/> the lifecycle transitions use.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGrantOfferRequest)]
[Immutable]
public sealed record TenantAdminGrantOfferRequest
{
    /// <summary>The tenant offering a scope of its own data.</summary>
    [Id(0)] public required string GranterTenantId { get; init; }

    /// <summary>The tenant the grant is offered to.</summary>
    [Id(1)] public required string GranteeTenantId { get; init; }

    /// <summary>The scope of the granting tenant's data the grant covers.</summary>
    [Id(2)] public required string Scope { get; init; }

    /// <summary>The operations the grant will authorize on the scope once it is active.</summary>
    [Id(3)] public TenantGrantAccess Operations { get; init; }
}
