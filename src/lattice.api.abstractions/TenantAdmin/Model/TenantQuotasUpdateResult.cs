namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of authoring a tenant's resource quotas through
/// <see cref="ILatticeTenantAdmin.SetTenantQuotasAsync"/>. Reports the tenant id
/// whose quotas were set and the quotas now in effect for it, so a caller can
/// confirm the applied allocation without a follow-up read.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantQuotasUpdateResult)]
[Immutable]
public sealed record TenantQuotasUpdateResult
{
    /// <summary>The tenant id whose quotas were authored.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The quotas now in effect for the tenant after the call.</summary>
    [Id(1)] public TenantQuotasDescriptor Quotas { get; init; }
}
