namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The policy layer an authorization rule belongs to, as seen by a tenant
/// administrator. Operator rules form the <see cref="Platform"/> layer and are
/// final when they match; tenant-tier rules form the <see cref="Tenant"/> layer,
/// which is consulted only when no platform rule matches.
/// </summary>
/// <remarks>
/// The zero value is <see cref="Platform"/>, the read-only layer, so a reading
/// that never set the layer fails closed to "not editable by the tenant" rather
/// than to "the tenant's own rule".
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRuleLayer)]
public enum TenantRuleLayer
{
    /// <summary>An operator rule. Read-only to a tenant administrator, and final when it matches.</summary>
    Platform = 0,

    /// <summary>A tenant-tier rule, authored by the tenant's own administrators beneath the platform layer.</summary>
    Tenant = 1,
}
