namespace Orleans.Lattice.Apps;

/// <summary>
/// The isolation context an app install was recorded in: the tenant that owns the
/// install and the cluster that recorded it. With tenancy off the tenant is
/// <see cref="TenantId.Default"/>, which yields the per-cluster behaviour. Recorded
/// from the first install so a later runtime acquisition provider can key on it
/// without a schema migration.
/// </summary>
[GenerateSerializer, Alias(AppRegistryTypeAliases.AppIsolationContext), Immutable]
public sealed record AppIsolationContext
{
    /// <summary>The tenant that owns the install.</summary>
    [Id(0)] public required TenantId Tenant { get; init; }

    /// <summary>The Orleans cluster id of the cluster that recorded the install.</summary>
    [Id(1)] public required string ClusterId { get; init; }
}
