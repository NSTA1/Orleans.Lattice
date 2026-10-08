namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>Durable coordinator for one tenant's local-region backfill.</summary>
[Alias(TenantAdminTypeAliases.TenantRegionBackfillCoordinator)]
internal interface ITenantRegionBackfillCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>Starts or resumes backfill when the tenant region is pending.</summary>
    Task EnsureRunningAsync();
}
