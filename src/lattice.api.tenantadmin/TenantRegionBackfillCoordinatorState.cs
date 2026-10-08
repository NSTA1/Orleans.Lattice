namespace Orleans.Lattice.Api.TenantAdmin;

[GenerateSerializer]
[Alias(TenantAdminTypeAliases.TenantRegionBackfillCoordinatorState)]
internal sealed class TenantRegionBackfillCoordinatorState
{
    [Id(0)]
    public bool InProgress { get; set; }

    [Id(1)]
    public string? SourceClusterId { get; set; }
}
