namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The result of a tenant deletion. Reports the deleted tenant id and the number
/// of the tenant's trees that were cascaded (soft-deleted) as part of removing
/// it, so an operator can confirm the blast radius of the delete.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantDeletionResult)]
[Immutable]
public sealed record TenantDeletionResult
{
    /// <summary>The tenant id that was deleted.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>
    /// The number of the tenant's trees that were cascaded (soft-deleted) as part
    /// of the delete. Zero when the tenant owned no trees.
    /// </summary>
    [Id(1)] public int CascadedTreeCount { get; init; }
}
