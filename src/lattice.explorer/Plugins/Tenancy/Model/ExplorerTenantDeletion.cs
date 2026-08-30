namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The outcome of deleting a tenant, carrying how many of its trees the deletion
/// cascaded through. The count is what a confirmation prompt should show, since
/// the deletion is irreversible and takes the tenant's data with it.
/// </summary>
/// <param name="TenantId">The deleted tenant's id.</param>
/// <param name="CascadedTreeCount">The number of trees removed with the tenant.</param>
public readonly record struct ExplorerTenantDeletion(
    string TenantId,
    int CascadedTreeCount);
