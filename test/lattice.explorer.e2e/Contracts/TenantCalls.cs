using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>The tenancy facade calls the Fabrikam contracts share.</summary>
public static class TenantCalls
{
    /// <summary>A tenant's admin subjects, or none when the tenant does not exist.</summary>
    /// <param name="tenants">The TenantAdmin API.</param>
    /// <param name="tenant">The tenant.</param>
    public static async Task<IReadOnlyList<string>> ListTenantAdminSubjectsTolerantAsync(this LatticeTenantAdminApiGrpcClient tenants, string tenant)
    {
        try
        {
            return (await tenants.ListTenantAdminSubjectsAsync(tenant)).Subjects;
        }
        catch (RpcException ex) when (!ContractWait.IsTransient(ex.StatusCode))
        {
            return [];
        }
    }

    /// <summary>Whether a tenant exists.</summary>
    /// <param name="tenants">The TenantAdmin API.</param>
    /// <param name="tenant">The tenant.</param>
    public static async Task<bool> TenantExistsAsync(this LatticeTenantAdminApiGrpcClient tenants, string tenant)
    {
        try
        {
            await tenants.GetTenantRegionStatusAsync(tenant);
            return true;
        }
        catch (RpcException ex) when (!ContractWait.IsTransient(ex.StatusCode))
        {
            return false;
        }
    }

    /// <summary>Every grant <paramref name="tenant"/> issued.</summary>
    /// <param name="tenants">The TenantAdmin API.</param>
    /// <param name="tenant">The granting tenant.</param>
    public static async Task<IReadOnlyList<TenantGrantDescriptor>> IssuedGrantsAsync(this LatticeTenantAdminApiGrpcClient tenants, string tenant) =>
        (await tenants.ListCrossTenantGrantsAsync(tenant)).Issued;
}
