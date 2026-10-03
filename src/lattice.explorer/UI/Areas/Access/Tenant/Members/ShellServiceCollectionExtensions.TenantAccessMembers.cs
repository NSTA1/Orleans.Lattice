using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Members;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// The tenant Members page's own services: the reader of the tenant's
    /// administrator entries, which the page lists read-only as implicit members.
    /// The facade it reads resolves lazily and optionally, so a head that serves
    /// none still builds.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessMembers(IServiceCollection services) =>
        services.TryAddScoped(provider => new TenantAdminSubjects(provider));
}
