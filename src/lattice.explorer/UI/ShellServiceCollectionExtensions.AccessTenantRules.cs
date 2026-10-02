using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// The tenant Rules pages' own services (issue #4163): the tenant's trees a
    /// tenant rule may govern, which the rule editor and the layer-aware Explain
    /// complete against. The catalogue they read is resolved on first use, so a
    /// head without the Data catalogue still builds and its tree field is free text.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessRules(IServiceCollection services) =>
        services.TryAddScoped(provider => new TenantTreeSuggestionSource(
            provider,
            () => provider.GetService<ExplorerNavigator>()?.Current?.Tenant));
}
