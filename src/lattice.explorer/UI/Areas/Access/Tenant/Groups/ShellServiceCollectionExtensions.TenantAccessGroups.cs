using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// The tenant Groups pages' own services: the Explorer's existing history reader,
    /// through which a tenant group's history is shown (D19). It reads through the
    /// circuit's state connection, which the Data area registers; a head without one
    /// leaves the history unavailable rather than failing the page.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddTenantAccessGroups(IServiceCollection services) => services.AddExplorerHistory();
}
