using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Data area and the Core state-API readers it reads through.
    /// Every registration is a <c>TryAdd</c>, so a head's own reader or a test
    /// double registered first is kept, and the readers resolve the circuit's
    /// state connection only when the area is first used.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddData(IServiceCollection services)
    {
        services.AddExplorerCatalog();
        services.AddExplorerData();
        services.AddExplorerDeadLetter();
        services.AddExplorerMetrics();

        services.TryAddScoped<DataDirectory>();
        services.TryAddScoped<DataCompletionSource>();
        services.TryAddScoped<DataAdminGate>();
        services.AddExplorerArea<DataArea>();
    }
}
