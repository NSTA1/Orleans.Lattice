using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    static partial void AddSchema(IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddScoped<SchemaFacades>();
        services.TryAddScoped<SchemaAccess>();
        services.TryAddScoped<SchemaTreeCatalog>();
        services.TryAddScoped<SchemaDirectory>();
        services.TryAddScoped<SchemaComplianceLedger>();
        services.TryAddScoped<SchemaOperations>();
        services.TryAddScoped<SchemaCommandSignals>();
        services.TryAddScoped<SchemaCompletionSource>();
        services.AddExplorerArea<SchemaArea>();
    }
}
