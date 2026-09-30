using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    static partial void AddReplication(IServiceCollection services)
    {
        services.TryAddSingleton(new ReplicationOptions());
        services.TryAddScoped<ReplicationDataSource>();
        services.TryAddScoped<ReplicationCompletionSource>();
        services.TryAddScoped<IReplicationPageVisibility, JsReplicationPageVisibility>();
        services.AddExplorerArea<ReplicationArea>();
    }
}
