using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Shell.Areas.Replication;
using Orleans.Lattice.Explorer.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Shell;

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
