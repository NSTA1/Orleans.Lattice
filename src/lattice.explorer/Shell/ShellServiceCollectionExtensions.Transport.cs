using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Shell.Transport;

namespace Orleans.Lattice.Explorer.Shell;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the per-circuit transport channel and one scoped adapter per
    /// facade the Shell consumes (T1, issue #3830). The app bridge client joins
    /// here, once it lands, through
    /// <see cref="ShellTransportServiceCollectionExtensions.AddShellTransportClient{TFacade}"/>.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddTransport(IServiceCollection services)
    {
        services.AddShellTransport();
    }
}
