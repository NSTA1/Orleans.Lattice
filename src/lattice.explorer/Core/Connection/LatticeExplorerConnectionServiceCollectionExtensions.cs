using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// Registration helpers for the explorer's state-API connection.
/// </summary>
public static class LatticeExplorerConnectionServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="ILatticeStateConnection"/> as a scoped service so
    /// each Blazor circuit (and each DI scope) gets its own cluster connection
    /// keyed on that scope's credential, rather than sharing a process-global
    /// connection across every operator. Every cluster read in a scope resolves
    /// and flows through that scope's connection. The endpoint is supplied later
    /// via <see cref="ILatticeStateConnection.ConfigureAsync"/>. The connection is
    /// <see cref="IAsyncDisposable"/>, so its gRPC channel is torn down when the
    /// owning scope ends.
    /// </summary>
    public static IServiceCollection AddLatticeStateConnection(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<ILatticeStateConnection, LatticeStateConnection>();
        return services;
    }
}
