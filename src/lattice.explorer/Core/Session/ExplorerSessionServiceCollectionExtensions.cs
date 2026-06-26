using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Registration helpers for the explorer's session-scoped UI state store.
/// </summary>
public static class ExplorerSessionServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IUiSessionStore"/> with a scoped lifetime so each
    /// user session keeps its own transient UI state.
    /// </summary>
    public static IServiceCollection AddExplorerSession(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IUiSessionStore, UiSessionStore>();
        return services;
    }
}
