using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Registration helpers for the explorer's session-scoped UI state store.
/// </summary>
public static class ExplorerSessionServiceCollectionExtensions
{
    /// <summary>
    /// Registers the explorer's UI state stores: the in-memory
    /// <see cref="IUiSessionStore"/> for transient (session-lived) state and the
    /// durable <see cref="IUiPreferenceStore"/> for preferences, both scoped per
    /// session. A non-durable in-memory preference backing store is registered as
    /// a fallback; a host overrides <see cref="IUiPreferenceBackingStore"/> with a
    /// genuinely durable backing store.
    /// </summary>
    public static IServiceCollection AddExplorerSession(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IUiSessionStore, UiSessionStore>();
        services.TryAddScoped<IUiPreferenceBackingStore, InMemoryUiPreferenceBackingStore>();
        services.TryAddScoped<IUiPreferenceStore, UiPreferenceStore>();
        return services;
    }
}
