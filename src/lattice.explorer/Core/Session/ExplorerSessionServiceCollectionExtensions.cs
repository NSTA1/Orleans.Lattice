using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Navigation;

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
    /// <para>
    /// It also registers the shell's state model: the declared preference
    /// contract (<see cref="IExplorerShellPreferences"/> over
    /// <see cref="IExplorerPreferenceCatalog"/> and
    /// <see cref="IExplorerPreferenceScopeProvider"/>) and the route model
    /// (<see cref="IExplorerShellRouter"/>). The two halves are registered
    /// together deliberately: they are one contract split by lifetime - the route
    /// carries where you are, the preferences where you were last time - and a
    /// head that got one without the other would have a shell that either cannot
    /// deep link or cannot remember.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerSession(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IUiSessionStore, UiSessionStore>();
        services.TryAddScoped<IUiPreferenceBackingStore, InMemoryUiPreferenceBackingStore>();
        services.TryAddScoped<IUiPreferenceStore, UiPreferenceStore>();

        // Key declarations are statements about the application, so the catalog
        // is a singleton; what is remembered under them is per session.
        services.TryAddSingleton<IExplorerPreferenceCatalog, ExplorerPreferenceCatalog>();

        // Both identity sources are optional: a head or a test that registers the
        // stores without a sign-in or a configured connection still gets a working
        // contract, scoped to the signed-out, unconfigured identity.
        services.TryAddScoped<IExplorerPreferenceScopeProvider>(provider =>
            new ExplorerPreferenceScopeProvider(
                provider.GetService<IExplorerAuthSession>(),
                provider.GetService<IExplorerSession>()));

        services.TryAddScoped<IExplorerShellPreferences, ExplorerShellPreferences>();

        return services.AddExplorerNavigation();
    }
}

