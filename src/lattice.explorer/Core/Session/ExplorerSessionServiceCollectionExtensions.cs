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

        // Registered through an explicit factory rather than by implementation
        // type for the reason given at ExplorerPreferenceCatalog below: the type
        // has more than one public constructor, so registering it by type leaves
        // the choice to the container's greedy selection. Its longer constructor
        // is currently unsatisfiable (nothing registers a TimeSpan), so the
        // selection happens to be right today - but that is an accident of the
        // signature, not a property anyone declared, and it would flip silently
        // if a satisfiable parameter were ever added. Naming the constructor here
        // costs one line and removes the question.
        services.TryAddScoped<IUiPreferenceStore>(provider =>
            new UiPreferenceStore(provider.GetRequiredService<IUiPreferenceBackingStore>()));

        // Key declarations are statements about the application, so the catalog
        // is a singleton; what is remembered under them is per session.
        //
        // The factory is load-bearing, not a style choice. ExplorerPreferenceCatalog
        // has two public constructors - a parameterless one that seeds the shell's
        // declared keys, and one taking IEnumerable<ExplorerPreferenceKey> for a
        // caller that wants to seed explicitly. Registering it by implementation
        // type lets the container pick, and it picks the constructor with the most
        // satisfiable parameters; an IEnumerable<T> is ALWAYS satisfiable, because
        // the container synthesises an empty sequence for it. So the type
        // registration silently produced an EMPTY catalog, which made every member
        // of IExplorerShellPreferences throw "not a registered preference key" on a
        // real head while every unit test - each of which constructs the catalog
        // directly - passed. Name the constructor and the ambiguity disappears.
        services.TryAddSingleton<IExplorerPreferenceCatalog>(_ => new ExplorerPreferenceCatalog());

        // Both identity sources are optional: a head or a test that registers the
        // stores without a sign-in or a configured connection still gets a working
        // contract, scoped to the signed-out, unconfigured identity.
        services.TryAddScoped<IExplorerPreferenceScopeProvider>(provider =>
            new ExplorerPreferenceScopeProvider(
                provider.GetService<IExplorerAuthSession>(),
                provider.GetService<IExplorerSession>()));

        services.TryAddScoped<IExplorerShellPreferences, ExplorerShellPreferences>();

        // Scoped, so the single restore opportunity it records belongs to the
        // session rather than to whichever page happens to claim it.
        services.TryAddScoped<IExplorerShellEntryGate, ExplorerShellEntryGate>();

        return services.AddExplorerNavigation();
    }
}

