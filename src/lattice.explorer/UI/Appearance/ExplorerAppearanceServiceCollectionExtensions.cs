using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// Registration for the Explorer's appearance state: the three preference keys,
/// the state service, and the applier that puts the resolved appearance on the
/// document.
/// </summary>
public static class ExplorerAppearanceServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IExplorerAppearance"/> and its applier, and declares
    /// the appearance keys on the shell's preference catalog.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Call after <c>AddExplorerSession()</c>, whose catalog the keys are declared
    /// on. Declaring them here rather than in
    /// <see cref="ExplorerPreferenceKeys"/> is what lets the reset-view page
    /// disclose and clear them without being edited, and is the extension point
    /// the contract was built to have.
    /// </para>
    /// <para>
    /// A head whose platform carries a theme of its own also registers an
    /// <see cref="IExplorerHostTheme"/>; a head that does not - the web head,
    /// where the browser answers <c>prefers-color-scheme</c> in the document
    /// itself - registers nothing, and "follow the system" is left for the
    /// document to resolve.
    /// </para>
    /// <para>
    /// Calling this more than once is harmless: the service registrations use
    /// <c>TryAdd</c>, and the keys are registered by reference, so re-declaring
    /// the same instances is a no-op rather than a duplicate-name failure.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <returns>The same collection, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerAppearance(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IExplorerAppearanceApplier, ExplorerAppearanceApplier>();

        // Constructed by hand rather than by convention because the host theme is
        // genuinely optional, and GetService (not GetRequiredService) is what
        // makes "this head has no opinion" a supported answer.
        services.TryAddScoped<IExplorerAppearance>(provider => new ExplorerAppearance(
            provider.GetRequiredService<IExplorerShellPreferences>(),
            provider.GetRequiredService<IExplorerAppearanceApplier>(),
            provider.GetService<IExplorerHostTheme>()));

        DeclareAppearanceKeys(services);

        return services;
    }

    /// <summary>
    /// Declares the appearance keys on whatever catalog the container resolves,
    /// by wrapping its registration rather than replacing it.
    /// </summary>
    /// <remarks>
    /// The shell registers the catalog with <c>TryAdd</c>, so appending a second
    /// registration would silently shadow a head's own catalog, and dropping the
    /// existing one would discard it. Wrapping keeps whichever catalog the head
    /// chose and simply declares three more keys on it.
    /// </remarks>
    private static void DeclareAppearanceKeys(IServiceCollection services)
    {
        // A head that reaches the appearance feature without the session stores
        // still gets a working contract rather than a resolution failure.
        services.TryAddSingleton<IExplorerPreferenceCatalog, ExplorerPreferenceCatalog>();

        for (var i = services.Count - 1; i >= 0; i--)
        {
            var existing = services[i];

            if (existing.IsKeyedService || existing.ServiceType != typeof(IExplorerPreferenceCatalog))
            {
                continue;
            }

            if (existing.ImplementationInstance is IExplorerPreferenceCatalog instance)
            {
                services[i] = Wrap(_ => instance);
                return;
            }

            if (existing.ImplementationFactory is { } factory)
            {
                services[i] = Wrap(provider => (IExplorerPreferenceCatalog)factory(provider));
                return;
            }

            // Resolved through the container rather than activated here, so the
            // catalog is constructed by exactly the rules that built it before
            // this feature wrapped it. Activating it directly would pick a
            // different constructor - ExplorerPreferenceCatalog has a seeded
            // overload, and an empty seed would silently discard the shell's own
            // keys.
            var implementation = existing.ImplementationType!;
            services[i] = Wrap(provider => (IExplorerPreferenceCatalog)provider.GetRequiredService(implementation));
            services.TryAddSingleton(implementation);
            return;
        }
    }

    private static ServiceDescriptor Wrap(Func<IServiceProvider, IExplorerPreferenceCatalog> inner) =>
        new(typeof(IExplorerPreferenceCatalog), provider => Declare(inner(provider)), ServiceLifetime.Singleton);

    private static IExplorerPreferenceCatalog Declare(IExplorerPreferenceCatalog catalog)
    {
        var keys = ExplorerAppearancePreferenceKeys.All;

        for (var i = 0; i < keys.Count; i++)
        {
            catalog.Register(keys[i]);
        }

        return catalog;
    }
}
