using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps;

public static partial class LatticeAppsServiceCollectionExtensions
{
    /// <summary>
    /// Adds an app catalogue source to the silo. See
    /// <see cref="AddLatticeAppSource{TSource}(IServiceCollection)"/>.
    /// </summary>
    /// <typeparam name="TSource">The source type.</typeparam>
    /// <param name="builder">The silo builder.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    public static ISiloBuilder AddLatticeAppSource<TSource>(this ISiloBuilder builder)
        where TSource : class, IAppCatalogSource
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeAppSource<TSource>();
        return builder;
    }

    /// <summary>
    /// Adds a named app catalogue source, created once from the service provider (constructor injection),
    /// to the <see cref="AppSourceSet"/> that <c>AddLatticeApps()</c> composes alongside the in-image source.
    /// Sources compose in registration order. Registering the same type twice registers it once.
    /// </summary>
    /// <remarks>
    /// Two sources that report the same <see cref="AppSourceDescriptor.Key"/> never fail silo startup: the
    /// set records the duplicate and every resolution reports <see cref="AppSourceStatus.SourceMisconfigured"/>,
    /// so the problem surfaces at the activation of each app.
    /// </remarks>
    /// <typeparam name="TSource">The source type.</typeparam>
    /// <param name="services">The silo service collection.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeAppSource<TSource>(this IServiceCollection services)
        where TSource : class, IAppCatalogSource
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IAppCatalogSource, TSource>());
        return services;
    }

    /// <summary>
    /// Adds an app catalogue source instance to the silo. See
    /// <see cref="AddLatticeAppSource(IServiceCollection, IAppCatalogSource)"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="instance">The source instance.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="instance"/> is <c>null</c>.</exception>
    public static ISiloBuilder AddLatticeAppSource(this ISiloBuilder builder, IAppCatalogSource instance)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeAppSource(instance);
        return builder;
    }

    /// <summary>
    /// Adds a named app catalogue source instance to the <see cref="AppSourceSet"/> that
    /// <c>AddLatticeApps()</c> composes alongside the in-image source. Sources compose in registration order;
    /// every call adds the instance, so adding it twice is reported as a duplicate source key when an app
    /// resolves, never at silo startup.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    /// <param name="instance">The source instance.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="instance"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeAppSource(this IServiceCollection services, IAppCatalogSource instance)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(instance);
        services.AddSingleton<IAppCatalogSource>(instance);
        return services;
    }

    /// <summary>
    /// Registers an app shipped in the silo image whose UI bundle assets use an explicit embedded-resource
    /// prefix. See <see cref="AddLatticeApp(IServiceCollection, string, Assembly, string, string)"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="slug">The app slug the manifest must declare.</param>
    /// <param name="assembly">The assembly carrying the manifest and asset resources.</param>
    /// <param name="manifestResourceName">The manifest's embedded resource name.</param>
    /// <param name="assetResourcePrefix">The embedded-resource name prefix of the app's UI bundle assets.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <c>null</c>.</exception>
    /// <exception cref="FormatException"><paramref name="slug"/> is not a valid app slug.</exception>
    public static ISiloBuilder AddLatticeApp(
        this ISiloBuilder builder,
        string slug,
        Assembly assembly,
        string manifestResourceName,
        string assetResourcePrefix)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeApp(slug, assembly, manifestResourceName, assetResourcePrefix);
        return builder;
    }

    /// <summary>
    /// Registers an app shipped in the silo image with the in-image app source, exactly as
    /// <see cref="AddLatticeApp(IServiceCollection, string, Assembly, string)"/> does, but with an explicit
    /// embedded-resource prefix for its UI bundle assets: an asset at relative path <c>p</c> is read from the
    /// resource named <paramref name="assetResourcePrefix"/> followed by <c>p</c> with every <c>/</c> mapped to
    /// <c>.</c>. The prefix is used verbatim, so include its trailing separator (for example
    /// <c>Contoso.Notes.Bundle.</c>). Use this overload when the bundle's resource names do not follow the
    /// default <c>{manifestResourceNamespace}.ui.{path}</c> convention, for example because they are set with
    /// <c>LogicalName</c>.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    /// <param name="slug">The app slug the manifest must declare.</param>
    /// <param name="assembly">The assembly carrying the manifest and asset resources.</param>
    /// <param name="manifestResourceName">The manifest's embedded resource name.</param>
    /// <param name="assetResourcePrefix">The embedded-resource name prefix of the app's UI bundle assets.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <c>null</c>.</exception>
    /// <exception cref="FormatException"><paramref name="slug"/> is not a valid app slug.</exception>
    public static IServiceCollection AddLatticeApp(
        this IServiceCollection services,
        string slug,
        Assembly assembly,
        string manifestResourceName,
        string assetResourcePrefix)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(slug);
        ArgumentNullException.ThrowIfNull(assembly);
        ArgumentNullException.ThrowIfNull(manifestResourceName);
        ArgumentNullException.ThrowIfNull(assetResourcePrefix);
        var registration = new InImageAppRegistration(AppSlug.Parse(slug), assembly, manifestResourceName)
        {
            AssetResourcePrefix = assetResourcePrefix,
        };
        services.Configure<InImageAppSourceOptions>(options => options.Registrations.Add(registration));
        return services;
    }

    static partial void AddSourcesCore(IServiceCollection services)
    {
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IAppCatalogSource, InImageAppSource>(
                static provider => provider.GetRequiredService<InImageAppSource>()));
        services.TryAddSingleton(static provider => new AppSourceSet(provider.GetServices<IAppCatalogSource>()));
        services.TryAddSingleton<IAppSource>(static provider => provider.GetRequiredService<AppSourceSet>());
    }
}
