using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Extension methods that register the <c>Orleans.Lattice.Apps</c> add-on on a silo: the app
/// registry and its compiled snapshot, the <see cref="IAppSource"/> seam with its in-image
/// default, the <see cref="IAppActivationPipeline"/>, the background startup reconcile, and the
/// configuration-time replication and per-tree option intent of in-image apps.
/// </summary>
public static partial class LatticeAppsServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Apps</c> add-on to the silo. See
    /// <see cref="AddLatticeApps(IServiceCollection, Action{LatticeAppsOptions})"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeAppsOptions"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static ISiloBuilder AddLatticeApps(this ISiloBuilder builder, Action<LatticeAppsOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeApps(configure);
        return builder;
    }

    /// <summary>
    /// Adds the <c>Orleans.Lattice.Apps</c> add-on: the <see cref="IAppRegistry"/> over the
    /// reserved <c>sys-app-registry</c> tree, the compiled <see cref="IAppRegistryProjection"/>
    /// (refreshed off the change feed), the <see cref="IAppSource"/> seam defaulting to
    /// <see cref="InImageAppSource"/>, the <see cref="IAppActivationPipeline"/>, and a background
    /// reconcile of every enabled app at silo start.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Must be called after <c>AddLattice(...)</c>. App activation additionally needs
    /// <c>AddLatticeMembership(...)</c> and <c>AddLatticeAuth(...)</c>; registering apps without
    /// them is allowed, but activation then fails closed with a diagnostic naming the App to Auth
    /// to Membership chain instead of activating into a state where no app rule can match.
    /// Activation problems never fail silo startup.
    /// </para>
    /// <para>
    /// A repeat call layers any supplied <paramref name="configure"/> delegate but performs the
    /// structural wiring once. Register apps shipped in the image with
    /// <see cref="AddLatticeApp(IServiceCollection, string, Assembly, string)"/>.
    /// </para>
    /// </remarks>
    /// <param name="services">The silo service collection.</param>
    /// <param name="configure">Optional delegate that populates <see cref="LatticeAppsOptions"/>.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLattice(...)</c> was not called first.</exception>
    public static IServiceCollection AddLatticeApps(this IServiceCollection services, Action<LatticeAppsOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Ordering guard, mirroring the other add-ons: AddLattice registers the core options
        // validator; without it there is no tree registry for the app registry to dogfood.
        if (!services.Any(d => d.ServiceType == typeof(IValidateOptions<LatticeOptions>)))
        {
            throw new InvalidOperationException(
                "AddLatticeApps() must be called after AddLattice(). Register the core lattice " +
                "(siloBuilder.AddLattice(...)) before adding apps.");
        }

        var alreadyRegistered = services.Any(d => d.ServiceType == typeof(AppsRegistrationMarker));
        if (configure is not null)
        {
            services.Configure(configure);
        }

        if (alreadyRegistered)
        {
            return services;
        }

        services.AddSingleton<AppsRegistrationMarker>();
        services.AddOptions<LatticeAppsOptions>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LatticeAppsOptions>, LatticeAppsOptionsValidator>());

        // Registry.
        services.TryAddSingleton<IAppRegistryStore, LatticeAppRegistryStore>();
        services.TryAddSingleton<AppInstallAuthorizer>();
        services.TryAddSingleton<IAppRegistry, AppRegistry>();

        // The compiled snapshot maintainer is one singleton serving both as the change-feed
        // observer and the projection, so a registry write refreshes the snapshot readers see.
        // AddSingleton<IMutationObserver> is not idempotent under TryAdd, hence the marker above.
        services.TryAddSingleton<CompiledAppRegistrySnapshotMaintainer>();
        services.AddSingleton<IMutationObserver>(sp => sp.GetRequiredService<CompiledAppRegistrySnapshotMaintainer>());
        services.TryAddSingleton<IAppRegistryProjection>(sp => sp.GetRequiredService<CompiledAppRegistrySnapshotMaintainer>());

        // App source seam, defaulting to the apps registered in the image.
        services.AddOptions<InImageAppSourceOptions>();
        services.TryAddSingleton<IAppSource, InImageAppSource>();

        // Activation pipeline.
        services.TryAddSingleton<IAppActivationStatusStore, LatticeAppActivationStatusStore>();
        services.TryAddSingleton<IAppTreeProvisioner, LatticeAppTreeProvisioner>();
        services.TryAddSingleton<AppActivationEngine>();
        services.TryAddSingleton<AppActivationRunner>();
        services.TryAddSingleton<IAppActivationPipeline, AppActivationPipeline>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, AppStartupReconciler>());

        // Configuration-time intent of in-image apps. Inert when nothing resolves the options:
        // without the replication add-on LatticeReplicationOptions is never materialised.
        services.TryAddSingleton<InImageAppManifestCatalog>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IPostConfigureOptions<LatticeReplicationOptions>, AppReplicationIntentPostConfigure>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IConfigureOptions<LatticeOptions>, AppTreeOptionsConfigurator>());

        AddSubscriptionsCore(services);
        return services;
    }

    /// <summary>
    /// Registers an app shipped in the silo image with the in-image app source: its manifest is
    /// the embedded resource <paramref name="manifestResourceName"/> of <paramref name="assembly"/>.
    /// Registering makes the app installable; it does not install or enable it. See
    /// <see cref="AddLatticeApp(IServiceCollection, string, Assembly, string)"/>.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="slug">The app slug the manifest must declare.</param>
    /// <param name="assembly">The assembly carrying the manifest resource.</param>
    /// <param name="manifestResourceName">The manifest's embedded resource name.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <c>null</c>.</exception>
    /// <exception cref="FormatException"><paramref name="slug"/> is not a valid app slug.</exception>
    public static ISiloBuilder AddLatticeApp(this ISiloBuilder builder, string slug, Assembly assembly, string manifestResourceName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeApp(slug, assembly, manifestResourceName);
        return builder;
    }

    /// <summary>
    /// Registers an app shipped in the silo image with the in-image app source: its manifest is
    /// the embedded resource <paramref name="manifestResourceName"/> of <paramref name="assembly"/>.
    /// Registering makes the app installable and makes its declared replication intent and
    /// per-tree soft-delete windows known at configuration time; it does not install or enable
    /// it. A manifest that fails to load or validate fails only that app's activation.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    /// <param name="slug">The app slug the manifest must declare.</param>
    /// <param name="assembly">The assembly carrying the manifest resource.</param>
    /// <param name="manifestResourceName">The manifest's embedded resource name.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException">An argument is <c>null</c>.</exception>
    /// <exception cref="FormatException"><paramref name="slug"/> is not a valid app slug.</exception>
    public static IServiceCollection AddLatticeApp(this IServiceCollection services, string slug, Assembly assembly, string manifestResourceName)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(slug);
        ArgumentNullException.ThrowIfNull(assembly);
        ArgumentNullException.ThrowIfNull(manifestResourceName);
        var registration = new InImageAppRegistration(AppSlug.Parse(slug), assembly, manifestResourceName);
        services.Configure<InImageAppSourceOptions>(options => options.Registrations.Add(registration));
        return services;
    }

    /// <summary>
    /// The subscription wiring hook, implemented by the subscriptions partial of this class.
    /// Called once, from the structural wiring of <see cref="AddLatticeApps(IServiceCollection, Action{LatticeAppsOptions})"/>.
    /// </summary>
    static partial void AddSubscriptionsCore(IServiceCollection services);

    /// <summary>Marks that the structural wiring of <c>AddLatticeApps</c> has run.</summary>
    private sealed class AppsRegistrationMarker;
}
