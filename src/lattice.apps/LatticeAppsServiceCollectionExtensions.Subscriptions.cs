using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Apps;

/// <summary>Service registration for the installable-app runtime.</summary>
public static partial class LatticeAppsServiceCollectionExtensions
{
    /// <summary>
    /// Registers the change-feed handler for one manifest-declared subscription of one app. The
    /// handler is created once from the service provider (constructor injection) the first time the
    /// app's subscriptions are activated, and serves every tenant that enables the app. A subscription
    /// the manifest declares with no registered handler, or with more than one, fails that app's
    /// subscription activation.
    /// </summary>
    /// <typeparam name="THandler">The handler type.</typeparam>
    /// <param name="services">The silo service collection.</param>
    /// <param name="app">The app that declares the subscription.</param>
    /// <param name="subscriptionName">The manifest subscription name.</param>
    /// <returns><paramref name="services"/>, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="subscriptionName"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="app"/> is uninitialised or <paramref name="subscriptionName"/> is empty.</exception>
    public static IServiceCollection AddLatticeAppSubscriptionHandler<THandler>(
        this IServiceCollection services,
        AppSlug app,
        string subscriptionName)
        where THandler : class, IAppChangeFeedHandler =>
        AddLatticeAppSubscriptionHandler(
            services,
            app,
            subscriptionName,
            static provider => ActivatorUtilities.CreateInstance<THandler>(provider));

    /// <summary>
    /// Registers the change-feed handler for one manifest-declared subscription of one app, created
    /// once by <paramref name="factory"/> the first time the app's subscriptions are activated.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    /// <param name="app">The app that declares the subscription.</param>
    /// <param name="subscriptionName">The manifest subscription name.</param>
    /// <param name="factory">Creates the handler from the silo service provider.</param>
    /// <returns><paramref name="services"/>, for chaining.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/>, <paramref name="subscriptionName"/> or <paramref name="factory"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="app"/> is uninitialised or <paramref name="subscriptionName"/> is empty.</exception>
    public static IServiceCollection AddLatticeAppSubscriptionHandler(
        this IServiceCollection services,
        AppSlug app,
        string subscriptionName,
        Func<IServiceProvider, IAppChangeFeedHandler> factory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(subscriptionName);
        ArgumentNullException.ThrowIfNull(factory);
        if (app.Value is null)
            throw new ArgumentException("The app slug is uninitialised.", nameof(app));
        if (subscriptionName.Length == 0)
            throw new ArgumentException("The subscription name must not be empty.", nameof(subscriptionName));

        services.AddSingleton(new AppSubscriptionHandlerRegistration(app, subscriptionName, factory));
        return services;
    }

    // TEMPORARY: the declaring half of this hook lives in W1's (#2244)
    // LatticeAppsServiceCollectionExtensions.cs, which calls it from AddLatticeApps. Until W1 is
    // integrated this file declares it too so it compiles; remove this declaration at integration.
    static partial void AddSubscriptionsCore(IServiceCollection services);

    static partial void AddSubscriptionsCore(IServiceCollection services) => AddLatticeAppSubscriptions(services);

    /// <summary>
    /// Registers the change-feed subscription runtime: the handler catalog and the router, which is
    /// the <see cref="IMutationObserver"/> delivering committed mutations to app handlers. Idempotent.
    /// Requires <see cref="IAppRegistryProjection"/> and <see cref="IAppSource"/>, which the app
    /// activation registration supplies.
    /// </summary>
    /// <param name="services">The silo service collection.</param>
    /// <returns><paramref name="services"/>, for chaining.</returns>
    internal static IServiceCollection AddLatticeAppSubscriptions(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        foreach (var descriptor in services)
            if (descriptor.ServiceType == typeof(AppSubscriptionRouter))
                return services;

        services.AddSingleton(static provider => new AppSubscriptionHandlerCatalog(
            provider,
            provider.GetServices<AppSubscriptionHandlerRegistration>()));
        services.AddSingleton<AppSubscriptionRouter>();
        services.AddSingleton<IMutationObserver>(static provider => provider.GetRequiredService<AppSubscriptionRouter>());
        return services;
    }
}
