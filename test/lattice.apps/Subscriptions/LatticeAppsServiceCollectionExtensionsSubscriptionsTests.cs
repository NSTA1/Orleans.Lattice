using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using static Orleans.Lattice.Apps.Tests.SubscriptionTestData;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsServiceCollectionExtensionsSubscriptionsTests
{
    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IAppRegistryProjection>(new FakeAppRegistryProjection());
        services.AddSingleton<IAppSource>(new FakeAppSource());
        services.AddSingleton(AppRegistryTestData.CreateLedger(new InMemoryAppRegistryStore()));
        return services;
    }

    [Test]
    public void AddLatticeAppSubscriptions_registers_the_router_as_a_mutation_observer_once()
    {
        var services = Services();

        LatticeAppsServiceCollectionExtensions.AddLatticeAppSubscriptions(services);
        LatticeAppsServiceCollectionExtensions.AddLatticeAppSubscriptions(services);
        using var provider = services.BuildServiceProvider();

        var observers = provider.GetServices<IMutationObserver>().ToArray();
        Assert.That(observers, Has.Length.EqualTo(1));
        Assert.That(observers[0], Is.SameAs(provider.GetRequiredService<AppSubscriptionRouter>()));
    }

    [Test]
    public void AddLatticeAppSubscriptionHandler_generic_constructs_the_handler_through_the_provider()
    {
        var services = Services();
        services.AddSingleton(new HandlerDependency());
        services.AddLatticeAppSubscriptionHandler<InjectedHandler>(Notes, "feed");
        LatticeAppsServiceCollectionExtensions.AddLatticeAppSubscriptions(services);
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<AppSubscriptionHandlerCatalog>();

        Assert.That(catalog.TryResolve(Notes, "feed", out var handler, out _), Is.True);
        Assert.That(((InjectedHandler)handler!).Dependency, Is.SameAs(provider.GetRequiredService<HandlerDependency>()));
    }

    [Test]
    public void AddLatticeAppSubscriptionHandler_factory_registers_the_supplied_handler()
    {
        var services = Services();
        var handler = new RecordingChangeFeedHandler();

        var returned = services.AddLatticeAppSubscriptionHandler(Notes, "feed", _ => handler);
        LatticeAppsServiceCollectionExtensions.AddLatticeAppSubscriptions(services);
        using var provider = services.BuildServiceProvider();

        Assert.That(returned, Is.SameAs(services));
        Assert.That(provider.GetRequiredService<AppSubscriptionHandlerCatalog>().TryResolve(Notes, "feed", out var resolved, out _), Is.True);
        Assert.That(resolved, Is.SameAs(handler));
    }

    [Test]
    public void AddLatticeAppSubscriptionHandler_validates_its_arguments()
    {
        var services = new ServiceCollection();
        Func<IServiceProvider, IAppChangeFeedHandler> factory = _ => new RecordingChangeFeedHandler();

        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppSubscriptionHandler(Notes, "feed", factory));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeAppSubscriptionHandler(Notes, null!, factory));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeAppSubscriptionHandler(Notes, "feed", null!));
        Assert.Throws<ArgumentException>(() => services.AddLatticeAppSubscriptionHandler(default, "feed", factory));
        Assert.Throws<ArgumentException>(() => services.AddLatticeAppSubscriptionHandler(Notes, string.Empty, factory));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeAppSubscriptionHandler<InjectedHandler>(Notes, null!));
        Assert.Throws<ArgumentNullException>(() => LatticeAppsServiceCollectionExtensions.AddLatticeAppSubscriptions(null!));
    }

    public sealed class HandlerDependency;

    public sealed class InjectedHandler(HandlerDependency dependency) : IAppChangeFeedHandler
    {
        public HandlerDependency Dependency { get; } = dependency;

        public Task HandleAsync(AppSubscriptionContext subscription, LatticeMutation mutation, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
}
