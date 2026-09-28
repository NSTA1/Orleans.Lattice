using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsApiServiceCollectionExtensionsTests
{
    private static ServiceCollection EngineServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IAppRegistry>());
        services.AddSingleton(Substitute.For<IAppSource>());
        services.AddSingleton(Substitute.For<IAppActivationPipeline>());
        services.AddSingleton(Substitute.For<ILatticeAccessGate>());
        services.AddSingleton(Substitute.For<ITenantContextResolver>());
        return services;
    }

    [Test]
    public void AddLatticeAppsApi_registers_the_facade_as_the_control_singleton()
    {
        var services = EngineServices();

        services.AddLatticeAppsApi();
        services.AddLatticeAppsApi();

        Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeAppsControl)), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        var control = provider.GetRequiredService<ILatticeAppsControl>();
        Assert.That(control, Is.TypeOf<LatticeAppsControl>());
        Assert.That(provider.GetRequiredService<ILatticeAppsControl>(), Is.SameAs(control));
    }

    [Test]
    public void AddLatticeAppsApi_registers_the_catalog_and_workspace_facades_once()
    {
        var services = EngineServices();

        services.AddLatticeAppsApi();
        services.AddLatticeAppsApi();

        Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeAppCatalog)), Is.EqualTo(1));
        Assert.That(services.Count(d => d.ServiceType == typeof(ILatticeAppWorkspace)), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<ILatticeAppCatalog>(), Is.TypeOf<LatticeAppCatalog>());
        Assert.That(provider.GetRequiredService<ILatticeAppWorkspace>(), Is.TypeOf<LatticeAppWorkspace>());
        Assert.That(provider.GetRequiredService<AppRoleGrantEvaluator>().CanServe, Is.False, "no registry projection is registered here");
    }

    [Test]
    public void AddLatticeAppsApi_without_apps_add_on_throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddLatticeAppsApi());
        Assert.That(ex!.Message, Does.Contain("AddLatticeApps()"));
    }

    [Test]
    public void AddLatticeAppsApi_null_arguments_throw()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppsApi());
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeAppsApi());
    }

    [Test]
    public void AddLatticeAppsApi_silo_builder_overload_registers_on_its_services()
    {
        var services = EngineServices();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        Assert.That(builder.AddLatticeAppsApi(), Is.SameAs(builder));
        Assert.That(services.Any(d => d.ServiceType == typeof(ILatticeAppsControl)), Is.True);
    }
}
