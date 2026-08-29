using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class ExplorerTenantServiceCollectionExtensionsTests
{
    private static ServiceProvider BuildProvider(bool addTenantView)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerPluginAccessStore>());
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());
        if (addTenantView)
        {
            services.AddExplorerTenantView();
        }

        return services.BuildServiceProvider();
    }

    [Test]
    public void AddExplorerTenantView_registersActiveView()
    {
        using var provider = BuildProvider(addTenantView: true);
        using var scope = provider.CreateScope();

        var view = scope.ServiceProvider.GetRequiredService<IExplorerTenantView>();

        Assert.That(view, Is.InstanceOf<ExplorerTenantView>());
        Assert.That(view.IsActive, Is.True);
    }

    [Test]
    public async Task AddExplorerTenantView_registersContextAndAFailClosedGateByDefault()
    {
        using var provider = BuildProvider(addTenantView: true);
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerTenantContext>(),
            Is.InstanceOf<ExplorerTenantContext>());

        // A real platform-operator signal is a probed decision owned by the
        // plugin that performs the probe, so the navigation core's own default
        // admits nobody.
        var gate = scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>();
        Assert.That(gate, Is.InstanceOf<DeniedExplorerTenantOperatorGate>());
        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public void An_administrative_surface_registered_first_supplies_the_real_operator_gate()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());
        services.AddExplorerAccess();
        services.AddExplorerTenantView();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>(),
            Is.InstanceOf<AccessExplorerTenantOperatorGate>());
    }

    [Test]
    public void AddExplorerTenantView_registersIdentityResolverAndSwitcher()
    {
        using var provider = BuildProvider(addTenantView: true);
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerTenantIdentityResolver>(),
            Is.InstanceOf<DefaultExplorerTenantIdentityResolver>());
        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerTenantSwitcher>(),
            Is.InstanceOf<ExplorerTenantSwitcher>());
    }

    [Test]
    public void WithoutAddExplorerTenantView_viewIsNotRegistered()
    {
        using var provider = BuildProvider(addTenantView: false);
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetService<IExplorerTenantView>(), Is.Null);
    }

    [Test]
    public void WithoutAddExplorerTenantView_resolverAndSwitcherAreNotRegistered()
    {
        using var provider = BuildProvider(addTenantView: false);
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetService<IExplorerTenantIdentityResolver>(), Is.Null);
        Assert.That(scope.ServiceProvider.GetService<IExplorerTenantSwitcher>(), Is.Null);
    }

    [Test]
    public void AddExplorerTenantView_nullServices_throws()
    {
        Assert.That(
            () => ExplorerTenantServiceCollectionExtensions.AddExplorerTenantView(null!),
            Throws.ArgumentNullException);
    }
}
