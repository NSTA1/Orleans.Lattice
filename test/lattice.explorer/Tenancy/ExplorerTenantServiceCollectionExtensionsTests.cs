using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class ExplorerTenantServiceCollectionExtensionsTests
{
    private static ServiceProvider BuildProvider(bool addTenantView)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerCapabilityStore>());
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
    public void AddExplorerTenantView_registersContextAndGate()
    {
        using var provider = BuildProvider(addTenantView: true);
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerTenantContext>(),
            Is.InstanceOf<ExplorerTenantContext>());
        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerTenantOperatorGate>(),
            Is.InstanceOf<CapabilityExplorerTenantOperatorGate>());
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
