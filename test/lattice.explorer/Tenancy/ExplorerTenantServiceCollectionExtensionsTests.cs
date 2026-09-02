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
    public async Task AddExplorerTenantView_registersAFailClosedAccessibleTenantSourceByDefault()
    {
        // Enumerating a cluster's tenants belongs to the administrative surface
        // that already asks the cluster for them, so the core's own default
        // reports only the scope already established - never a guess.
        using var provider = BuildProvider(addTenantView: true);
        using var scope = provider.CreateScope();

        var source = scope.ServiceProvider.GetRequiredService<IExplorerAccessibleTenantSource>();

        Assert.That(source, Is.InstanceOf<ActiveTenantOnlyAccessibleTenantSource>());
        Assert.That(await source.GetAccessibleTenantsAsync(), Is.Empty);
    }

    [Test]
    public void An_administrative_surface_registered_first_supplies_the_real_accessible_tenant_source()
    {
        // The one-source-of-truth seam: the tenant scope control and the tenant
        // administration area must offer the same list.
        var services = new ServiceCollection();
        services.AddSingleton(Substitute.For<IExplorerAuthSession>());
        services.AddScoped<IExplorerAccessibleTenantSource>(
            _ => new FakeAccessibleTenantSource(SampleTenant.TenantId, SampleTenant.OtherTenantId));
        services.AddExplorerTenantView();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(
            scope.ServiceProvider.GetRequiredService<IExplorerAccessibleTenantSource>(),
            Is.InstanceOf<FakeAccessibleTenantSource>());
    }

    [Test]
    public void AddExplorerTenantView_registersThePerCircuitNoticeSlot()
    {
        using var provider = BuildProvider(addTenantView: true);
        using var scope = provider.CreateScope();

        var notices = scope.ServiceProvider.GetRequiredService<IExplorerTenantScopeNotices>();

        Assert.That(notices, Is.InstanceOf<ExplorerTenantScopeNotices>());
        Assert.That(notices.Current, Is.Null);
    }

    [Test]
    public void The_notice_slot_is_scoped_so_one_circuit_never_announces_anothers_outcome()
    {
        using var provider = BuildProvider(addTenantView: true);
        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        first.ServiceProvider.GetRequiredService<IExplorerTenantScopeNotices>()
            .Publish(ExplorerTenantScopeNotice.Refused());

        Assert.That(
            second.ServiceProvider.GetRequiredService<IExplorerTenantScopeNotices>().Current,
            Is.Null);
    }

    [Test]
    public void WithoutAddExplorerTenantView_theAccessibleSourceAndNoticeSlotAreNotRegistered()
    {
        using var provider = BuildProvider(addTenantView: false);
        using var scope = provider.CreateScope();

        Assert.Multiple(() =>
        {
            Assert.That(scope.ServiceProvider.GetService<IExplorerAccessibleTenantSource>(), Is.Null);
            Assert.That(scope.ServiceProvider.GetService<IExplorerTenantScopeNotices>(), Is.Null);
        });
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
