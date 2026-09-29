using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The tenant a circuit asserts on its calls: the context's active tenant, read
/// live, nothing when there is none or it is the reserved default, and one per
/// circuit.
/// </summary>
[TestFixture]
public sealed class ExplorerActiveTenantProviderTests
{
    [Test]
    public void The_constructor_rejects_a_missing_context() =>
        Assert.That(() => new ExplorerActiveTenantProvider(null!), Throws.ArgumentNullException);

    [Test]
    public void It_asserts_nothing_until_a_tenant_is_established()
    {
        var provider = new ExplorerActiveTenantProvider(new ExplorerTenantContext());

        Assert.That(provider.AssertedTenant, Is.Null);
    }

    [Test]
    public void It_asserts_the_active_tenant_and_follows_a_switch_at_once()
    {
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") };
        var provider = new ExplorerActiveTenantProvider(context);
        var before = provider.AssertedTenant;

        context.ActiveTenant = new ExplorerTenantId("globex");

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo("acme"));
            Assert.That(provider.AssertedTenant, Is.EqualTo("globex"));
        });
    }

    [Test]
    public void The_reserved_default_tenant_is_not_asserted()
    {
        var provider = new ExplorerActiveTenantProvider(new ExplorerTenantContext { ActiveTenant = ExplorerTenantId.Default });

        Assert.That(provider.AssertedTenant, Is.Null, "no assertion is exactly a default-tenant call");
    }

    [Test]
    public void AddExplorerTenantView_registers_one_provider_per_circuit()
    {
        using var root = new ServiceCollection()
            .AddScoped(_ => Substitute.For<IExplorerAuthSession>())
            .AddExplorerTenantView()
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        using var first = root.CreateScope();
        using var second = root.CreateScope();

        first.ServiceProvider.GetRequiredService<IExplorerTenantContext>().ActiveTenant = new ExplorerTenantId("acme");
        second.ServiceProvider.GetRequiredService<IExplorerTenantContext>().ActiveTenant = new ExplorerTenantId("globex");

        Assert.Multiple(() =>
        {
            Assert.That(first.ServiceProvider.GetRequiredService<ILatticeActiveTenantProvider>().AssertedTenant, Is.EqualTo("acme"));
            Assert.That(second.ServiceProvider.GetRequiredService<ILatticeActiveTenantProvider>().AssertedTenant, Is.EqualTo("globex"));
        });
    }

    [Test]
    public void Without_tenancy_no_provider_is_registered()
    {
        using var root = new ServiceCollection().BuildServiceProvider();

        Assert.That(root.GetService<ILatticeActiveTenantProvider>(), Is.Null);
    }
}
