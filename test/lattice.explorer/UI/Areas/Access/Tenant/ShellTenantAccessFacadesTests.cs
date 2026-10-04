using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// The seam the tenant pages read their delegated administration through. Both of its
/// refusals are fail-closed and answer exactly what a head serving nothing answers, so
/// neither is visible from a page: an absent registration and a registration the head
/// cannot build both read as <see langword="null"/>, and the pages stay cluster-wide
/// either way. Each needs a collaborator no page can supply.
/// </summary>
[TestFixture]
public sealed class ShellTenantAccessFacadesTests
{
    [Test]
    public void Both_facades_are_the_shells_own_keyed_registrations()
    {
        var directory = Substitute.For<ILatticeTenantDirectoryAdmin>();
        var policy = Substitute.For<ILatticeTenantPolicyAdmin>();
        var services = new ServiceCollection()
            .AddKeyedSingleton(ShellFacades.Key, directory)
            .AddKeyedSingleton(ShellFacades.Key, policy)
            .BuildServiceProvider();

        var facades = new ShellTenantAccessFacades(services);

        Assert.Multiple(() =>
        {
            Assert.That(facades.Directory, Is.SameAs(directory));
            Assert.That(facades.Policy, Is.SameAs(policy));
        });
    }

    [Test]
    public void A_facade_registered_only_by_bare_interface_is_not_the_shells_and_is_not_served()
    {
        var services = new ServiceCollection()
            .AddSingleton(Substitute.For<ILatticeTenantDirectoryAdmin>())
            .AddSingleton(Substitute.For<ILatticeTenantPolicyAdmin>())
            .BuildServiceProvider();

        var facades = new ShellTenantAccessFacades(services);

        // A co-hosting head's own in-process facades carry no caller credential; the
        // Explorer must never pick them up, whichever order they were added in.
        Assert.Multiple(() =>
        {
            Assert.That(facades.Directory, Is.Null);
            Assert.That(facades.Policy, Is.Null);
        });
    }

    [Test]
    public void A_head_that_serves_neither_facade_yields_null_rather_than_throwing()
    {
        var facades = new ShellTenantAccessFacades(new ServiceCollection().BuildServiceProvider());

        Assert.Multiple(() =>
        {
            Assert.That(facades.Directory, Is.Null);
            Assert.That(facades.Policy, Is.Null);
        });
    }

    [Test]
    public void A_registration_the_head_cannot_construct_reads_as_a_facade_it_does_not_serve()
    {
        var services = new ServiceCollection()
            .AddKeyedSingleton<ILatticeTenantDirectoryAdmin>(ShellFacades.Key, (_, _) => throw new InvalidOperationException("the endpoint is not configured"))
            .AddKeyedSingleton<ILatticeTenantPolicyAdmin>(ShellFacades.Key, (_, _) => throw new InvalidOperationException("the endpoint is not configured"))
            .BuildServiceProvider();

        var facades = new ShellTenantAccessFacades(services);

        // Distinct from the absent case above: that one never runs a factory at all.
        Assert.Multiple(() =>
        {
            Assert.That(facades.Directory, Is.Null);
            Assert.That(facades.Policy, Is.Null);
        });
    }

    [Test]
    public void Each_facade_is_resolved_once_and_remembered_for_the_circuit()
    {
        var directories = 0;
        var policies = 0;
        var services = new ServiceCollection()
            .AddKeyedTransient<ILatticeTenantDirectoryAdmin>(ShellFacades.Key, (_, _) =>
            {
                directories++;
                return Substitute.For<ILatticeTenantDirectoryAdmin>();
            })
            .AddKeyedTransient<ILatticeTenantPolicyAdmin>(ShellFacades.Key, (_, _) =>
            {
                policies++;
                return Substitute.For<ILatticeTenantPolicyAdmin>();
            })
            .BuildServiceProvider();

        var facades = new ShellTenantAccessFacades(services);

        Assert.Multiple(() =>
        {
            // Transient registrations, so a second resolve would hand back a new
            // instance; the same one twice is the laziness being remembered.
            Assert.That(facades.Directory, Is.SameAs(facades.Directory));
            Assert.That(facades.Policy, Is.SameAs(facades.Policy));
            Assert.That(directories, Is.EqualTo(1));
            Assert.That(policies, Is.EqualTo(1));
        });
    }

    [Test]
    public void Neither_facade_is_resolved_until_it_is_read()
    {
        var resolved = 0;
        var services = new ServiceCollection()
            .AddKeyedTransient<ILatticeTenantDirectoryAdmin>(ShellFacades.Key, (_, _) =>
            {
                resolved++;
                return Substitute.For<ILatticeTenantDirectoryAdmin>();
            })
            .BuildServiceProvider();

        var facades = new ShellTenantAccessFacades(services);
        Assert.That(resolved, Is.Zero, "constructing the seam must not call the head");

        _ = facades.Directory;

        Assert.That(resolved, Is.EqualTo(1));
    }

    [Test]
    public void The_shell_registers_this_seam_for_the_tenant_pages()
    {
        using var provider = Shell().BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<ITenantAccessFacades>(), Is.InstanceOf<ShellTenantAccessFacades>());
    }

    [Test]
    public void A_head_that_supplies_its_own_seam_keeps_it()
    {
        var mine = new StubTenantAccessFacades();
        var services = new ServiceCollection();
        services.AddScoped<ITenantAccessFacades>(_ => mine);

        using var provider = Shell(services).BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.That(scope.ServiceProvider.GetRequiredService<ITenantAccessFacades>(), Is.SameAs(mine));
    }

    private static IServiceCollection Shell(IServiceCollection? services = null)
    {
        services ??= new ServiceCollection();
        services.AddLogging();
        services.AddLatticeExplorerShell();
        return services;
    }

    /// <summary>A head's own seam. Hand-written: the contract is internal, so it cannot be proxied.</summary>
    private sealed class StubTenantAccessFacades : ITenantAccessFacades
    {
        public ILatticeTenantDirectoryAdmin? Directory => null;

        public ILatticeTenantPolicyAdmin? Policy => null;
    }
}
