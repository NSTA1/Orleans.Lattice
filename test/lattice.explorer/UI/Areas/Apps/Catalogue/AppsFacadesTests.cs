using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

[TestFixture]
public sealed class AppsFacadesTests
{
    [Test]
    public void AssertedTenant_and_caller_follow_the_current_tenant_without_recreating_the_facades()
    {
        var tenant = Substitute.For<ILatticeActiveTenantProvider>();
        var currentTenant = "acme";
        tenant.AssertedTenant.Returns(_ => currentTenant);
        using var services = new ServiceCollection()
            .AddSingleton(tenant)
            .AddSingleton(new ShellCaller(tenant: tenant))
            .BuildServiceProvider();
        var facades = new AppsFacades(services);
        var before = facades.Caller;

        Assert.That(facades.AssertedTenant, Is.EqualTo("acme"));
        Assert.That(before, Is.EqualTo(services.GetRequiredService<ShellCaller>().Current));

        currentTenant = "globex";

        Assert.That(facades.AssertedTenant, Is.EqualTo("globex"));
        Assert.That(facades.Caller, Is.EqualTo(services.GetRequiredService<ShellCaller>().Current));
        Assert.That(facades.Caller, Is.Not.EqualTo(before));
    }

    [TestCase(nameof(AppsFacades.Catalog))]
    [TestCase(nameof(AppsFacades.Control))]
    [TestCase(nameof(AppsFacades.Workspace))]
    [TestCase(nameof(AppsFacades.Auth))]
    [TestCase(nameof(AppsFacades.RoleBindings))]
    public void Resolve_missing_dependencies_yield_null_and_do_not_fall_back_to_the_hosts_facade(string name)
    {
        var type = FacadeType(name);
        var hostFacade = Substitute.For([type], []);
        var resolutions = 0;
        using var services = new ServiceCollection()
            .AddSingleton(type, hostFacade)
            .AddKeyedSingleton(type, ShellFacades.Key, (provider, _) =>
            {
                resolutions++;
                return provider.GetRequiredService<ILatticeStateConnection>();
            })
            .BuildServiceProvider();
        var facades = new AppsFacades(services);

        Assert.That(resolutions, Is.Zero, "facades must resolve lazily");
        Assert.That(Read(facades, name), Is.Null);
        Assert.That(Read(facades, name), Is.Null, "a failed lazy resolution remains absent");
        Assert.That(resolutions, Is.EqualTo(1));
        Assert.That(services.GetRequiredService(type), Is.SameAs(hostFacade));
    }

    [TestCase(nameof(AppsFacades.Catalog))]
    [TestCase(nameof(AppsFacades.Control))]
    [TestCase(nameof(AppsFacades.Workspace))]
    [TestCase(nameof(AppsFacades.Auth))]
    [TestCase(nameof(AppsFacades.RoleBindings))]
    public void Resolve_unregistered_shell_facade_never_uses_an_unkeyed_host_registration(string name)
    {
        var type = FacadeType(name);
        using var services = new ServiceCollection()
            .AddSingleton(type, Substitute.For([type], []))
            .BuildServiceProvider();

        var facades = new AppsFacades(services);

        Assert.That(Read(facades, name), Is.Null);
        Assert.That(facades.AssertedTenant, Is.Null, "a head without a tenant provider asserts no tenant");
    }

    [TestCase(nameof(AppsFacades.Catalog))]
    [TestCase(nameof(AppsFacades.Control))]
    [TestCase(nameof(AppsFacades.Workspace))]
    [TestCase(nameof(AppsFacades.Auth))]
    [TestCase(nameof(AppsFacades.RoleBindings))]
    public void Resolve_registered_shell_facade_is_resolved_once_and_not_confused_with_the_host(string name)
    {
        var type = FacadeType(name);
        var shellFacade = Substitute.For([type], []);
        var resolutions = 0;
        using var services = new ServiceCollection()
            .AddSingleton(type, Substitute.For([type], []))
            .AddKeyedSingleton(type, ShellFacades.Key, (_, _) =>
            {
                resolutions++;
                return shellFacade;
            })
            .BuildServiceProvider();
        var facades = new AppsFacades(services);

        Assert.That(resolutions, Is.Zero);
        Assert.That(Read(facades, name), Is.SameAs(shellFacade));
        Assert.That(Read(facades, name), Is.SameAs(shellFacade));
        Assert.That(resolutions, Is.EqualTo(1));
    }

    [TestCase(nameof(AppsFacades.Catalog))]
    [TestCase(nameof(AppsFacades.Control))]
    [TestCase(nameof(AppsFacades.Workspace))]
    [TestCase(nameof(AppsFacades.Auth))]
    [TestCase(nameof(AppsFacades.RoleBindings))]
    public void Resolve_unexpected_factory_errors_are_not_disguised_as_absent_facades(string name)
    {
        var fault = new FormatException("invalid endpoint");
        using var services = new ServiceCollection()
            .AddKeyedSingleton(FacadeType(name), ShellFacades.Key, (_, _) => throw fault)
            .BuildServiceProvider();

        var exception = Assert.Throws<FormatException>(() => Read(new AppsFacades(services), name));

        Assert.That(exception, Is.SameAs(fault));
    }

    private static Type FacadeType(string name) => name switch
    {
        nameof(AppsFacades.Catalog) => typeof(ILatticeAppCatalog),
        nameof(AppsFacades.Control) => typeof(ILatticeAppsControl),
        nameof(AppsFacades.Workspace) => typeof(ILatticeAppWorkspace),
        nameof(AppsFacades.Auth) => typeof(ILatticeAuthAdmin),
        nameof(AppsFacades.RoleBindings) => typeof(ILatticeAppRoleBindings),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    private static object? Read(AppsFacades facades, string name) => name switch
    {
        nameof(AppsFacades.Catalog) => facades.Catalog,
        nameof(AppsFacades.Control) => facades.Control,
        nameof(AppsFacades.Workspace) => facades.Workspace,
        nameof(AppsFacades.Auth) => facades.Auth,
        nameof(AppsFacades.RoleBindings) => facades.RoleBindings,
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };
}
