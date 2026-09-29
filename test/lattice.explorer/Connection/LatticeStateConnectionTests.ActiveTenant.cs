using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// The state connection carries the circuit's live tenant source into every
/// client it builds, whichever path rebuilt the channel, and dependency
/// injection selects it exactly when the head registers tenancy.
/// </summary>
public partial class LatticeStateConnectionTests
{
    [Test]
    public async Task Every_client_the_connection_builds_carries_its_tenant_source()
    {
        var provider = new FakeActiveTenantProvider("acme");
        var built = new List<LatticeConnectionSettings>();
        var connection = new LatticeStateConnection(
            settings =>
            {
                built.Add(settings);
                return new FakeStateClient();
            },
            new ControllableTimeProvider(Origin),
            provider);

        await connection.ConfigureAsync(Settings());
        await connection.ReconnectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(built, Has.Count.EqualTo(2));
            Assert.That(built.Select(settings => settings.ActiveTenantProvider), Is.All.SameAs(provider));
            Assert.That(connection.ActiveTenantProvider, Is.SameAs(provider));
        });
    }

    [Test]
    public async Task A_tenant_source_the_caller_supplied_is_kept()
    {
        var own = new FakeActiveTenantProvider("globex");
        LatticeConnectionSettings? built = null;
        var connection = new LatticeStateConnection(
            settings =>
            {
                built = settings;
                return new FakeStateClient();
            },
            new ControllableTimeProvider(Origin),
            new FakeActiveTenantProvider("acme"));

        await connection.ConfigureAsync(Settings() with { ActiveTenantProvider = own });

        Assert.That(built!.ActiveTenantProvider, Is.SameAs(own));
    }

    [Test]
    public async Task Without_a_tenant_source_the_settings_are_passed_through_unchanged()
    {
        var settings = Settings();
        LatticeConnectionSettings? built = null;
        var (connection, _) = NewConnection(candidate =>
        {
            built = candidate;
            return new FakeStateClient();
        });

        await connection.ConfigureAsync(settings);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.SameAs(settings));
            Assert.That(connection.ActiveTenantProvider, Is.Null);
        });
    }

    [Test]
    public void The_tenant_asserting_constructor_rejects_a_missing_source() =>
        Assert.That(() => new LatticeStateConnection((ILatticeActiveTenantProvider)null!), Throws.ArgumentNullException);

    [Test]
    public async Task Dependency_injection_selects_the_tenant_source_only_when_tenancy_is_registered()
    {
        await using var withTenancy = new ServiceCollection()
            .AddLatticeStateConnection()
            .AddExplorerTenantView()
            .AddScoped(_ => Substitute.For<IExplorerAuthSession>())
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var withoutTenancy = new ServiceCollection()
            .AddLatticeStateConnection()
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });

        await using var tenantScope = withTenancy.CreateAsyncScope();
        await using var plainScope = withoutTenancy.CreateAsyncScope();
        var tenantConnection = (LatticeStateConnection)tenantScope.ServiceProvider.GetRequiredService<ILatticeStateConnection>();
        var plainConnection = (LatticeStateConnection)plainScope.ServiceProvider.GetRequiredService<ILatticeStateConnection>();

        Assert.Multiple(() =>
        {
            Assert.That(
                tenantConnection.ActiveTenantProvider,
                Is.SameAs(tenantScope.ServiceProvider.GetRequiredService<ILatticeActiveTenantProvider>()));
            Assert.That(plainConnection.ActiveTenantProvider, Is.Null);
        });
    }
}
