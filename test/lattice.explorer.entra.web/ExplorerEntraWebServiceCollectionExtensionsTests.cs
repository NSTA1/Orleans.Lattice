using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Unit tests for
/// <see cref="ExplorerEntraWebServiceCollectionExtensions.AddLatticeExplorerEntraWebAuth"/>:
/// the registration validates the options and wires the scoped auth method,
/// acquirer, and (by default) the auto-sign-in circuit handler.
/// </summary>
[TestFixture]
public sealed class ExplorerEntraWebServiceCollectionExtensionsTests
{
    private static void ConfigureValid(ExplorerEntraWebOptions options)
    {
        options.TenantId = "tenant";
        options.ClientId = "client";
    }

    [Test]
    public void Throws_on_null_services()
    {
        Assert.Throws<ArgumentNullException>(
            () => ExplorerEntraWebServiceCollectionExtensions.AddLatticeExplorerEntraWebAuth(null!, ConfigureValid));
    }

    [Test]
    public void Throws_on_null_configure()
    {
        Assert.Throws<ArgumentNullException>(
            () => new ServiceCollection().AddLatticeExplorerEntraWebAuth(null!));
    }

    [Test]
    public void Throws_when_options_are_invalid()
    {
        Assert.Throws<InvalidOperationException>(
            () => new ServiceCollection().AddLatticeExplorerEntraWebAuth(_ => { }));
    }

    [Test]
    public void Registers_the_auth_method_as_a_scoped_provider()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        var descriptor = services.SingleOrDefault(d =>
            d.ServiceType == typeof(IExplorerAuthMethod)
            && d.ImplementationType == typeof(EntraWebExplorerAuthMethod));

        Assert.That(descriptor, Is.Not.Null);
        Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void Registers_the_token_acquirer_as_scoped()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        var descriptor = services.SingleOrDefault(d => d.ServiceType == typeof(IExplorerWebTokenAcquirer));

        Assert.That(descriptor, Is.Not.Null);
        Assert.That(descriptor!.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void Registers_the_auto_sign_in_circuit_handler_by_default()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(ConfigureValid);

        var registered = services.Any(d =>
            d.ServiceType == typeof(CircuitHandler)
            && d.ImplementationType == typeof(ExplorerEntraWebAutoSignInCircuitHandler));

        Assert.That(registered, Is.True);
    }

    [Test]
    public void Omits_the_circuit_handler_when_auto_sign_in_is_disabled()
    {
        var services = new ServiceCollection().AddLatticeExplorerEntraWebAuth(o =>
        {
            ConfigureValid(o);
            o.AutoSignIn = false;
        });

        var registered = services.Any(d =>
            d.ServiceType == typeof(CircuitHandler)
            && d.ImplementationType == typeof(ExplorerEntraWebAutoSignInCircuitHandler));

        Assert.That(registered, Is.False);
    }

    [Test]
    public void Returns_the_same_service_collection()
    {
        var services = new ServiceCollection();

        var result = services.AddLatticeExplorerEntraWebAuth(ConfigureValid);

        Assert.That(result, Is.SameAs(services));
    }
}
