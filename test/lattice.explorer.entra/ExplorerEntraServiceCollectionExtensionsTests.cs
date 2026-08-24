using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Entra.Tests;

[TestFixture]
public class ExplorerEntraServiceCollectionExtensionsTests
{
    [Test]
    public void AddExplorerEntraAuth_nullServices_throws()
        => Assert.That(
            () => ((IServiceCollection)null!).AddExplorerEntraAuth(),
            Throws.ArgumentNullException);

    [Test]
    public void AddExplorerEntraAuth_registersEntraMethod_andAcquirer()
    {
        var services = new ServiceCollection();

        services.AddExplorerEntraAuth();

        using var provider = services.BuildServiceProvider();
        var methods = provider.GetServices<IExplorerAuthMethod>().ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(methods, Has.One.InstanceOf<EntraExplorerAuthMethod>());
            Assert.That(provider.GetService<IEntraInteractiveTokenAcquirer>(), Is.InstanceOf<MsalEntraInteractiveTokenAcquirer>());
        });
    }

    [Test]
    public void AddExplorerEntraAuth_appliesConfiguration()
    {
        var services = new ServiceCollection();

        services.AddExplorerEntraAuth(o =>
        {
            o.ClientId = "configured-client";
            o.Authority = "https://login.microsoftonline.com/configured";
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<ExplorerEntraOptions>>();
        Assert.Multiple(() =>
        {
            Assert.That(options.CurrentValue.ClientId, Is.EqualTo("configured-client"));
            Assert.That(options.CurrentValue.Authority, Is.EqualTo("https://login.microsoftonline.com/configured"));
        });
    }

    [Test]
    public void AddExplorerEntraAuth_keepsPreviouslyRegisteredAcquirer()
    {
        var services = new ServiceCollection();
        var custom = new StubAcquirer();
        services.AddSingleton<IEntraInteractiveTokenAcquirer>(custom);

        services.AddExplorerEntraAuth();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetService<IEntraInteractiveTokenAcquirer>(), Is.SameAs(custom));
    }

    [Test]
    public void AddExplorerEntraAuth_registersPerCircuitScopedServices()
    {
        // Credential-isolation regression: the MSAL-backed acquirer owns an
        // in-memory token cache and the auth method drives a per-operator
        // sign-in, so both must be scoped to the Blazor circuit, never a
        // process-global singleton that would leak one operator's credential to
        // every circuit.
        var services = new ServiceCollection();

        services.AddExplorerEntraAuth();

        var acquirer = services.Single(d => d.ServiceType == typeof(IEntraInteractiveTokenAcquirer));
        var method = services.Single(d =>
            d.ServiceType == typeof(IExplorerAuthMethod)
            && d.ImplementationType == typeof(EntraExplorerAuthMethod));
        Assert.Multiple(() =>
        {
            Assert.That(acquirer.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(method.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        });
    }

    private sealed class StubAcquirer : IEntraInteractiveTokenAcquirer
    {
        public Task<EntraTokenResult> AcquireInteractiveAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new EntraTokenResult { AccessToken = "x", ExpiresOn = DateTimeOffset.MaxValue });

        public Task<EntraTokenResult?> AcquireSilentAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult<EntraTokenResult?>(null);
    }
}
