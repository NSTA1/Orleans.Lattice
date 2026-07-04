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

    private sealed class StubAcquirer : IEntraInteractiveTokenAcquirer
    {
        public Task<EntraTokenResult> AcquireInteractiveAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new EntraTokenResult { AccessToken = "x", ExpiresOn = DateTimeOffset.MaxValue });

        public Task<EntraTokenResult?> AcquireSilentAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult<EntraTokenResult?>(null);
    }
}
