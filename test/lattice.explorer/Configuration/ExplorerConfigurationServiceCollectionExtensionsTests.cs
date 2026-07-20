using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

[TestFixture]
public class ExplorerConfigurationServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddExplorerConfiguration_RegistersStoreSessionAndConnection()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IExplorerConfigStore>(), Is.InstanceOf<JsonExplorerConfigStore>());
        Assert.That(provider.GetRequiredService<ILatticeStateConnection>(), Is.Not.Null);
        var session = provider.GetRequiredService<IExplorerSession>();
        Assert.That(session, Is.InstanceOf<ExplorerSession>());
    }

    [Test]
    public async Task AddExplorerConfiguration_SessionAndConnectionAreScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();

        var sessionA = scopeA.ServiceProvider.GetRequiredService<IExplorerSession>();
        var sessionB = scopeB.ServiceProvider.GetRequiredService<IExplorerSession>();

        Assert.Multiple(() =>
        {
            // Same scope resolves the same session and its own connection.
            Assert.That(scopeA.ServiceProvider.GetRequiredService<IExplorerSession>(), Is.SameAs(sessionA));
            Assert.That(sessionA.Connection, Is.SameAs(scopeA.ServiceProvider.GetRequiredService<ILatticeStateConnection>()));

            // A second circuit gets an independent session and connection.
            Assert.That(sessionB, Is.Not.SameAs(sessionA));
            Assert.That(sessionB.Connection, Is.Not.SameAs(sessionA.Connection));
        });
    }

    [Test]
    public async Task AddExplorerConfiguration_AppliesFilePathOption()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/custom/path.json");
        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IExplorerConfigStore>();

        Assert.That(store.FilePath, Is.EqualTo("C:/custom/path.json"));
    }
}
