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
    public async Task AddExplorerConfiguration_SessionAndConnectionAreSingletons()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration();
        await using var provider = services.BuildServiceProvider();

        var session = provider.GetRequiredService<IExplorerSession>();
        Assert.That(provider.GetRequiredService<IExplorerSession>(), Is.SameAs(session));
        Assert.That(session.Connection, Is.SameAs(provider.GetRequiredService<ILatticeStateConnection>()));
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
