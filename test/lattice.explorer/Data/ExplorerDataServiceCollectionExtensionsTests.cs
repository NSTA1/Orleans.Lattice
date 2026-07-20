using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Data;

namespace Orleans.Lattice.Explorer.Tests.Data;

[TestFixture]
public class ExplorerDataServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddExplorerData_RegistersReader()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerData();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IDataReader>(), Is.InstanceOf<DataReader>());
    }

    [Test]
    public async Task AddExplorerData_ReaderIsScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerData();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();
        var reader = scopeA.ServiceProvider.GetRequiredService<IDataReader>();

        Assert.Multiple(() =>
        {
            Assert.That(scopeA.ServiceProvider.GetRequiredService<IDataReader>(), Is.SameAs(reader));
            Assert.That(scopeB.ServiceProvider.GetRequiredService<IDataReader>(), Is.Not.SameAs(reader));
        });
    }

    [Test]
    public async Task AddExplorerData_RegistersLiveFollower()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerData();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IEntryLiveFollower>(), Is.InstanceOf<EntryLiveFollower>());
    }

    [Test]
    public async Task AddExplorerData_LiveFollowerIsScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerData();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();
        var follower = scopeA.ServiceProvider.GetRequiredService<IEntryLiveFollower>();

        Assert.Multiple(() =>
        {
            Assert.That(scopeA.ServiceProvider.GetRequiredService<IEntryLiveFollower>(), Is.SameAs(follower));
            Assert.That(scopeB.ServiceProvider.GetRequiredService<IEntryLiveFollower>(), Is.Not.SameAs(follower));
        });
    }
}
