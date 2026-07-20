using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.History;

namespace Orleans.Lattice.Explorer.Tests.History;

[TestFixture]
public class ExplorerHistoryServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddExplorerHistory_RegistersReader()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerHistory();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IHistoryReader>(), Is.InstanceOf<HistoryReader>());
    }

    [Test]
    public async Task AddExplorerHistory_RegistersLiveFollower()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerHistory();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IHistoryLiveFollower>(), Is.InstanceOf<HistoryLiveFollower>());
    }

    [Test]
    public async Task AddExplorerHistory_ReaderIsScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerHistory();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();
        var reader = scopeA.ServiceProvider.GetRequiredService<IHistoryReader>();

        Assert.Multiple(() =>
        {
            Assert.That(scopeA.ServiceProvider.GetRequiredService<IHistoryReader>(), Is.SameAs(reader));
            Assert.That(scopeB.ServiceProvider.GetRequiredService<IHistoryReader>(), Is.Not.SameAs(reader));
        });
    }
}
