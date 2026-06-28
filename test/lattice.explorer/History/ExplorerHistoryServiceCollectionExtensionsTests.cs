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
    public async Task AddExplorerHistory_ReaderIsSingleton()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerHistory();
        await using var provider = services.BuildServiceProvider();

        var reader = provider.GetRequiredService<IHistoryReader>();
        Assert.That(provider.GetRequiredService<IHistoryReader>(), Is.SameAs(reader));
    }
}
