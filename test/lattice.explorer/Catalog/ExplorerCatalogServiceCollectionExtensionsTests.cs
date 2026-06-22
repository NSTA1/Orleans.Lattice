using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Catalog;

[TestFixture]
public class ExplorerCatalogServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddExplorerCatalog_RegistersReaderAndSelection()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ICatalogReader>(), Is.InstanceOf<CatalogReader>());
        Assert.That(provider.GetRequiredService<IExplorerSelection>(), Is.InstanceOf<ExplorerSelection>());
    }

    [Test]
    public async Task AddExplorerCatalog_ExposesConnectionAsClient()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        await using var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<ILatticeStateClient>();
        var connection = provider.GetRequiredService<ILatticeStateConnection>();

        Assert.That(client, Is.SameAs(connection));
    }

    [Test]
    public async Task AddExplorerCatalog_SelectionIsSingleton()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        await using var provider = services.BuildServiceProvider();

        var selection = provider.GetRequiredService<IExplorerSelection>();
        Assert.That(provider.GetRequiredService<IExplorerSelection>(), Is.SameAs(selection));
    }
}
