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
    public async Task AddExplorerData_ReaderIsSingleton()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerData();
        await using var provider = services.BuildServiceProvider();

        var reader = provider.GetRequiredService<IDataReader>();
        Assert.That(provider.GetRequiredService<IDataReader>(), Is.SameAs(reader));
    }
}
