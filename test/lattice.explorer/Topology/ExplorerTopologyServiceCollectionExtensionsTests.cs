using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Topology;

namespace Orleans.Lattice.Explorer.Tests.Topology;

[TestFixture]
public class ExplorerTopologyServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddExplorerTopology_RegistersReader()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerTopology();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<ITopologyReader>(), Is.InstanceOf<TopologyReader>());
    }

    [Test]
    public async Task AddExplorerTopology_ReaderIsScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerTopology();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();
        var reader = scopeA.ServiceProvider.GetRequiredService<ITopologyReader>();

        Assert.Multiple(() =>
        {
            Assert.That(scopeA.ServiceProvider.GetRequiredService<ITopologyReader>(), Is.SameAs(reader));
            Assert.That(scopeB.ServiceProvider.GetRequiredService<ITopologyReader>(), Is.Not.SameAs(reader));
        });
    }
}
