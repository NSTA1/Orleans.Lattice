using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Metrics;

namespace Orleans.Lattice.Explorer.Tests.Metrics;

[TestFixture]
public class ExplorerMetricsServiceCollectionExtensionsTests
{
    [Test]
    public async Task AddExplorerMetrics_RegistersReader()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerMetrics();
        await using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetRequiredService<IMetricsReader>(), Is.InstanceOf<MetricsReader>());
    }

    [Test]
    public async Task AddExplorerMetrics_ReaderIsScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerMetrics();
        await using var provider = services.BuildServiceProvider();

        await using var scopeA = provider.CreateAsyncScope();
        await using var scopeB = provider.CreateAsyncScope();
        var readerA = scopeA.ServiceProvider.GetRequiredService<IMetricsReader>();

        Assert.Multiple(() =>
        {
            Assert.That(scopeA.ServiceProvider.GetRequiredService<IMetricsReader>(), Is.SameAs(readerA));
            Assert.That(scopeB.ServiceProvider.GetRequiredService<IMetricsReader>(), Is.Not.SameAs(readerA));
        });
    }
}
