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
    public async Task AddExplorerMetrics_ReaderIsSingleton()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        services.AddExplorerMetrics();
        await using var provider = services.BuildServiceProvider();

        var reader = provider.GetRequiredService<IMetricsReader>();
        Assert.That(provider.GetRequiredService<IMetricsReader>(), Is.SameAs(reader));
    }
}
