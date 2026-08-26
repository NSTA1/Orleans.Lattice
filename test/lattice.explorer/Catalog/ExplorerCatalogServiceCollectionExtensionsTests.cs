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
    public async Task AddExplorerCatalog_SelectionIsScopedPerCircuit()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        await using var provider = services.BuildServiceProvider();

        var descriptor = services.Single(d => d.ServiceType == typeof(IExplorerSelection));
        Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));

        // Within one circuit the selection is a single shared instance...
        await using var circuitA = provider.CreateAsyncScope();
        Assert.That(
            circuitA.ServiceProvider.GetRequiredService<IExplorerSelection>(),
            Is.SameAs(circuitA.ServiceProvider.GetRequiredService<IExplorerSelection>()));

        // ...but two circuits never share one.
        await using var circuitB = provider.CreateAsyncScope();
        Assert.That(
            circuitB.ServiceProvider.GetRequiredService<IExplorerSelection>(),
            Is.Not.SameAs(circuitA.ServiceProvider.GetRequiredService<IExplorerSelection>()));
    }

    /// <summary>
    /// Regression: the selection state must not leak across Blazor circuits. It was
    /// registered as a singleton on a multi-user web head, so one operator's
    /// selected <see cref="CatalogItem"/> - its tree id, view source topology and
    /// restore-shadow links - was published into every other signed-in operator's
    /// detail panel with no authorization re-check, and any operator could
    /// re-target every other operator's panel.
    /// </summary>
    [Test]
    public async Task AddExplorerCatalog_SelectionDoesNotLeakAcrossCircuits()
    {
        var services = new ServiceCollection();
        services.AddExplorerConfiguration(options => options.FilePath = "C:/tmp/config.json");
        services.AddExplorerCatalog();
        await using var provider = services.BuildServiceProvider();

        await using var circuitA = provider.CreateAsyncScope();
        await using var circuitB = provider.CreateAsyncScope();

        var selectionA = circuitA.ServiceProvider.GetRequiredService<IExplorerSelection>();
        var selectionB = circuitB.ServiceProvider.GetRequiredService<IExplorerSelection>();

        var notifiedB = false;
        selectionB.SelectionChanged += () => notifiedB = true;

        selectionA.Select(new CatalogItem
        {
            Id = "confidential-tree",
            DisplayName = "confidential-tree",
            Kind = CatalogKind.Trees,
        });

        Assert.Multiple(() =>
        {
            Assert.That(selectionA.Selected!.Id, Is.EqualTo("confidential-tree"));
            Assert.That(selectionB.Selected, Is.Null, "another circuit's selection must not be visible");
            Assert.That(notifiedB, Is.False, "another circuit must not be notified of a foreign selection");
        });
    }
}
