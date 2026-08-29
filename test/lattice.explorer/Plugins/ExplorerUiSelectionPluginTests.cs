using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Detail.Tabs;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The per-selection plugins the shared UI ships, and the registration surface a
/// head uses to choose them. They occupy the same contract the area plugins do,
/// which is the whole point of the one-model decision: the detail panel gains
/// gating and ordering without a second registry.
/// </summary>
[TestFixture]
public sealed class ExplorerUiSelectionPluginTests
{
    private static readonly IExplorerPlugin[] Shipped =
    [
        new MetricsSelectionPlugin(),
        new TopologySelectionPlugin(),
        new DataSelectionPlugin(),
        new DeadLetterSelectionPlugin(),
        new TagIndexSelectionPlugin(),
    ];

    [Test]
    public void Every_selection_plugin_declares_a_stable_dotted_id_and_the_selection_surface()
    {
        Assert.Multiple(() =>
        {
            foreach (var plugin in Shipped)
            {
                Assert.That(plugin.Descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Selection));
                Assert.That(
                    plugin.Descriptor.PluginId,
                    Does.StartWith("orleans.lattice."),
                    "a package-owned id is what stops two independently authored plugins colliding");
                Assert.That(plugin.AccessGate, Is.Not.Null);
                Assert.That(plugin.ViewType, Is.Not.Null);
                Assert.That(plugin.DomainContract, Is.Null, "the views still resolve their own readers");
            }
        });
    }

    [Test]
    public void Each_selection_plugin_names_its_own_view_label_and_id()
    {
        var metrics = new MetricsSelectionPlugin();
        var topology = new TopologySelectionPlugin();
        var data = new DataSelectionPlugin();
        var deadLetter = new DeadLetterSelectionPlugin();
        var tagIndex = new TagIndexSelectionPlugin();

        Assert.Multiple(() =>
        {
            Assert.That(metrics.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.Metrics));
            Assert.That(metrics.Descriptor.Label, Is.EqualTo("Metrics"));
            Assert.That(metrics.ViewType, Is.EqualTo(typeof(MetricsTab)));

            Assert.That(topology.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.Topology));
            Assert.That(topology.Descriptor.Label, Is.EqualTo("Topology"));
            Assert.That(topology.ViewType, Is.EqualTo(typeof(TopologyTab)));

            Assert.That(data.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.Data));
            Assert.That(data.Descriptor.Label, Is.EqualTo("Data"));
            Assert.That(data.ViewType, Is.EqualTo(typeof(DataTab)));

            Assert.That(deadLetter.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.DeadLetter));
            Assert.That(deadLetter.Descriptor.Label, Is.EqualTo("Dead-letter"));
            Assert.That(deadLetter.ViewType, Is.EqualTo(typeof(DeadLetterTab)));

            Assert.That(tagIndex.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.TagIndex));
            Assert.That(tagIndex.Descriptor.Label, Is.EqualTo("Tag index"));
            Assert.That(tagIndex.ViewType, Is.EqualTo(typeof(TagIndexDetailTab)));
        });
    }

    [Test]
    public void The_shipped_surfaces_are_allowed_until_a_gate_is_deliberately_tightened()
    {
        // None of the five surfaces has a capability of its own to probe, so each
        // admits whoever reaches the panel; the server stays the sole enforcement
        // point either way. Behaviour is therefore unchanged from the ungated
        // tier the plugins replace.
        Assert.Multiple(() =>
        {
            foreach (var plugin in Shipped)
            {
                Assert.That(plugin.AccessGate, Is.SameAs(ExplorerPluginAccessGates.Allowed));
            }
        });
    }

    [Test]
    public void The_generic_surfaces_apply_to_trees_and_views_and_never_to_a_tag_index()
    {
        var generic = new IExplorerPlugin[]
        {
            new MetricsSelectionPlugin(),
            new TopologySelectionPlugin(),
            new DataSelectionPlugin(),
            new DeadLetterSelectionPlugin(),
        };

        Assert.Multiple(() =>
        {
            foreach (var plugin in generic)
            {
                Assert.That(plugin.Descriptor.AppliesTo(ExplorerPluginSelectionKind.Tree), Is.True);
                Assert.That(plugin.Descriptor.AppliesTo(ExplorerPluginSelectionKind.View), Is.True);
                Assert.That(
                    plugin.Descriptor.AppliesTo(ExplorerPluginSelectionKind.TagIndex),
                    Is.False,
                    "a membership tree's composite-key rows are not what these surfaces are for");
            }
        });
    }

    [Test]
    public void The_tag_index_surface_applies_to_a_tag_index_alone()
    {
        var descriptor = new TagIndexSelectionPlugin().Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.TagIndex), Is.True);
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.Tree), Is.False);
            Assert.That(descriptor.AppliesTo(ExplorerPluginSelectionKind.View), Is.False);
        });
    }

    [Test]
    public void A_catalog_over_the_shipped_set_resolves_each_selection_kind_to_its_own_surfaces()
    {
        var catalog = new ExplorerPluginCatalog(Shipped);

        Assert.Multiple(() =>
        {
            Assert.That(
                catalog.ForSelection(ExplorerPluginSelectionKind.Tree).Select(p => p.Descriptor.Label),
                Is.EqualTo(new[] { "Metrics", "Topology", "Data", "Dead-letter" }),
                "the strip's left-to-right order is preserved by the descriptor hints");

            Assert.That(
                catalog.ForSelection(ExplorerPluginSelectionKind.View).Select(p => p.Descriptor.Label),
                Is.EqualTo(new[] { "Metrics", "Topology", "Data", "Dead-letter" }));

            Assert.That(
                catalog.ForSelection(ExplorerPluginSelectionKind.TagIndex).Select(p => p.Descriptor.Label),
                Is.EqualTo(new[] { "Tag index" }),
                "a tag index resolves to a different set, not to a bypass");
        });
    }

    [Test]
    public async Task AddExplorerSelectionPlugins_registers_the_whole_tier()
    {
        await using var provider = BuildHost(services => services.AddExplorerSelectionPlugins());
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider
                .GetRequiredService<IExplorerPluginCatalog>()
                .ForSurface(ExplorerPluginSurface.Selection)
                .Select(p => p.Descriptor.PluginId),
            Is.EquivalentTo(new[]
            {
                SelectionPluginKeys.Metrics,
                SelectionPluginKeys.Topology,
                SelectionPluginKeys.Data,
                SelectionPluginKeys.DeadLetter,
                SelectionPluginKeys.TagIndex,
            }));
    }

    [Test]
    public async Task AddExplorerSelectionPlugins_is_idempotent()
    {
        await using var provider = BuildHost(services =>
        {
            services.AddExplorerSelectionPlugins();
            services.AddExplorerSelectionPlugins();
        });
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider.GetServices<IExplorerPlugin>().Count(),
            Is.EqualTo(5),
            "a duplicate registration must not fail the catalog's unique-id check");
    }

    [Test]
    public async Task A_head_may_register_a_single_selection_surface_instead_of_the_whole_tier()
    {
        await using var provider = BuildHost(services => services.AddExplorerPlugin<DataSelectionPlugin>());
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider
                .GetRequiredService<IExplorerPluginCatalog>()
                .ForSelection(ExplorerPluginSelectionKind.Tree)
                .Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { SelectionPluginKeys.Data }));
    }

    [Test]
    public void AddExplorerSelectionPlugins_rejects_a_null_service_collection()
    {
        Assert.That(
            () => ExplorerUiPluginServiceCollectionExtensions.AddExplorerSelectionPlugins(null!),
            Throws.ArgumentNullException);
    }

    private static ServiceProvider BuildHost(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        configure(services);
        return services.BuildServiceProvider();
    }
}
