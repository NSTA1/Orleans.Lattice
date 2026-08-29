using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Data;
using Orleans.Lattice.Explorer.Plugins.DeadLetter;
using Orleans.Lattice.Explorer.Plugins.History;
using Orleans.Lattice.Explorer.Plugins.Metrics;
using Orleans.Lattice.Explorer.Plugins.Selection;
using Orleans.Lattice.Explorer.Plugins.TagIndex;
using Orleans.Lattice.Explorer.Plugins.Topology;
using Orleans.Lattice.Explorer.UI.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The per-selection surfaces the Explorer ships, and the registration surface a
/// head uses to choose them. Each is now its own package with its own view, its
/// own scoped stylesheet and its own controlled domain contract, so what this
/// fixture pins is the contract they all satisfy rather than any one of them.
/// <para>
/// Five of them occupy the tier's strip. The per-key revision timeline does not
/// and never has: it is rendered inline in the value drill-down surface's
/// selected-row panel, behind that row's History button, so it is contributed as
/// a nested surface instead. Registering it in the strip would add a tab the
/// shipped product does not have.
/// </para>
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
            }
        });
    }

    [Test]
    public void Every_selection_plugin_declares_its_own_controlled_domain_contract()
    {
        // The reach of each surface is a compile-time fact stated in its own
        // signature: the host resolves exactly the declared contract for it and
        // nothing else, so no view can reach the state-API connection.
        Assert.Multiple(() =>
        {
            foreach (var plugin in Shipped)
            {
                Assert.That(
                    plugin.DomainContract,
                    Is.Not.Null,
                    $"{plugin.Descriptor.PluginId} must declare a controlled domain model");
                Assert.That(plugin.DomainContract!.IsInterface, Is.True);
            }

            Assert.That(Contract(new MetricsSelectionPlugin()), Is.EqualTo(typeof(IMetricsSurface)));
            Assert.That(Contract(new TopologySelectionPlugin()), Is.EqualTo(typeof(ITopologySurface)));
            Assert.That(Contract(new DataSelectionPlugin()), Is.EqualTo(typeof(IDataSurface)));
            Assert.That(Contract(new DeadLetterSelectionPlugin()), Is.EqualTo(typeof(IDeadLetterSurface)));
            Assert.That(Contract(new TagIndexSelectionPlugin()), Is.EqualTo(typeof(ITagIndexSurface)));
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
            Assert.That(metrics.ViewType.Name, Is.EqualTo("MetricsTab"));

            Assert.That(topology.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.Topology));
            Assert.That(topology.Descriptor.Label, Is.EqualTo("Topology"));
            Assert.That(topology.ViewType.Name, Is.EqualTo("TopologyTab"));

            Assert.That(data.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.Data));
            Assert.That(data.Descriptor.Label, Is.EqualTo("Data"));
            Assert.That(data.ViewType.Name, Is.EqualTo("DataTab"));

            Assert.That(deadLetter.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.DeadLetter));
            Assert.That(deadLetter.Descriptor.Label, Is.EqualTo("Dead-letter"));
            Assert.That(deadLetter.ViewType.Name, Is.EqualTo("DeadLetterTab"));

            Assert.That(tagIndex.Descriptor.PluginId, Is.EqualTo(SelectionPluginKeys.TagIndex));
            Assert.That(tagIndex.Descriptor.Label, Is.EqualTo("Tag index"));
            Assert.That(tagIndex.ViewType.Name, Is.EqualTo("TagIndexDetailTab"));
        });
    }

    [Test]
    public void Every_shipped_view_derives_from_the_shared_selection_view_base()
    {
        // The base carries the selection parameter and the lifetime token the
        // host cancels, so a view that did not derive from it would silently lose
        // the selection-change cancellation contract. The nested timeline is held
        // to the same rule, because it is handed the same parameter.
        Assert.Multiple(() =>
        {
            foreach (var plugin in Shipped)
            {
                Assert.That(
                    typeof(SelectionPluginViewBase).IsAssignableFrom(plugin.ViewType),
                    Is.True,
                    $"{plugin.Descriptor.PluginId}'s view must derive from SelectionPluginViewBase");
            }

            Assert.That(
                typeof(SelectionPluginViewBase).IsAssignableFrom(new EntryHistoryNestedSurface().ViewType),
                Is.True);
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
    public async Task AddExplorerSelectionPlugins_adds_no_strip_entry_for_the_revision_timeline()
    {
        // The timeline is reached from a row, not from the strip. A tab for it
        // would be a behaviour change dressed up as a conversion, so the composite
        // must never produce one.
        await using var provider = BuildHost(services => services.AddExplorerSelectionPlugins());
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                scope.ServiceProvider
                    .GetRequiredService<IExplorerPluginCatalog>()
                    .ForSurface(ExplorerPluginSurface.Selection)
                    .Select(p => p.Descriptor.Label),
                Has.No.Member("History"));

            Assert.That(
                scope.ServiceProvider
                    .GetRequiredService<ISelectionNestedSurfaceRegistry>()
                    .Find(SelectionNestedSurfaceKeys.EntryHistory),
                Is.Not.Null,
                "it is contributed as a nested surface instead");
        });
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
        await using var provider = BuildHost(services => services.AddExplorerDataPlugin());
        await using var scope = provider.CreateAsyncScope();

        Assert.That(
            scope.ServiceProvider
                .GetRequiredService<IExplorerPluginCatalog>()
                .ForSelection(ExplorerPluginSelectionKind.Tree)
                .Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { SelectionPluginKeys.Data }));
    }

    [Test]
    public void Every_per_package_registration_brings_its_own_declared_domain_contract()
    {
        // The head-facing promise of the split: registering one package
        // contributes its own contract, and only its own. Asserted on the
        // registrations rather than by resolving them, because a resolved
        // contract also needs the reader the Explorer core registers separately.
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        services.AddExplorerMetricsPlugin();
        services.AddExplorerTopologyPlugin();
        services.AddExplorerDeadLetterPlugin();

        Assert.Multiple(() =>
        {
            Assert.That(services.Any(d => d.ServiceType == typeof(IMetricsSurface)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(ITopologySurface)), Is.True);
            Assert.That(services.Any(d => d.ServiceType == typeof(IDeadLetterSurface)), Is.True);
            Assert.That(
                services.Any(d => d.ServiceType == typeof(IDataSurface)),
                Is.False,
                "an unregistered package must contribute no contract");
        });
    }

    [Test]
    public async Task A_head_that_withholds_the_timeline_package_registers_no_nested_surface()
    {
        // Withholding it is a complete opt-out: the value drill-down surface finds
        // no view for the nested id and therefore offers no History button.
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        services.AddExplorerDataPlugin();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                scope.ServiceProvider
                    .GetRequiredService<ISelectionNestedSurfaceRegistry>()
                    .Find(SelectionNestedSurfaceKeys.EntryHistory),
                Is.Null);
            Assert.That(services.Any(d => d.ServiceType == typeof(IHistorySurface)), Is.False);
        });
    }

    [Test]
    public async Task AddExplorerHistorySurface_contributes_the_nested_view_and_its_contract()
    {
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        services.AddExplorerHistorySurface();

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        Assert.Multiple(() =>
        {
            Assert.That(
                scope.ServiceProvider
                    .GetRequiredService<ISelectionNestedSurfaceRegistry>()
                    .Find(SelectionNestedSurfaceKeys.EntryHistory),
                Is.EqualTo(new EntryHistoryNestedSurface().ViewType));
            Assert.That(services.Any(d => d.ServiceType == typeof(IHistorySurface)), Is.True);
            Assert.That(
                scope.ServiceProvider.GetServices<IExplorerPlugin>(),
                Is.Empty,
                "the timeline is not a tier plugin");
        });
    }

    [Test]
    public async Task AddExplorerHistorySurface_is_idempotent()
    {
        await using var provider = BuildHost(services =>
        {
            services.AddExplorerHistorySurface();
            services.AddExplorerHistorySurface();
        });
        await using var scope = provider.CreateAsyncScope();

        Assert.That(scope.ServiceProvider.GetServices<ISelectionNestedSurface>().Count(), Is.EqualTo(1));
    }

    [Test]
    public void AddExplorerSelectionPlugins_rejects_a_null_service_collection()
    {
        Assert.That(
            () => ExplorerUiPluginServiceCollectionExtensions.AddExplorerSelectionPlugins(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Every_per_package_registration_rejects_a_null_service_collection()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => MetricsPluginServiceCollectionExtensions.AddExplorerMetricsPlugin(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => TopologyPluginServiceCollectionExtensions.AddExplorerTopologyPlugin(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => DataPluginServiceCollectionExtensions.AddExplorerDataPlugin(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => HistoryPluginServiceCollectionExtensions.AddExplorerHistorySurface(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => DeadLetterPluginServiceCollectionExtensions.AddExplorerDeadLetterPlugin(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => TagIndexPluginServiceCollectionExtensions.AddExplorerTagIndexPlugin(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => SelectionPluginHostServiceCollectionExtensions.AddExplorerSelectionPluginHost(null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => SelectionPluginHostServiceCollectionExtensions
                    .AddExplorerSelectionNestedSurface<EntryHistoryNestedSurface>(null!),
                Throws.ArgumentNullException);
        });
    }

    // The declared contract is supplied by IExplorerPlugin<TDomain> as a default
    // interface member, so it is read through the interface rather than off the
    // concrete type.
    private static Type? Contract(IExplorerPlugin plugin) => plugin.DomainContract;

    private static ServiceProvider BuildHost(Action<IServiceCollection> configure)
    {
        var services = new ServiceCollection();
        services.AddExplorerCatalogStub();
        configure(services);
        return services.BuildServiceProvider();
    }
}
