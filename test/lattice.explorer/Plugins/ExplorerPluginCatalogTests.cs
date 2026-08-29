using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

[TestFixture]
public sealed class ExplorerPluginCatalogTests
{
    [Test]
    public void Plugins_sort_by_ordering_hint()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            new FakeExplorerPlugin("c", order: 30),
            new FakeExplorerPlugin("a", order: 10),
            new FakeExplorerPlugin("b", order: 20),
        });

        Assert.That(
            catalog.All.Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { "a", "b", "c" }).AsCollection);
    }

    [Test]
    public void Negative_ordering_hints_sort_before_zero()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            new FakeExplorerPlugin("zero", order: 0),
            new FakeExplorerPlugin("first", order: -100),
        });

        Assert.That(
            catalog.All.Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { "first", "zero" }).AsCollection);
    }

    [Test]
    public void An_ordering_tie_breaks_on_label_then_id_so_the_order_is_total()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            new FakeExplorerPlugin("z", order: 5, label: "Same"),
            new FakeExplorerPlugin("a", order: 5, label: "Same"),
            new FakeExplorerPlugin("m", order: 5, label: "Different"),
        });

        Assert.That(
            catalog.All.Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { "m", "a", "z" }).AsCollection);
    }

    [Test]
    public void Ordering_does_not_depend_on_registration_order()
    {
        string[] Ids(params FakeExplorerPlugin[] plugins) =>
            [.. new ExplorerPluginCatalog(plugins).All.Select(p => p.Descriptor.PluginId)];

        var forward = Ids(
            new FakeExplorerPlugin("a", order: 1),
            new FakeExplorerPlugin("b", order: 1),
            new FakeExplorerPlugin("c", order: 1));

        var reversed = Ids(
            new FakeExplorerPlugin("c", order: 1),
            new FakeExplorerPlugin("b", order: 1),
            new FakeExplorerPlugin("a", order: 1));

        Assert.That(forward, Is.EqualTo(reversed).AsCollection);
    }

    [Test]
    public void ForSurface_filters_to_the_requested_tier_and_keeps_the_order()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            new FakeExplorerPlugin("area-2", ExplorerPluginSurface.Area, order: 20),
            new FakeExplorerPlugin("sel-1", ExplorerPluginSurface.Selection, order: 10),
            new FakeExplorerPlugin("area-1", ExplorerPluginSurface.Area, order: 10),
            new FakeExplorerPlugin("sel-2", ExplorerPluginSurface.Selection, order: 20),
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                catalog.ForSurface(ExplorerPluginSurface.Area).Select(p => p.Descriptor.PluginId),
                Is.EqualTo(new[] { "area-1", "area-2" }).AsCollection);
            Assert.That(
                catalog.ForSurface(ExplorerPluginSurface.Selection).Select(p => p.Descriptor.PluginId),
                Is.EqualTo(new[] { "sel-1", "sel-2" }).AsCollection);
        });
    }

    [Test]
    public void ForSurface_returns_empty_for_an_unoccupied_tier()
    {
        var catalog = new ExplorerPluginCatalog(new[] { new FakeExplorerPlugin("a", ExplorerPluginSurface.Area) });

        Assert.That(catalog.ForSurface(ExplorerPluginSurface.Selection), Is.Empty);
    }

    [Test]
    public void ForSurface_returns_empty_for_an_undefined_tier()
    {
        var catalog = new ExplorerPluginCatalog(new[] { new FakeExplorerPlugin("a") });

        Assert.That(catalog.ForSurface((ExplorerPluginSurface)999), Is.Empty);
    }

    [Test]
    public void ForSurface_returns_the_same_cached_list_on_every_call()
    {
        var catalog = new ExplorerPluginCatalog(new[] { new FakeExplorerPlugin("a", ExplorerPluginSurface.Area) });

        Assert.That(
            catalog.ForSurface(ExplorerPluginSurface.Area),
            Is.SameAs(catalog.ForSurface(ExplorerPluginSurface.Area)));
    }

    [Test]
    public void ForSelection_filters_to_the_surfaces_that_declare_the_kind_and_keeps_the_order()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            Selection("tags", order: 10, kinds: ExplorerPluginSelectionKinds.TagIndex),
            Selection("data", order: 20, kinds: ExplorerPluginSelectionKinds.Tree | ExplorerPluginSelectionKinds.View),
            Selection("deadletter", order: 30, kinds: ExplorerPluginSelectionKinds.Tree),
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                catalog.ForSelection(ExplorerPluginSelectionKind.Tree).Select(p => p.Descriptor.PluginId),
                Is.EqualTo(new[] { "data", "deadletter" }).AsCollection);
            Assert.That(
                catalog.ForSelection(ExplorerPluginSelectionKind.View).Select(p => p.Descriptor.PluginId),
                Is.EqualTo(new[] { "data" }).AsCollection);
            Assert.That(
                catalog.ForSelection(ExplorerPluginSelectionKind.TagIndex).Select(p => p.Descriptor.PluginId),
                Is.EqualTo(new[] { "tags" }).AsCollection);
        });
    }

    [Test]
    public void ForSelection_never_yields_an_area_plugin()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            new FakeExplorerPlugin("area", ExplorerPluginSurface.Area),
            Selection("tab", order: 10, kinds: ExplorerPluginSelectionKinds.All),
        });

        Assert.That(
            catalog.ForSelection(ExplorerPluginSelectionKind.Tree).Select(p => p.Descriptor.PluginId),
            Is.EqualTo(new[] { "tab" }).AsCollection);
    }

    [Test]
    public void ForSelection_returns_empty_for_a_kind_no_plugin_declares()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            Selection("tags", order: 10, kinds: ExplorerPluginSelectionKinds.TagIndex),
        });

        Assert.That(catalog.ForSelection(ExplorerPluginSelectionKind.View), Is.Empty);
    }

    [Test]
    public void ForSelection_returns_empty_for_an_undefined_kind()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            Selection("tab", order: 10, kinds: ExplorerPluginSelectionKinds.All),
        });

        Assert.That(catalog.ForSelection((ExplorerPluginSelectionKind)999), Is.Empty);
    }

    [Test]
    public void ForSelection_returns_the_same_cached_list_on_every_call()
    {
        var catalog = new ExplorerPluginCatalog(new[]
        {
            Selection("tab", order: 10, kinds: ExplorerPluginSelectionKinds.All),
        });

        Assert.That(
            catalog.ForSelection(ExplorerPluginSelectionKind.Tree),
            Is.SameAs(catalog.ForSelection(ExplorerPluginSelectionKind.Tree)),
            "the render path must read a pre-computed projection, never build one");
    }

    private static FakeExplorerPlugin Selection(string id, int order, ExplorerPluginSelectionKinds kinds) =>
        new(id, ExplorerPluginSurface.Selection, order, selectionKinds: kinds);

    [Test]
    public void Find_returns_the_registered_plugin()
    {
        var plugin = new FakeExplorerPlugin("a");
        var catalog = new ExplorerPluginCatalog(new[] { plugin });

        Assert.That(catalog.Find("a"), Is.SameAs(plugin));
    }

    [Test]
    public void Find_returns_null_for_an_unregistered_id()
    {
        var catalog = new ExplorerPluginCatalog(new[] { new FakeExplorerPlugin("a") });

        Assert.That(catalog.Find("missing"), Is.Null);
    }

    [Test]
    public void Find_compares_ids_ordinally()
    {
        var catalog = new ExplorerPluginCatalog(new[] { new FakeExplorerPlugin("backups") });

        Assert.That(catalog.Find("Backups"), Is.Null);
    }

    [Test]
    public void Find_rejects_a_null_id()
    {
        var catalog = new ExplorerPluginCatalog(new[] { new FakeExplorerPlugin("a") });

        Assert.That(() => catalog.Find(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void An_empty_registration_yields_an_empty_catalog()
    {
        var catalog = new ExplorerPluginCatalog([]);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.All, Is.Empty);
            Assert.That(catalog.ForSurface(ExplorerPluginSurface.Area), Is.Empty);
            Assert.That(catalog.Find("a"), Is.Null);
        });
    }

    [Test]
    public void Constructor_rejects_a_null_plugin_sequence()
    {
        Assert.That(() => new ExplorerPluginCatalog(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Duplicate_plugin_ids_fail_fast()
    {
        Assert.That(
            () => new ExplorerPluginCatalog(new[]
            {
                new FakeExplorerPlugin("clash"),
                new FakeExplorerPlugin("clash"),
            }),
            Throws.InvalidOperationException.With.Message.Contains("clash"));
    }

    [Test]
    public void A_null_plugin_fails_fast()
    {
        Assert.That(
            () => new ExplorerPluginCatalog(new IExplorerPlugin[] { null! }),
            Throws.InvalidOperationException);
    }

    [Test]
    public void A_plugin_with_no_view_type_fails_fast()
    {
        Assert.That(
            () => new ExplorerPluginCatalog(new IExplorerPlugin[] { new IncompletePlugin("a", withView: false) }),
            Throws.InvalidOperationException.With.Message.Contains("view type"));
    }

    [Test]
    public void A_plugin_with_no_access_gate_fails_fast()
    {
        Assert.That(
            () => new ExplorerPluginCatalog(new IExplorerPlugin[] { new IncompletePlugin("a", withGate: false) }),
            Throws.InvalidOperationException.With.Message.Contains("access gate"));
    }

    [Test]
    public void A_plugin_with_no_descriptor_fails_fast()
    {
        Assert.That(
            () => new ExplorerPluginCatalog(new IExplorerPlugin[] { new IncompletePlugin("a", withDescriptor: false) }),
            Throws.InvalidOperationException.With.Message.Contains("descriptor"));
    }

    /// <summary>
    /// A deliberately malformed plugin: the contract's non-nullable members are
    /// only compile-time guarantees, so the catalog still has to reject a
    /// plugin that returns null from one of them.
    /// </summary>
    private sealed class IncompletePlugin(
        string pluginId,
        bool withDescriptor = true,
        bool withView = true,
        bool withGate = true) : IExplorerPlugin
    {
        public ExplorerPluginDescriptor Descriptor => withDescriptor
            ? new ExplorerPluginDescriptor
            {
                PluginId = pluginId,
                Label = pluginId,
                Surface = ExplorerPluginSurface.Area,
            }
            : null!;

        public Type ViewType => withView ? typeof(IncompletePlugin) : null!;

        public Type? DomainContract => null;

        public IExplorerPluginAccessGate AccessGate => withGate ? ExplorerPluginAccessGates.Denied : null!;
    }
}
