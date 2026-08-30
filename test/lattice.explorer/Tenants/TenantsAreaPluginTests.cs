using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Views;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The Tenants area plugin's descriptor, view, declared reach, and gate, plus
/// the sub-surface vocabulary its panel renders.
/// </summary>
[TestFixture]
public sealed class TenantsAreaPluginTests
{
    private static TenantsAreaPlugin Plugin() => new(new TenantsAccessGate(new FakeTenancyDomain()));

    [Test]
    public void Constructor_null_gate_throws()
    {
        Assert.That(() => new TenantsAreaPlugin(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void The_descriptor_places_the_area_last_in_the_area_tier()
    {
        var descriptor = Plugin().Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.PluginId, Is.EqualTo(TenantsPluginKeys.PluginId));
            Assert.That(descriptor.PluginId, Is.EqualTo("orleans.lattice.tenants"));
            Assert.That(descriptor.Label, Is.EqualTo("Tenants"));
            Assert.That(descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
            Assert.That(descriptor.Order, Is.EqualTo(400));
        });
    }

    [Test]
    public void The_descriptor_is_shared_rather_than_rebuilt_per_read()
    {
        var plugin = Plugin();

        Assert.That(plugin.Descriptor, Is.SameAs(plugin.Descriptor));
    }

    [Test]
    public void The_plugin_declares_the_tenancy_domain_as_its_whole_reach()
    {
        // Declared through IExplorerPlugin<TDomain>, so the reach is a
        // compile-time fact stated once in the plugin's signature (epic decision
        // D3) rather than a value it has to remember to set.
        IExplorerPlugin plugin = Plugin();

        Assert.That(plugin.DomainContract, Is.EqualTo(typeof(ITenancyDomain)));
    }

    [Test]
    public void The_plugin_renders_its_own_panel()
    {
        Assert.That(Plugin().ViewType, Is.EqualTo(typeof(TenantsPanel)));
    }

    [Test]
    public void The_plugin_carries_its_own_gate()
    {
        var gate = new TenantsAccessGate(new FakeTenancyDomain());

        Assert.That(new TenantsAreaPlugin(gate).AccessGate, Is.SameAs(gate));
    }
}

/// <summary>
/// The Tenants plugin's internal sub-surface vocabulary.
/// </summary>
[TestFixture]
public sealed class TenantsSurfacesTests
{
    [Test]
    public void The_strip_lists_every_sub_surface_in_display_order()
    {
        var ids = TenantsSurfaces.Tabs.Select(tab => tab.Id).ToArray();

        Assert.That(ids, Is.EqualTo(new[]
        {
            TenantsSurfaces.Tenants,
            TenantsSurfaces.Quotas,
            TenantsSurfaces.Regions,
            TenantsSurfaces.Access,
        }));
    }

    [Test]
    public void The_strip_is_one_shared_cached_list()
    {
        Assert.That(TenantsSurfaces.Tabs, Is.SameAs(TenantsSurfaces.Tabs));
    }

    [Test]
    public void Every_tab_carries_a_label_and_an_explanation()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in TenantsSurfaces.Tabs)
            {
                Assert.That(tab.Label, Is.Not.Empty, tab.Id);
                Assert.That(tab.Description, Is.Not.Null.And.Not.Empty, tab.Id);
                Assert.That(tab.IsEnabled, Is.True, tab.Id);
            }
        });
    }

    [Test]
    public void The_tenant_list_is_the_only_surface_that_stands_alone()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsSurfaces.RequiresTenant(TenantsSurfaces.Tenants), Is.False);
            Assert.That(TenantsSurfaces.RequiresTenant(TenantsSurfaces.Quotas), Is.True);
            Assert.That(TenantsSurfaces.RequiresTenant(TenantsSurfaces.Regions), Is.True);
            Assert.That(TenantsSurfaces.RequiresTenant(TenantsSurfaces.Access), Is.True);
        });
    }

    [Test]
    public void The_sub_surface_ids_are_distinct()
    {
        Assert.That(TenantsSurfaces.Tabs.Select(tab => tab.Id), Is.Unique);
    }
}
