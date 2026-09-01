using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Views;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The tenant administration area plugin's descriptor, view, declared reach, and
/// gate, plus the sub-surface vocabulary its panel renders.
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
            Assert.That(descriptor.Surface, Is.EqualTo(ExplorerPluginSurface.Area));
            Assert.That(descriptor.Order, Is.EqualTo(400));
        });
    }

    [Test]
    public void The_descriptor_carries_the_settled_area_name_and_not_the_retired_one()
    {
        var descriptor = Plugin().Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Label, Is.EqualTo(ExplorerVocabulary.TenantAdministrationArea));
            Assert.That(descriptor.Label, Is.EqualTo("Tenant administration"));

            // The bare word said nothing about which of the two tenancy areas
            // administered whose tenants, and the shell rendered both in one
            // strip.
            Assert.That(descriptor.Label, Is.Not.EqualTo("Tenants"));
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
/// The tenant administration plugin's key vocabulary: the area slug it is
/// addressed by, the query key that makes its open sub-surface bookmarkable, and
/// the preference key that remembers it.
/// </summary>
[TestFixture]
public sealed class TenantsPluginKeysTests
{
    [Test]
    public void The_area_slug_is_derived_the_way_the_shell_derives_it()
    {
        Assert.That(TenantsPluginKeys.AreaSlug, Is.EqualTo("tenants"));
    }

    [Test]
    public void The_query_key_is_canonical_lower_case_and_names_this_area()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsPluginKeys.SurfaceQueryKey, Is.EqualTo("tenant-admin-surface"));
            Assert.That(
                TenantsPluginKeys.SurfaceQueryKey,
                Is.EqualTo(TenantsPluginKeys.SurfaceQueryKey.ToLowerInvariant()));
        });
    }

    [Test]
    public void The_preference_key_is_scoped_per_user_and_cluster()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsPluginKeys.SurfacePreference.Name, Is.EqualTo("tenants.surface"));
            Assert.That(
                TenantsPluginKeys.SurfacePreference.Scope,
                Is.EqualTo(ExplorerPreferenceScope.UserAndCluster));
            Assert.That(TenantsPluginKeys.SurfacePreference.Description, Is.Not.Empty);
        });
    }

    [Test]
    public void The_preference_key_is_one_shared_instance()
    {
        // Preference keys are compared by reference, so a second instance with
        // the same name would be rejected by the catalog.
        Assert.That(
            TenantsPluginKeys.SurfacePreference,
            Is.SameAs(TenantsPluginKeys.SurfacePreference));
    }
}

/// <summary>
/// The tenant administration plugin's internal sub-surface vocabulary.
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
            TenantsSurfaces.Overview,
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
    public void The_root_surface_is_named_Overview_rather_than_repeating_the_area()
    {
        var root = TenantsSurfaces.Tabs[0];

        Assert.Multiple(() =>
        {
            Assert.That(root.Id, Is.EqualTo("overview"));
            Assert.That(root.Label, Is.EqualTo("Overview"));

            // The measured defect: the area's own first sub-surface was also
            // called "Tenants", so the word appeared twice in adjacent tiers.
            Assert.That(root.Label, Is.Not.EqualTo("Tenants"));
        });
    }

    [Test]
    public void The_tenant_list_is_the_only_surface_that_stands_alone()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsSurfaces.RequiresTenant(TenantsSurfaces.Overview), Is.False);
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

    [Test]
    public void Every_sub_surface_id_is_a_canonical_lower_case_slug()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in TenantsSurfaces.Tabs)
            {
                Assert.That(tab.Id, Is.EqualTo(tab.Id.ToLowerInvariant()), tab.Id);
                Assert.That(tab.Id, Does.Not.Contain(" "), tab.Id);
            }
        });
    }

    [Test]
    public void Only_a_declared_surface_is_known()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in TenantsSurfaces.Tabs)
            {
                Assert.That(TenantsSurfaces.IsKnown(tab.Id), Is.True, tab.Id);
            }

            // The retired id must not resolve, so a remembered "tenants" reopens
            // on the default rather than on nothing.
            Assert.That(TenantsSurfaces.IsKnown("tenants"), Is.False);
            Assert.That(TenantsSurfaces.IsKnown("nope"), Is.False);
            Assert.That(TenantsSurfaces.IsKnown(""), Is.False);
            Assert.That(TenantsSurfaces.IsKnown(null), Is.False);
        });
    }

    [Test]
    public void Every_declared_surface_names_a_glossary_term_to_explain_it()
    {
        Assert.Multiple(() =>
        {
            foreach (var tab in TenantsSurfaces.Tabs)
            {
                var term = TenantsSurfaces.TermFor(tab.Id);
                Assert.That(term, Is.Not.Null, tab.Id);
                Assert.That(term!.Explanation, Is.Not.Empty, tab.Id);
            }
        });
    }

    [Test]
    public void An_unknown_surface_names_no_term()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsSurfaces.TermFor("nope"), Is.Null);
            Assert.That(TenantsSurfaces.TermFor(null), Is.Null);
        });
    }

    [Test]
    public void Every_declared_surface_has_a_distinct_help_element_id()
    {
        var ids = TenantsSurfaces.Tabs
            .Select(tab => TenantsSurfaces.HelpIdFor(tab.Id))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.Unique);
            Assert.That(ids, Has.None.Null);

            foreach (var id in ids)
            {
                Assert.That(id, Does.StartWith(TenantsSurfaces.HelpIdPrefix));

                // An element id, not a class: an lx-prefixed literal would be
                // read as a class the repository's orphan-class gate then
                // demands a rule for.
                Assert.That(id, Does.Not.StartWith("lx"));
            }
        });
    }

    [Test]
    public void An_unknown_surface_names_no_help_element_id()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsSurfaces.HelpIdFor("nope"), Is.Null);
            Assert.That(TenantsSurfaces.HelpIdFor(null), Is.Null);
        });
    }

    [Test]
    public void A_help_element_id_is_a_constant_rather_than_composed_per_render()
    {
        // Reference equality proves the arms are compile-time constants, so the
        // panel spends no allocation composing one per render.
        Assert.That(
            TenantsSurfaces.HelpIdFor(TenantsSurfaces.Quotas),
            Is.SameAs(TenantsSurfaces.HelpIdFor(TenantsSurfaces.Quotas)));
    }
}
