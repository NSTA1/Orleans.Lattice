using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Plugins.MyTenant;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The plugin's declared sub-surfaces and the tab items rendered for them.
/// </summary>
[TestFixture]
public sealed class MyTenantSurfacesTests
{
    [Test]
    public void Every_declared_surface_has_a_tab_in_display_order() =>
        Assert.That(
            MyTenantSurfaces.Tabs.Select(tab => tab.Id).ToArray(),
            Is.EqualTo(new[]
            {
                MyTenantSurfaces.Overview,
                MyTenantSurfaces.Members,
                MyTenantSurfaces.Quota,
                MyTenantSurfaces.Regions,
                MyTenantSurfaces.Sharing,
                MyTenantSurfaces.Metrics,
            }));

    [Test]
    public void The_metrics_tab_ships_from_the_start_so_the_strip_does_not_shift_later() =>
        // Adding it once the metrics work lands would move every tab beside it
        // and change where the compact strip overflows.
        Assert.That(MyTenantSurfaces.Tabs.Any(tab => tab.Id == MyTenantSurfaces.Metrics), Is.True);

    [Test]
    public void Every_tab_is_labelled_described_and_enabled() =>
        Assert.Multiple(() =>
        {
            foreach (var tab in MyTenantSurfaces.Tabs)
            {
                Assert.That(tab.Label, Is.Not.Empty, tab.Id);
                Assert.That(tab.Description, Is.Not.Null.And.Not.Empty, tab.Id);
                Assert.That(tab.IsEnabled, Is.True, tab.Id);
            }
        });

    [Test]
    public void Tab_ids_and_labels_are_unique() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantSurfaces.Tabs.Select(tab => tab.Id).ToArray(), Is.Unique);
            Assert.That(MyTenantSurfaces.Tabs.Select(tab => tab.Label).ToArray(), Is.Unique);
        });

    [Test]
    public void The_tab_list_is_a_cached_instance_so_a_render_allocates_nothing() =>
        Assert.That(MyTenantSurfaces.Tabs, Is.SameAs(MyTenantSurfaces.Tabs));

    [Test]
    public void Every_tab_id_is_recognised() =>
        Assert.Multiple(() =>
        {
            foreach (var tab in MyTenantSurfaces.Tabs)
            {
                Assert.That(MyTenantSurfaces.IsKnown(tab.Id), Is.True, tab.Id);
            }
        });

    [Test]
    public void An_unknown_surface_id_is_rejected() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantSurfaces.IsKnown("retired"), Is.False);
            Assert.That(MyTenantSurfaces.IsKnown(null), Is.False);
            Assert.That(MyTenantSurfaces.IsKnown(string.Empty), Is.False);
            Assert.That(MyTenantSurfaces.IsKnown("OVERVIEW"), Is.False, "matching is ordinal");
        });

    [Test]
    public void Every_declared_surface_names_a_glossary_term_to_explain_it() =>
        Assert.Multiple(() =>
        {
            foreach (var tab in MyTenantSurfaces.Tabs)
            {
                var term = MyTenantSurfaces.TermFor(tab.Id);
                Assert.That(term, Is.Not.Null, tab.Id);
                Assert.That(term!.Explanation, Is.Not.Empty, tab.Id);
            }
        });

    [Test]
    public void An_unknown_surface_names_no_term() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantSurfaces.TermFor("retired"), Is.Null);
            Assert.That(MyTenantSurfaces.TermFor(null), Is.Null);
        });

    [Test]
    public void Every_declared_surface_has_a_distinct_help_element_id()
    {
        var ids = MyTenantSurfaces.Tabs
            .Select(tab => MyTenantSurfaces.HelpIdFor(tab.Id))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(ids, Is.Unique);
            Assert.That(ids, Has.None.Null);

            foreach (var id in ids)
            {
                Assert.That(id, Does.StartWith(MyTenantSurfaces.HelpIdPrefix));

                // An element id, not a class: an lx-prefixed literal would be
                // read as a class the repository's orphan-class gate then
                // demands a rule for.
                Assert.That(id, Does.Not.StartWith("lx"));
            }
        });
    }

    [Test]
    public void An_unknown_surface_names_no_help_element_id() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantSurfaces.HelpIdFor("retired"), Is.Null);
            Assert.That(MyTenantSurfaces.HelpIdFor(null), Is.Null);
        });

    [Test]
    public void A_help_element_id_is_a_constant_rather_than_composed_per_render() =>
        // Reference equality proves the arms are compile-time constants, so the
        // panel spends no allocation composing one per render.
        Assert.That(
            MyTenantSurfaces.HelpIdFor(MyTenantSurfaces.Sharing),
            Is.SameAs(MyTenantSurfaces.HelpIdFor(MyTenantSurfaces.Sharing)));

    [Test]
    public void Every_sub_surface_id_is_a_canonical_lower_case_slug() =>
        Assert.Multiple(() =>
        {
            foreach (var tab in MyTenantSurfaces.Tabs)
            {
                Assert.That(tab.Id, Is.EqualTo(tab.Id.ToLowerInvariant()), tab.Id);
                Assert.That(tab.Id, Does.Not.Contain(" "), tab.Id);
            }
        });
}

/// <summary>
/// The My tenant plugin's key vocabulary: the area slug it is addressed by, the
/// query key that makes its open sub-surface bookmarkable, and the preference
/// key that remembers it.
/// </summary>
[TestFixture]
public sealed class MyTenantPluginKeysTests
{
    [Test]
    public void The_area_slug_is_derived_the_way_the_shell_derives_it() =>
        Assert.That(MyTenantPluginKeys.AreaSlug, Is.EqualTo("mytenant"));

    [Test]
    public void The_query_key_is_canonical_lower_case_and_names_this_area() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantPluginKeys.SurfaceQueryKey, Is.EqualTo("my-tenant-surface"));
            Assert.That(
                MyTenantPluginKeys.SurfaceQueryKey,
                Is.EqualTo(MyTenantPluginKeys.SurfaceQueryKey.ToLowerInvariant()));
        });

    [Test]
    public void The_preference_key_is_scoped_per_user_and_cluster() =>
        Assert.Multiple(() =>
        {
            Assert.That(MyTenantPluginKeys.SurfacePreference.Name, Is.EqualTo("mytenant.surface"));
            Assert.That(
                MyTenantPluginKeys.SurfacePreference.Scope,
                Is.EqualTo(ExplorerPreferenceScope.UserAndCluster));
            Assert.That(MyTenantPluginKeys.SurfacePreference.Description, Is.Not.Empty);
        });

    [Test]
    public void The_preference_key_is_one_shared_instance() =>
        // Preference keys are compared by reference, so a second instance with
        // the same name would be rejected by the catalog.
        Assert.That(
            MyTenantPluginKeys.SurfacePreference,
            Is.SameAs(MyTenantPluginKeys.SurfacePreference));
}
