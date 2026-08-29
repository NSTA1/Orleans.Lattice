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
}
