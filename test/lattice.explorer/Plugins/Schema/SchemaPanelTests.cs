using Orleans.Lattice.Explorer.DesignSystem.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Schema.Components;
using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Schema;

// The harness reads the render tree to assert what a component actually
// rendered; see ComponentTestRenderer for why that is worth the
// framework-internal API.
#pragma warning disable BL0006

namespace Orleans.Lattice.Explorer.Tests.Plugins.Schema;

/// <summary>
/// The Schema area's shell component: the sub-surface strip, the plugin-level
/// gate, and the delegation of each concern to its own component.
/// <para>
/// Every transition is driven explicitly on the renderer's dispatcher and every
/// domain call answers synchronously, so nothing here depends on timing,
/// ordering, or a wall clock.
/// </para>
/// </summary>
/// <remarks>
/// The strip is the design system's shared tab primitive rather than markup
/// this panel owns (issue #1857), so a surface transition is driven through the
/// panel's own <see cref="SchemaPanel.SelectSurfaceAsync"/> - which is what the
/// strip invokes - instead of by clicking a button in the panel's own render
/// tree. Asserting on the primitive's internals here would be testing the
/// primitive, which has its own tests.
/// </remarks>
[TestFixture]
public sealed class SchemaPanelTests
{
    [Test]
    public async Task The_panel_renders_three_sub_surfaces_with_policy_active()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();

        var (id, panel) = await harness.RenderWithInstanceAsync<SchemaPanel>();
        var strip = harness.Renderer.FindComponent<LatticeAdaptiveTabs>(id);

        Assert.Multiple(() =>
        {
            Assert.That(strip, Is.Not.Null, "the area offers its surfaces through the shared primitive");
            Assert.That(
                strip!.Value.Component.Tabs?.Select(t => t.Label),
                Is.EqualTo(new[] { "Policy", "Versions", "Dead letters" }));
            Assert.That(strip.Value.Component.ActiveId, Is.EqualTo(SchemaSurfaces.Policy));
            Assert.That(panel.ActiveSurfaceId, Is.EqualTo(SchemaSurfaces.Policy));
        });
    }

    [Test]
    public async Task The_sub_surface_strip_is_the_subordinate_variant_the_shell_reserves_for_a_plugin()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();

        var id = await harness.RenderAsync<SchemaPanel>();
        var strip = harness.Renderer.FindComponent<LatticeAdaptiveTabs>(id);

        Assert.Multiple(() =>
        {
            Assert.That(strip, Is.Not.Null);
            Assert.That(
                strip!.Value.Component.Variant,
                Is.EqualTo(LatticeTabsVariant.Subordinate),
                "a plugin's own surfaces must not read as a fourth peer strip");
            Assert.That(
                strip.Value.Component.PanelId,
                Is.EqualTo(SchemaSurfaces.PanelElementId),
                "a tab that controls nothing leaves a screen-reader caller nowhere to move into");
        });
    }

    [Test]
    public async Task A_denied_plugin_level_gate_loads_no_trees()
    {
        using var harness = SchemaComponentHarness.Create();

        await harness.RenderAsync<SchemaPanel>();

        Assert.That(
            harness.Domain.ListCallCount,
            Is.Zero,
            "the area must not reach the cluster while its coarse gate is closed");
    }

    [Test]
    public async Task An_allowed_plugin_level_gate_loads_the_governable_trees_on_mount()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();

        await harness.RenderAsync<SchemaPanel>();

        Assert.That(harness.Domain.ListCallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task A_gate_that_opens_after_mount_loads_the_trees_without_a_manual_refresh()
    {
        using var harness = SchemaComponentHarness.Create();
        await harness.RenderAsync<SchemaPanel>();
        var beforeSignIn = harness.Domain.ListCallCount;

        await harness.Renderer.OnDispatcherAsync(harness.Allow);

        Assert.Multiple(() =>
        {
            Assert.That(beforeSignIn, Is.Zero);
            Assert.That(harness.Domain.ListCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_sibling_plugins_decision_never_reloads_the_area()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();
        await harness.RenderAsync<SchemaPanel>();

        await harness.Renderer.OnDispatcherAsync(
            () => harness.Store.Set("orleans.lattice.backups", ExplorerPluginAccess.Allowed));

        Assert.That(harness.Domain.ListCallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task This_areas_own_scoped_decision_never_reloads_the_area()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();
        await harness.RenderAsync<SchemaPanel>();

        await harness.Renderer.OnDispatcherAsync(() => harness.Store.Set(
            SchemaTreeGrants.KeyFor("orders", SchemaCapability.ManagePolicy),
            ExplorerPluginAccess.Allowed));

        Assert.That(
            harness.Domain.ListCallCount,
            Is.EqualTo(1),
            "a scoped grant is read on the render path, not re-loaded from");
    }

    [Test]
    public async Task The_panel_delegates_tree_selection_and_the_active_concern_to_child_components()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();

        var id = await harness.RenderAsync<SchemaPanel>();

        Assert.That(
            harness.Renderer.ChildComponents(id).Select(c => c.GetType()),
            Does.Contain(typeof(SchemaTreeSelector)).And.Contain(typeof(SchemaPolicyTab)));
    }

    [Test]
    public async Task Activating_a_sub_surface_swaps_the_concern_component()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();
        var (id, panel) = await harness.RenderWithInstanceAsync<SchemaPanel>();

        await harness.Renderer.Dispatcher.InvokeAsync(
            () => panel.SelectSurfaceAsync(SchemaSurfaces.Versions));

        Assert.Multiple(() =>
        {
            Assert.That(panel.ActiveSurfaceId, Is.EqualTo(SchemaSurfaces.Versions));
            Assert.That(
                harness.Renderer.ChildComponents(id).Select(c => c.GetType()),
                Does.Contain(typeof(SchemaVersionsTab)).And.Not.Contain(typeof(SchemaPolicyTab)));
        });
    }

    [Test]
    public async Task Activating_the_dead_letters_surface_renders_the_dead_letter_concern()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();
        var (id, panel) = await harness.RenderWithInstanceAsync<SchemaPanel>();

        await harness.Renderer.Dispatcher.InvokeAsync(
            () => panel.SelectSurfaceAsync(SchemaSurfaces.DeadLetters));

        Assert.That(
            harness.Renderer.ChildComponents(id).Select(c => c.GetType()),
            Does.Contain(typeof(SchemaDeadLettersTab)));
    }

    [Test]
    public async Task Re_activating_the_open_sub_surface_is_a_no_op()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();
        var (_, panel) = await harness.RenderWithInstanceAsync<SchemaPanel>();

        await harness.Renderer.Dispatcher.InvokeAsync(
            () => panel.SelectSurfaceAsync(SchemaSurfaces.Policy));

        Assert.Multiple(() =>
        {
            Assert.That(panel.ActiveSurfaceId, Is.EqualTo(SchemaSurfaces.Policy));
            Assert.That(harness.Renderer.Exceptions, Is.Empty);
        });
    }

    [Test]
    public async Task A_surface_slug_this_area_does_not_offer_is_ignored()
    {
        using var harness = SchemaComponentHarness.Create();
        harness.Allow();
        var (_, panel) = await harness.RenderWithInstanceAsync<SchemaPanel>();

        await harness.Renderer.Dispatcher.InvokeAsync(
            () => panel.SelectSurfaceAsync("not-a-surface"));

        Assert.Multiple(() =>
        {
            Assert.That(
                panel.ActiveSurfaceId,
                Is.EqualTo(SchemaSurfaces.Policy),
                "a value that was never rendered must not open a surface");
            Assert.That(harness.Renderer.Exceptions, Is.Empty);
        });
    }

    [Test]
    public async Task The_panel_surfaces_no_unhandled_error_when_its_gate_is_closed()
    {
        using var harness = SchemaComponentHarness.Create();

        var id = await harness.RenderAsync<SchemaPanel>();
        var strip = harness.Renderer.FindComponent<LatticeAdaptiveTabs>(id);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Renderer.Exceptions, Is.Empty);
            Assert.That(strip, Is.Not.Null, "the surface strip stays navigable");
            Assert.That(strip!.Value.Component.Tabs, Has.Count.EqualTo(3));
        });
    }
}
