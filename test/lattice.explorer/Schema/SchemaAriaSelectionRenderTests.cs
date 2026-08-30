using Orleans.Lattice.Explorer.Schema.Domain;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Schema;

/// <summary>
/// The WAI-ARIA selection contract the Schema area's tab strip and tree listbox
/// must satisfy in the markup a browser actually receives.
/// </summary>
/// <remarks>
/// <c>aria-selected</c> is an enumerated attribute taking the literal string
/// <c>"true"</c> or <c>"false"</c>. Blazor renders a <see langword="bool"/> value
/// as an HTML boolean attribute instead - the selected element emits
/// <c>aria-selected=""</c> and every unselected one omits the attribute
/// altogether - which leaves a screen-reader user unable to tell which tab or
/// option is selected. These tests count, because a spot check for
/// "the selected element carries aria-selected" passes on the broken form.
/// </remarks>
[TestFixture]
public sealed class SchemaAriaSelectionRenderTests
{
    [Test]
    public async Task The_tab_strip_states_selection_on_every_tab_not_just_the_active_one()
    {
        var html = await SchemaRenderHarness.RenderPanelAsync(new StubSchemaDomain());
        var aria = PluginAriaMarkup.TallyAriaSelected(html);

        Assert.Multiple(() =>
        {
            Assert.That(PluginAriaMarkup.Count(html, "role=\"tab\""), Is.EqualTo(3));
            Assert.That(aria.Invalid, Is.Zero, "a bare or empty aria-selected is not a valid enumerated value");
            Assert.That(aria.Valid, Is.EqualTo(aria.Total), "every occurrence must read true or false");
            Assert.That(aria.True, Is.EqualTo(1), "exactly one sub-tab is selected");
            Assert.That(
                aria.False,
                Is.EqualTo(2),
                "the two inactive sub-tabs must say so rather than omit the attribute");
        });
    }

    [Test]
    public async Task The_tree_selector_states_selection_on_every_option()
    {
        // role="option" carries the same enumerated requirement as role="tab".
        // No tree is pinned on mount, so every option must read "false" - the
        // state is "nothing selected", not "unknown".
        var domain = new StubSchemaDomain
        {
            Trees = SchemaTreeCatalog.Succeeded(
            [
                new SchemaTreeSummary("orders", "orders", null, null),
                new SchemaTreeSummary("invoices", "invoices", null, null),
            ]),
        };

        var html = await SchemaRenderHarness.RenderPanelAsync(domain);
        var aria = PluginAriaMarkup.TallyAriaSelected(html);

        Assert.Multiple(() =>
        {
            Assert.That(PluginAriaMarkup.Count(html, "role=\"option\""), Is.EqualTo(2));
            Assert.That(aria.Invalid, Is.Zero, "a bare or empty aria-selected is not a valid enumerated value");
            Assert.That(aria.Valid, Is.EqualTo(aria.Total));

            // One active sub-tab, two inactive ones, and two unselected options.
            Assert.That(aria.True, Is.EqualTo(1));
            Assert.That(aria.False, Is.EqualTo(4));
        });
    }
}
