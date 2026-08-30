using AngleSharp.Dom;
using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The ARIA state the shell's area strip publishes (issue #1793), asserted
/// through bUnit's parsed DOM rather than a raw markup string.
/// </summary>
/// <remarks>
/// <para>
/// This is the bUnit exemplar for issue #1793. The string-based version in
/// <see cref="AppShellReflowTests"/> had to <em>derive</em> an invalid count by
/// subtracting the two valid spellings from the total, because the static
/// renderer emits the bare attribute name and a naive
/// <c>Does.Not.Contain("aria-selected=\"\"")</c> guard is vacuous - it can never
/// fire, since the empty-string form is what a browser DOM reports after parsing,
/// not what the renderer emits.
/// </para>
/// <para>
/// bUnit parses the markup through AngleSharp into a real DOM, so
/// <c>tab.GetAttribute("aria-selected")</c> returns the browser value directly:
/// <c>"true"</c>, <c>"false"</c>, or <c>""</c> for a bare boolean attribute. The
/// assertion below reads exactly like the guarantee it enforces - every tab
/// states an explicit <c>"true"</c>/<c>"false"</c> - with no derived arithmetic
/// and no knowledge of the raw-markup quirk.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AppShellAriaSelectedBunitTests : LatticeComponentTestContext
{
    [Test]
    public void The_area_strip_publishes_an_explicit_aria_selected_state_on_every_tab()
    {
        ConfigureShellServices(
            AreaPlugin("a", "Alpha", 100),
            AreaPlugin("b", "Bravo", 200));

        var cut = RenderShell();

        var tablist = cut.Find("[role=tablist]");
        var tabs = cut.FindAll("[role=tab]");

        Assert.Multiple(() =>
        {
            Assert.That(tablist, Is.Not.Null);
            Assert.That(tabs, Has.Count.EqualTo(3), "the home surface and both areas");

            foreach (var tab in tabs)
            {
                var value = tab.GetAttribute("aria-selected");
                Assert.That(
                    value,
                    Is.EqualTo("true").Or.EqualTo("false"),
                    "aria-selected is enumerated: every tab must state an explicit "
                        + "\"true\"/\"false\", never a bare or empty boolean attribute");
            }

            Assert.That(
                tabs.Count(tab => tab.GetAttribute("aria-selected") == "true"),
                Is.EqualTo(1),
                "exactly one area is selected");
        });
    }

    private IRenderedComponent<AppShell> RenderShell()
    {
        var catalog = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "nav");
            builder.AddContent(1, "catalog-surface");
            builder.CloseElement();
        });

        var detail = (RenderFragment)(builder =>
        {
            builder.OpenElement(0, "section");
            builder.AddContent(1, "detail-surface");
            builder.CloseElement();
        });

        return Render<AppShell>(parameters => parameters
            .AddCascadingValue(AdaptiveContext(LatticeBreakpoint.Expanded))
            .Add(shell => shell.Catalog, catalog)
            .Add(shell => shell.ChildContent, detail));
    }
}
