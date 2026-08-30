using Bunit;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// Activating an area tab in the shell, driven through bUnit's DOM click rather
/// than a hand-dispatched render-tree event.
/// </summary>
/// <remarks>
/// <para>
/// This is the interaction exemplar for the bUnit pattern. The string harnesses
/// cannot click - they render once to static markup - so the render-tree
/// counterpart in <see cref="AppShellTests"/> reaches into framework-internal
/// frame types through a bespoke <c>ComponentTestRenderer</c> to find an
/// <c>onclick</c> handler id and dispatch to it (behind a <c>#pragma warning
/// disable BL0006</c>). bUnit does the same thing with <c>Find(...).Click()</c>:
/// it locates the element in the parsed DOM and dispatches the bound handler,
/// then re-renders, with no framework-internal API and no manual handler-id
/// bookkeeping.
/// </para>
/// <para>
/// The assertion reads the result from the parsed DOM: after the click the
/// clicked tab reports <c>aria-selected="true"</c> and the home surface the shell
/// rendered before activation is gone, replaced by the active plugin's view.
/// </para>
/// </remarks>
[TestFixture]
public sealed class AppShellActivationBunitTests : LatticeComponentTestContext
{
    [Test]
    public void Clicking_an_allowed_tab_selects_it_and_replaces_the_home_surface()
    {
        ConfigureShellServices(AreaPlugin("a", "Alpha", 100));

        var cut = RenderShell();

        Assert.That(
            cut.FindAll("#detail-surface"),
            Has.Count.EqualTo(1),
            "the home surface renders before any activation");

        var alpha = cut.FindAll("[role=tab]")
            .Single(tab => tab.TextContent.Trim() == "Alpha");
        alpha.Click();

        var alphaAfter = cut.FindAll("[role=tab]")
            .Single(tab => tab.TextContent.Trim() == "Alpha");

        Assert.Multiple(() =>
        {
            Assert.That(
                alphaAfter.GetAttribute("aria-selected"),
                Is.EqualTo("true"),
                "the clicked tab becomes the selected one");
            Assert.That(
                cut.FindAll("#detail-surface"),
                Is.Empty,
                "and the home surface is replaced by the activated plugin's view");
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
            builder.AddAttribute(1, "id", "detail-surface");
            builder.CloseElement();
        });

        return Render<AppShell>(parameters => parameters
            .AddCascadingValue(AdaptiveContext(LatticeBreakpoint.Expanded))
            .Add(shell => shell.Catalog, catalog)
            .Add(shell => shell.ChildContent, detail));
    }
}
