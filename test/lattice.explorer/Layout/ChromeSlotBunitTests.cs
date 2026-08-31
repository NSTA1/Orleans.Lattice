using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// The banner region that renders whatever a feature contributed to a
/// placement, asserted through the parsed DOM.
/// </summary>
/// <remarks>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ChromeSlotBunitTests : LatticeComponentTestContext
{
    [Test]
    public void A_region_renders_what_was_contributed_to_it()
    {
        Services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.TenantScope);

        var cut = Render<ChromeSlot>(parameters => parameters
            .Add(slot => slot.Placement, ExplorerChromeSlotPlacement.TenantScope));

        Assert.That(cut.Find("[data-stub=scope]"), Is.Not.Null);
    }

    [Test]
    public void A_region_renders_nothing_of_another_regions_contribution()
    {
        Services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.TenantScope);

        var cut = Render<ChromeSlot>(parameters => parameters
            .Add(slot => slot.Placement, ExplorerChromeSlotPlacement.ViewSettings));

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void A_region_renders_nothing_when_no_catalog_is_registered()
    {
        // The default shape of a deployment that contributes no chrome. The
        // banner must still render, so the region resolves the catalog
        // optionally rather than requiring it.
        var cut = Render<ChromeSlot>(parameters => parameters
            .Add(slot => slot.Placement, ExplorerChromeSlotPlacement.TenantScope));

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void Two_contributions_to_one_region_render_in_order()
    {
        Services.AddExplorerChromeSlot<StubSettings>(ExplorerChromeSlotPlacement.ViewSettings, order: 20);
        Services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.ViewSettings, order: 10);

        var cut = Render<ChromeSlot>(parameters => parameters
            .Add(slot => slot.Placement, ExplorerChromeSlotPlacement.ViewSettings));

        var stubs = cut.FindAll("[data-stub]").Select(element => element.GetAttribute("data-stub")).ToArray();

        Assert.That(stubs, Is.EqualTo(new[] { "scope", "settings" }));
    }

    private sealed class StubScope : ComponentBase
    {
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddAttribute(1, "data-stub", "scope");
            builder.CloseElement();
        }
    }

    private sealed class StubSettings : ComponentBase
    {
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "span");
            builder.AddAttribute(1, "data-stub", "settings");
            builder.CloseElement();
        }
    }
}
