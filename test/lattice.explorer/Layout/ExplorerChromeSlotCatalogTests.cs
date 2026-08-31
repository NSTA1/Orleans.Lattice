using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// The banner's slot catalog: grouped once, so a region lookup on the render
/// path is an array index rather than a filter.
/// </summary>
[TestFixture]
public sealed class ExplorerChromeSlotCatalogTests
{
    [Test]
    public void A_region_yields_only_what_was_contributed_to_it()
    {
        var scope = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.TenantScope, typeof(StubScope));
        var settings = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.ViewSettings, typeof(StubSettings));

        var catalog = new ExplorerChromeSlotCatalog([scope, settings]);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.TenantScope), Is.EqualTo(new[] { scope }));
            Assert.That(
                catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings),
                Is.EqualTo(new[] { settings }));
        });
    }

    [Test]
    public void An_empty_region_yields_an_empty_list_rather_than_null()
    {
        var catalog = new ExplorerChromeSlotCatalog([]);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.TenantScope), Is.Empty);
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings), Is.Empty);
        });
    }

    [Test]
    public void Contributions_are_ordered_by_hint_then_by_registration()
    {
        var third = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.ViewSettings, typeof(StubScope), 10);
        var first = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.ViewSettings, typeof(StubSettings), 1);
        var second = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.ViewSettings, typeof(StubChrome), 1);

        var catalog = new ExplorerChromeSlotCatalog([third, first, second]);

        Assert.That(
            catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings),
            Is.EqualTo(new[] { first, second, third }),
            "equal hints keep the order the heads registered them in");
    }

    [Test]
    public void A_placement_outside_the_declared_set_is_dropped_rather_than_misplaced()
    {
        // Only reachable by casting an integer. Dropping it is the fail-closed
        // answer: the banner renders no region for it, so nothing lands somewhere
        // the shell never promised.
        var stray = new ExplorerChromeSlot((ExplorerChromeSlotPlacement)99, typeof(StubChrome));

        var catalog = new ExplorerChromeSlotCatalog([stray]);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.TenantScope), Is.Empty);
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings), Is.Empty);
            Assert.That(catalog.ForPlacement((ExplorerChromeSlotPlacement)99), Is.Empty);
        });
    }

    [Test]
    public void A_null_registration_set_is_rejected()
    {
        Assert.That(() => new ExplorerChromeSlotCatalog(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void A_null_registration_is_rejected_rather_than_skipped()
    {
        Assert.That(() => new ExplorerChromeSlotCatalog([null!]), Throws.ArgumentException);
    }

    private sealed class StubScope : ComponentBase
    {
    }

    private sealed class StubSettings : ComponentBase
    {
    }

    private sealed class StubChrome : ComponentBase
    {
    }
}
