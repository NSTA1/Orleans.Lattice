using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// One contributed piece of shell chrome: which region it renders into, and the
/// guard that keeps a non-component out of the banner.
/// </summary>
[TestFixture]
public sealed class ExplorerChromeSlotTests
{
    [Test]
    public void A_slot_records_the_region_the_component_and_the_hint()
    {
        var slot = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.ViewSettings, typeof(StubChrome), 5);

        Assert.Multiple(() =>
        {
            Assert.That(slot.Placement, Is.EqualTo(ExplorerChromeSlotPlacement.ViewSettings));
            Assert.That(slot.ComponentType, Is.EqualTo(typeof(StubChrome)));
            Assert.That(slot.Order, Is.EqualTo(5));
        });
    }

    [Test]
    public void The_hint_defaults_to_zero_so_registration_order_decides()
    {
        var slot = new ExplorerChromeSlot(ExplorerChromeSlotPlacement.TenantScope, typeof(StubChrome));

        Assert.That(slot.Order, Is.Zero);
    }

    [Test]
    public void The_scope_control_leads_the_banner_because_it_changes_what_is_shown()
    {
        // The order of the two declared placements is the reading order of the
        // banner's trailing group, so it is part of the contract a sibling
        // feature codes against.
        Assert.That(
            (int)ExplorerChromeSlotPlacement.TenantScope,
            Is.LessThan((int)ExplorerChromeSlotPlacement.ViewSettings));
    }

    [Test]
    public void A_type_that_is_not_a_component_is_rejected_where_it_is_declared()
    {
        // Rendering it would fail deep inside the framework's dynamic component,
        // with nothing naming the registration that caused it.
        Assert.That(
            () => new ExplorerChromeSlot(ExplorerChromeSlotPlacement.TenantScope, typeof(string)),
            Throws.ArgumentException.With.Message.Contains("IComponent"));
    }

    [Test]
    public void A_null_component_type_is_rejected()
    {
        Assert.That(
            () => new ExplorerChromeSlot(ExplorerChromeSlotPlacement.TenantScope, null!),
            Throws.ArgumentNullException);
    }

    /// <summary>A stand-in chrome contribution, so a slot has something to name.</summary>
    private sealed class StubChrome : ComponentBase
    {
    }
}
