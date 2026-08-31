using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// The registration seam a feature uses to put a control in the shell's banner
/// without either side referencing the other.
/// </summary>
/// <remarks>
/// This is the contract the epic's two in-flight sibling features code against:
/// the tenant scope control and the theme and density controls each register
/// themselves against a placement, so neither has to edit the layout and the
/// layout names neither of them.
/// </remarks>
[TestFixture]
public sealed class ExplorerChromeServiceCollectionExtensionsTests
{
    [Test]
    public void Contributing_registers_the_catalog_on_the_first_call()
    {
        var services = new ServiceCollection();

        services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.TenantScope);

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IExplorerChromeSlotCatalog>();

        Assert.That(
            catalog.ForPlacement(ExplorerChromeSlotPlacement.TenantScope).Single().ComponentType,
            Is.EqualTo(typeof(StubScope)));
    }

    [Test]
    public void Two_features_can_contribute_to_two_regions_independently()
    {
        var services = new ServiceCollection();

        services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.TenantScope);
        services.AddExplorerChromeSlot<StubSettings>(ExplorerChromeSlotPlacement.ViewSettings);

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IExplorerChromeSlotCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(
                catalog.ForPlacement(ExplorerChromeSlotPlacement.TenantScope).Single().ComponentType,
                Is.EqualTo(typeof(StubScope)));
            Assert.That(
                catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings).Single().ComponentType,
                Is.EqualTo(typeof(StubSettings)));
        });
    }

    [Test]
    public void A_deployment_that_contributes_nothing_registers_no_catalog()
    {
        // The banner resolves the catalog optionally for exactly this case, so a
        // head that wants none pays for none and still renders.
        var services = new ServiceCollection();

        using var provider = services.BuildServiceProvider();

        Assert.That(provider.GetService<IExplorerChromeSlotCatalog>(), Is.Null);
    }

    [Test]
    public void The_hint_reaches_the_catalog()
    {
        var services = new ServiceCollection();

        services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.ViewSettings, order: 7);

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IExplorerChromeSlotCatalog>();

        Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings).Single().Order, Is.EqualTo(7));
    }

    [Test]
    public void A_null_service_collection_is_rejected()
    {
        Assert.That(
            () => ((IServiceCollection)null!).AddExplorerChromeSlot<StubScope>(
                ExplorerChromeSlotPlacement.TenantScope),
            Throws.ArgumentNullException);
    }

    private sealed class StubScope : ComponentBase
    {
    }

    private sealed class StubSettings : ComponentBase
    {
    }
}
