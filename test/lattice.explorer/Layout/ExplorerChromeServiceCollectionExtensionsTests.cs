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
    public void Nothing_is_registered_by_a_type_whose_constructor_the_container_must_guess_between()
    {
        // The generalisation of the defect the merged AddExplorerSession fix
        // closed, applied to this seam so it cannot come back through it. When a
        // descriptor names an implementation TYPE the container picks the
        // constructor, and it takes the one with the most satisfiable
        // parameters - an IEnumerable<T> parameter is always satisfiable, so an
        // ambiguous type can be silently constructed empty. This catalog takes
        // exactly such a parameter, which is why it is registered by factory.
        var services = new ServiceCollection()
            .AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.TenantScope);

        var ambiguous = services
            .Where(static descriptor => descriptor.ImplementationType is not null)
            .Where(static descriptor => descriptor.ImplementationType!.GetConstructors().Length > 1)
            .Select(static descriptor =>
                $"{descriptor.ServiceType.Name} -> {descriptor.ImplementationType!.Name}")
            .ToArray();

        Assert.That(ambiguous, Is.Empty, string.Join('\n', ambiguous));
    }

    [Test]
    public void The_catalog_resolved_from_the_container_actually_holds_the_contributions()
    {
        // The end-to-end shape the type-level guard above only implies: a
        // catalog constructed empty by the container would pass every assertion
        // about its type and still render an empty banner.
        var services = new ServiceCollection();
        services.AddExplorerChromeSlot<StubScope>(ExplorerChromeSlotPlacement.TenantScope);
        services.AddExplorerChromeSlot<StubSettings>(ExplorerChromeSlotPlacement.ViewSettings);

        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IExplorerChromeSlotCatalog>();

        Assert.Multiple(() =>
        {
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.TenantScope), Is.Not.Empty);
            Assert.That(catalog.ForPlacement(ExplorerChromeSlotPlacement.ViewSettings), Is.Not.Empty);
        });
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
