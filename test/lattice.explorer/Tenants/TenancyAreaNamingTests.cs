using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The naming this issue exists to settle, asserted across <em>both</em> tenancy
/// areas at once, because the defect was a relationship between them rather than
/// a fault in either.
/// </summary>
/// <remarks>
/// <para>
/// Two near-identically named areas for what a user perceives as one concept,
/// rendered in the same strip, and the operator area's own first sub-surface was
/// also called "Tenants" - so the word appeared twice in adjacent navigation
/// tiers simultaneously. This fixture is the regression net for all three parts.
/// </para>
/// <para>
/// The shell carries a de-duplicating backstop for a sub-surface that collides
/// with its area. These assertions are the reason it never has to fire.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TenancyAreaNamingTests
{
    private static string TenantAdministrationLabel { get; } = ExplorerVocabulary.TenantAdministrationArea;

    private static string MyTenantLabel { get; } = ExplorerVocabulary.MyTenantArea;

    [Test]
    public void The_two_areas_do_not_share_a_name()
    {
        Assert.That(
            TenantAdministrationLabel,
            Is.Not.EqualTo(MyTenantLabel).IgnoreCase);
    }

    [Test]
    public void Neither_area_is_called_the_retired_word()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantAdministrationLabel, Is.Not.EqualTo("Tenants").IgnoreCase);
            Assert.That(MyTenantLabel, Is.Not.EqualTo("Tenants").IgnoreCase);
        });
    }

    [Test]
    public void Each_area_says_whose_tenants_it_administers()
    {
        Assert.Multiple(() =>
        {
            // One administers other people's tenants as a platform operator...
            Assert.That(TenantAdministrationLabel, Does.Contain("administration"));

            // ...the other manages your own.
            Assert.That(MyTenantLabel, Does.StartWith("My"));
        });
    }

    [Test]
    public void No_tenant_administration_sub_surface_repeats_its_own_area_name()
    {
        AssertNoCollision(TenantsSurfaces.Tabs, TenantAdministrationLabel);
    }

    [Test]
    public void No_my_tenant_sub_surface_repeats_its_own_area_name()
    {
        AssertNoCollision(MyTenantSurfaces.Tabs, MyTenantLabel);
    }

    [Test]
    public void No_sub_surface_of_either_area_repeats_the_other_areas_name()
    {
        // Both areas render in the same rail, so a sub-surface that borrowed the
        // sibling's word would be just as ambiguous as one that borrowed its
        // own area's.
        Assert.Multiple(() =>
        {
            AssertNoCollision(TenantsSurfaces.Tabs, MyTenantLabel);
            AssertNoCollision(MyTenantSurfaces.Tabs, TenantAdministrationLabel);
        });
    }

    [Test]
    public void No_sub_surface_of_either_area_is_called_the_retired_word()
    {
        Assert.Multiple(() =>
        {
            AssertNoCollision(TenantsSurfaces.Tabs, "Tenants");
            AssertNoCollision(MyTenantSurfaces.Tabs, "Tenants");
        });
    }

    [Test]
    public void The_two_areas_are_addressed_at_distinct_slugs()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantsPluginKeys.AreaSlug, Is.EqualTo("tenants"));
            Assert.That(MyTenantPluginKeys.AreaSlug, Is.EqualTo("mytenant"));
            Assert.That(TenantsPluginKeys.AreaSlug, Is.Not.EqualTo(MyTenantPluginKeys.AreaSlug));
        });
    }

    [Test]
    public void The_two_areas_carry_their_sub_surface_state_under_distinct_keys()
    {
        // A route keeps its parameters when the area changes, so a shared key
        // would let one area overwrite the other's remembered surface.
        Assert.Multiple(() =>
        {
            Assert.That(
                TenantsPluginKeys.SurfaceQueryKey,
                Is.Not.EqualTo(MyTenantPluginKeys.SurfaceQueryKey));
            Assert.That(
                TenantsPluginKeys.SurfacePreference.Name,
                Is.Not.EqualTo(MyTenantPluginKeys.SurfacePreference.Name));
        });
    }

    [Test]
    public void The_two_areas_hold_distinct_positions_in_the_rail()
    {
        var tenantAdministration = new TenantsAreaPlugin(
            new TenantsAccessGate(new FakeTenancyDomain())).Descriptor;
        var myTenant = new MyTenantAreaPlugin(new AllowingGate()).Descriptor;

        Assert.Multiple(() =>
        {
            Assert.That(tenantAdministration.Order, Is.Not.EqualTo(myTenant.Order));
            Assert.That(tenantAdministration.Label, Is.EqualTo(TenantAdministrationLabel));
            Assert.That(myTenant.Label, Is.EqualTo(MyTenantLabel));
        });
    }

    private static void AssertNoCollision(IReadOnlyList<LatticeTabItem> tabs, string areaLabel)
    {
        foreach (var tab in tabs)
        {
            Assert.That(
                tab.Label,
                Is.Not.EqualTo(areaLabel).IgnoreCase,
                $"sub-surface '{tab.Id}' repeats the adjacent tier's label '{areaLabel}'");
        }
    }

    /// <summary>
    /// A My tenant gate that admits every caller, so the descriptor can be read
    /// without arranging a domain.
    /// </summary>
    private sealed class AllowingGate : IMyTenantAccessGate
    {
        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default) =>
            new(ExplorerPluginAccess.Allowed);
    }
}
