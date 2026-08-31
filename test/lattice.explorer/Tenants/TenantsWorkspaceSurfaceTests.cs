using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// Builds a tenant administration workspace wired to a real router and a real
/// preference contract over in-memory stores, so the surface-state tests drive
/// the same seams the shell supplies rather than a bespoke double.
/// </summary>
/// <remarks>
/// Nothing here is timing, ordering, or clock dependent: the preference store is
/// a dictionary and the router is a pure state machine over an address string.
/// </remarks>
internal static class TenantsSurfaceHarness
{
    /// <summary>The address the area answers at, with no sub-surface named.</summary>
    public const string AreaAddress = "/area/tenants";

    /// <summary>Builds the preference contract with this plugin's key registered.</summary>
    public static ExplorerShellPreferences Preferences(FakeUiPreferenceStore? store = null)
    {
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(TenantsPluginKeys.SurfacePreference);
        return new ExplorerShellPreferences(
            store ?? new FakeUiPreferenceStore(),
            catalog,
            new FakeExplorerPreferenceScopeProvider());
    }

    /// <summary>
    /// Builds a workspace over a seeded domain, a router already pointed at
    /// <paramref name="address"/>, and a preference contract.
    /// </summary>
    /// <param name="address">The address the router starts on.</param>
    /// <param name="preferences">The preference contract, built fresh when omitted.</param>
    /// <param name="access">The gate decision to publish; allowed by default.</param>
    /// <returns>The workspace, its domain, the router, and the preferences.</returns>
    public static (
        TenantsWorkspace Workspace,
        FakeTenancyDomain Domain,
        ExplorerShellRouter Router,
        ExplorerShellPreferences Preferences) Create(
        string address = AreaAddress,
        ExplorerShellPreferences? preferences = null,
        ExplorerPluginAccess? access = null)
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Details[SampleTenants.Acme] = SampleTenants.Detail();
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage();

        var store = new ExplorerPluginAccessStore();
        store.Set(TenantsPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);

        var router = new ExplorerShellRouter();
        router.SetAddress(address);

        preferences ??= Preferences();

        return (
            new TenantsWorkspace(domain, store, preferences, router),
            domain,
            router,
            preferences);
    }
}

/// <summary>
/// Where the open sub-surface lives: the address, the preference contract, and
/// the precedence between them.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceSurfaceTests
{
    [Test]
    public async Task An_address_naming_a_surface_opens_that_surface()
    {
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            TenantsSurfaceHarness.AreaAddress + "?tenant-admin-surface=quotas");
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Quotas));
    }

    [Test]
    public async Task An_address_naming_a_surface_this_plugin_does_not_offer_is_ignored()
    {
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            TenantsSurfaceHarness.AreaAddress + "?tenant-admin-surface=nope");
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Overview));
    }

    [Test]
    public async Task The_retired_surface_id_no_longer_addresses_anything()
    {
        // "tenants" was the first sub-surface's id before the naming was settled.
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            TenantsSurfaceHarness.AreaAddress + "?tenant-admin-surface=tenants");
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Overview));
    }

    [Test]
    public async Task Selecting_a_surface_puts_it_in_the_address()
    {
        var (workspace, _, router, _) = TenantsSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Regions);

        Assert.That(
            ExplorerRoutePath.Format(router.Current),
            Is.EqualTo("/area/tenants?tenant-admin-surface=regions"));
    }

    [Test]
    public async Task Selecting_a_surface_remembers_it_for_the_next_session()
    {
        var store = new FakeUiPreferenceStore();
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            preferences: TenantsSurfaceHarness.Preferences(store));
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);

        // A second session over the same durable store, with no surface in the
        // address at all.
        var (restored, _, _, _) = TenantsSurfaceHarness.Create(
            preferences: TenantsSurfaceHarness.Preferences(store));
        using var restoredGuard = restored;
        await restored.InitializeAsync();

        Assert.That(restored.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Access));
    }

    [Test]
    public async Task The_address_wins_over_what_was_remembered()
    {
        var store = new FakeUiPreferenceStore();
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            preferences: TenantsSurfaceHarness.Preferences(store));
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);

        var (linked, _, _, _) = TenantsSurfaceHarness.Create(
            TenantsSurfaceHarness.AreaAddress + "?tenant-admin-surface=quotas",
            TenantsSurfaceHarness.Preferences(store));
        using var linkedGuard = linked;
        await linked.InitializeAsync();

        // A link somebody sent must show what they saw, not what this caller
        // left open.
        Assert.That(linked.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Quotas));
    }

    [Test]
    public async Task A_remembered_surface_is_also_written_into_the_address_so_the_view_is_shareable()
    {
        var store = new FakeUiPreferenceStore();
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            preferences: TenantsSurfaceHarness.Preferences(store));
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Regions);

        var (restored, _, router, _) = TenantsSurfaceHarness.Create(
            preferences: TenantsSurfaceHarness.Preferences(store));
        using var restoredGuard = restored;
        await restored.InitializeAsync();

        Assert.That(
            router.Current.Parameters.GetValueOrEmpty(TenantsPluginKeys.SurfaceQueryKey),
            Is.EqualTo(TenantsSurfaces.Regions));
    }

    [Test]
    public async Task A_later_address_change_moves_the_surface_so_Back_works()
    {
        var (workspace, _, router, _) = TenantsSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();

        router.SetAddress(TenantsSurfaceHarness.AreaAddress + "?tenant-admin-surface=access");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Access));
    }

    [Test]
    public async Task An_address_that_left_the_area_does_not_move_the_surface()
    {
        var (workspace, _, router, _) = TenantsSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        router.SetAddress("/area/mytenant?my-tenant-surface=sharing");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Quotas));
    }

    [Test]
    public async Task A_disposed_workspace_stops_following_the_address()
    {
        var (workspace, _, router, _) = TenantsSurfaceHarness.Create();
        await workspace.InitializeAsync();
        workspace.Dispose();

        router.SetAddress(TenantsSurfaceHarness.AreaAddress + "?tenant-admin-surface=access");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Overview));
    }

    [Test]
    public async Task The_workspace_works_with_neither_a_router_nor_a_preference_contract()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var guard = workspace;

        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Quotas));
            Assert.That(workspace.SurfaceRestoreNotice, Is.Null);
        });
    }

    [Test]
    public async Task A_remembered_surface_that_no_longer_exists_is_abandoned_and_explained()
    {
        var store = new FakeUiPreferenceStore();
        var preferences = TenantsSurfaceHarness.Preferences(store);
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(TenantsPluginKeys.SurfacePreference, "tenants");

        var (workspace, _, _, _) = TenantsSurfaceHarness.Create(
            preferences: TenantsSurfaceHarness.Preferences(store));
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Overview));
            Assert.That(workspace.SurfaceRestoreNotice, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task Selecting_the_surface_already_open_changes_nothing()
    {
        var (workspace, _, router, _) = TenantsSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Overview);

        Assert.That(ExplorerRoutePath.Format(router.Current), Is.EqualTo("/area/tenants"));
    }

    [Test]
    public async Task Selecting_a_surface_this_plugin_does_not_offer_is_refused()
    {
        var (workspace, _, _, _) = TenantsSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();

        await workspace.SelectSurfaceAsync("nope");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Overview));
    }
}
