using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Tests.Detail;
using Orleans.Lattice.Explorer.Tests.Session;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// Builds a My tenant workspace wired to a real router and a real preference
/// contract over in-memory stores, so the surface-state tests drive the same
/// seams the shell supplies rather than a bespoke double.
/// </summary>
internal static class MyTenantSurfaceHarness
{
    /// <summary>The address the area answers at, with no sub-surface named.</summary>
    public const string AreaAddress = "/area/mytenant";

    /// <summary>Builds the preference contract with this plugin's key registered.</summary>
    public static ExplorerShellPreferences Preferences(FakeUiPreferenceStore? store = null)
    {
        var catalog = new ExplorerPreferenceCatalog();
        catalog.Register(MyTenantPluginKeys.SurfacePreference);
        return new ExplorerShellPreferences(
            store ?? new FakeUiPreferenceStore(),
            catalog,
            new FakeExplorerPreferenceScopeProvider());
    }

    /// <summary>
    /// Builds a workspace over a fresh domain, a router already pointed at
    /// <paramref name="address"/>, and a preference contract.
    /// </summary>
    /// <param name="address">The address the router starts on.</param>
    /// <param name="preferences">The preference contract, built fresh when omitted.</param>
    /// <param name="access">The gate decision to publish; allowed by default.</param>
    /// <returns>The workspace, the router, and the preferences.</returns>
    public static (MyTenantWorkspace Workspace, ExplorerShellRouter Router, ExplorerShellPreferences Preferences)
        Create(
            string address = AreaAddress,
            ExplorerShellPreferences? preferences = null,
            ExplorerPluginAccess? access = null)
    {
        var domain = new FakeTenancyDomain();
        var store = new ExplorerPluginAccessStore();
        store.Set(MyTenantPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);

        var router = new ExplorerShellRouter();
        router.SetAddress(address);

        preferences ??= Preferences();

        return (new MyTenantWorkspace(domain, store, preferences, router), router, preferences);
    }
}

/// <summary>
/// Where the My tenant area's open sub-surface lives: the address, the
/// preference contract, and the precedence between them.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceSurfaceTests
{
    [Test]
    public async Task An_address_naming_a_surface_opens_that_surface()
    {
        var (workspace, _, _) = MyTenantSurfaceHarness.Create(
            MyTenantSurfaceHarness.AreaAddress + "?my-tenant-surface=sharing");
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Sharing));
    }

    [Test]
    public async Task An_address_naming_a_surface_this_plugin_does_not_offer_is_ignored()
    {
        var (workspace, _, _) = MyTenantSurfaceHarness.Create(
            MyTenantSurfaceHarness.AreaAddress + "?my-tenant-surface=quotas");
        using var guard = workspace;

        await workspace.InitializeAsync();

        // "quotas" is the tenant administration area's surface, not this one's.
        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Overview));
    }

    [Test]
    public async Task Selecting_a_surface_puts_it_in_the_address()
    {
        var (workspace, router, _) = MyTenantSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();

        await workspace.SelectSurfaceAsync(MyTenantSurfaces.Members);

        Assert.That(
            ExplorerRoutePath.Format(router.Current),
            Is.EqualTo("/area/mytenant?my-tenant-surface=members"));
    }

    [Test]
    public async Task Selecting_a_surface_remembers_it_for_the_next_session()
    {
        var store = new FakeUiPreferenceStore();
        var (workspace, _, _) = MyTenantSurfaceHarness.Create(
            preferences: MyTenantSurfaceHarness.Preferences(store));
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(MyTenantSurfaces.Regions);

        var (restored, _, _) = MyTenantSurfaceHarness.Create(
            preferences: MyTenantSurfaceHarness.Preferences(store));
        using var restoredGuard = restored;
        await restored.InitializeAsync();

        Assert.That(restored.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Regions));
    }

    [Test]
    public async Task The_address_wins_over_what_was_remembered()
    {
        var store = new FakeUiPreferenceStore();
        var (workspace, _, _) = MyTenantSurfaceHarness.Create(
            preferences: MyTenantSurfaceHarness.Preferences(store));
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(MyTenantSurfaces.Regions);

        var (linked, _, _) = MyTenantSurfaceHarness.Create(
            MyTenantSurfaceHarness.AreaAddress + "?my-tenant-surface=sharing",
            MyTenantSurfaceHarness.Preferences(store));
        using var linkedGuard = linked;
        await linked.InitializeAsync();

        Assert.That(linked.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Sharing));
    }

    [Test]
    public async Task A_later_address_change_moves_the_surface_so_Back_works()
    {
        var (workspace, router, _) = MyTenantSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();

        router.SetAddress(MyTenantSurfaceHarness.AreaAddress + "?my-tenant-surface=quota");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Quota));
    }

    [Test]
    public async Task An_address_that_left_the_area_does_not_move_the_surface()
    {
        var (workspace, router, _) = MyTenantSurfaceHarness.Create();
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(MyTenantSurfaces.Members);

        router.SetAddress("/area/tenants?tenant-admin-surface=quotas");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Members));
    }

    [Test]
    public async Task A_disposed_workspace_stops_following_the_address()
    {
        var (workspace, router, _) = MyTenantSurfaceHarness.Create();
        await workspace.InitializeAsync();
        workspace.Dispose();

        router.SetAddress(MyTenantSurfaceHarness.AreaAddress + "?my-tenant-surface=sharing");

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Overview));
    }

    [Test]
    public async Task A_remembered_surface_that_no_longer_exists_is_abandoned_and_explained()
    {
        var store = new FakeUiPreferenceStore();
        var preferences = MyTenantSurfaceHarness.Preferences(store);
        await preferences.EnsureLoadedAsync();
        await preferences.SetAsync(MyTenantPluginKeys.SurfacePreference, "retired-surface");

        var (workspace, _, _) = MyTenantSurfaceHarness.Create(
            preferences: MyTenantSurfaceHarness.Preferences(store));
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Overview));
            Assert.That(workspace.SurfaceRestoreNotice, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task The_workspace_works_with_neither_a_router_nor_a_preference_contract()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();
        using var guard = harness.Workspace;

        await harness.OpenAsync(MyTenantSurfaces.Members);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Members));
            Assert.That(harness.Workspace.SurfaceRestoreNotice, Is.Null);
        });
    }

    [Test]
    public async Task A_refused_caller_restores_no_surface_at_all()
    {
        // InitializeAsync short-circuits on a closed gate, so a refused caller
        // does not spend a preference read on a surface they cannot open.
        var (workspace, _, _) = MyTenantSurfaceHarness.Create(
            MyTenantSurfaceHarness.AreaAddress + "?my-tenant-surface=sharing",
            access: ExplorerPluginAccess.Denied);
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Overview));
    }
}

/// <summary>
/// The gate contract as the My tenant area renders it: the four states, and the
/// remedy a denial carries.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceAccessCopyTests
{
    private static MyTenantWorkspace Workspace(ExplorerPluginAccess access) =>
        MyTenantWorkspaceHarness.Create(access: access).Workspace;

    [Test]
    public void An_anonymous_caller_is_told_to_sign_in_and_never_that_they_are_denied()
    {
        using var workspace = Workspace(ExplorerPluginAccess.AuthenticationRequired);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AuthenticationRequired, Is.True);
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(
                workspace.AccessMessage?.Kind,
                Is.EqualTo(ExplorerStateKind.SignInRequired));

            // The measured defect: this area told a signed-out visitor they held
            // no admin authority over an active tenant they never had.
            Assert.That(
                workspace.AccessMessage?.Kind,
                Is.Not.EqualTo(ExplorerStateKind.NotPermitted));
        });
    }

    [Test]
    public void A_denial_renders_the_remedy_the_gate_declared()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Deny(
            "you do not administer this tenant",
            ExplorerAccessRemedy.Requiring("Tenant admin", "your tenant's administrator")));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AccessRemedy.Permission, Is.EqualTo("Tenant admin"));
            Assert.That(
                workspace.AccessRemedyText,
                Is.EqualTo("Requires the Tenant admin permission - ask your tenant's administrator."));
        });
    }

    [Test]
    public void A_denial_whose_gate_declared_no_remedy_still_states_one()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Deny("no authority"));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AccessRemedy.IsSpecified, Is.False);
            Assert.That(workspace.AccessRemedyText, Is.Not.Null.And.Not.Empty);
            Assert.That(workspace.AccessRemedyText, Is.EqualTo(workspace.AccessMessage?.Remedy));
        });
    }

    [Test]
    public void An_allowed_caller_composes_no_refusal_at_all()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AccessMessage, Is.Null);
            Assert.That(workspace.AccessRemedyText, Is.Null);
        });
    }

    [Test]
    public void The_refusal_names_the_area_the_settled_way()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Denied);

        Assert.That(
            workspace.AccessMessage?.Headline,
            Does.Contain(ExplorerVocabulary.MyTenantArea));
    }
}
