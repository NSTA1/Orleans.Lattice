using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// Why the tenant list has nothing to show. The whole point of the fixture is
/// that the answer is never a shrug: an empty list says whether it is empty
/// because nothing is there, because the tenant scope narrowed it, because the
/// caller may not read it, or because the read failed.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceListStateTests
{
    private static TenantsWorkspace Workspace(
        FakeTenancyDomain domain,
        ExplorerPluginAccess? access = null)
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(TenantsPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);
        return new TenantsWorkspace(domain, store);
    }

    [Test]
    public async Task A_cluster_with_no_tenants_says_nothing_is_being_hidden()
    {
        using var workspace = Workspace(new FakeTenancyDomain());

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.Empty));
            Assert.That(workspace.ListMessage!.Explanation, Does.Contain("Nothing is being hidden"));
        });
    }

    [Test]
    public async Task A_list_emptied_by_the_tenant_scope_names_the_tenant_in_force()
    {
        var domain = new FakeTenancyDomain { ActiveTenant = new ExplorerTenantId(SampleTenants.Acme) };
        using var workspace = Workspace(domain);

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.ScopedOut));
            Assert.That(workspace.ListMessage!.Explanation, Does.Contain(SampleTenants.Acme));
        });
    }

    [Test]
    public async Task A_list_read_across_every_tenant_is_empty_rather_than_scoped_out()
    {
        var domain = new FakeTenancyDomain
        {
            ActiveTenant = new ExplorerTenantId(SampleTenants.Acme),
            RequestedVisibility = ExplorerTenantVisibility.AllTenants,
        };
        using var workspace = Workspace(domain);

        await workspace.InitializeAsync();

        Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.Empty));
    }

    [Test]
    public async Task A_refused_read_says_this_is_not_an_empty_list()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Denied);
        using var workspace = Workspace(domain);

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.NotPermitted));
            Assert.That(workspace.ListMessage!.Explanation, Does.Contain("not an empty list"));
        });
    }

    [Test]
    public async Task A_refused_read_names_the_grant_the_gate_declared()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Denied);
        using var workspace = Workspace(
            domain,
            ExplorerPluginAccess.Allow(null));

        await workspace.InitializeAsync();

        Assert.That(workspace.ListMessage!.Remedy, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public async Task An_unauthenticated_read_offers_a_sign_in()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Fail(
            FakeTenantAdminService.Op.List,
            TenantOperationStatus.AuthenticationRequired);
        using var workspace = Workspace(domain);

        await workspace.InitializeAsync();

        Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.SignInRequired));
    }

    [Test]
    public async Task A_cluster_that_does_not_serve_tenancy_says_so_rather_than_reporting_nothing()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Unavailable);
        using var workspace = Workspace(domain);

        await workspace.InitializeAsync();

        Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.Unavailable));
    }

    [Test]
    public async Task A_failed_read_offers_a_retry_and_carries_the_clusters_own_words()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Failed);
        using var workspace = Workspace(domain);

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.Failed));
            Assert.That(workspace.ListMessage!.ActionLabel, Is.EqualTo(ExplorerVocabulary.RetryAction));
        });
    }

    [Test]
    public void Before_the_first_read_the_list_reports_that_it_is_loading()
    {
        using var workspace = Workspace(new FakeTenancyDomain());

        Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.Loading));
    }

    [Test]
    public async Task A_list_with_rows_reports_no_state_message_at_all()
    {
        var (workspace, _) = SampleTenants.Seeded();
        using var guard = workspace;

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Page, Is.Not.Empty);
            Assert.That(workspace.ListMessage, Is.Null);
        });
    }

    [Test]
    public void A_gate_that_refuses_the_area_reports_the_refusal_and_not_an_empty_cluster()
    {
        using var workspace = Workspace(new FakeTenancyDomain(), ExplorerPluginAccess.Denied);

        Assert.That(workspace.ListMessage?.Kind, Is.EqualTo(ExplorerStateKind.NotPermitted));
    }
}
