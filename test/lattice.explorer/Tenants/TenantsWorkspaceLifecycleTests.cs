using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// Tenant lifecycle: creating with seeded admin subjects, suspending, resuming,
/// and deleting - and the confirmation gate that stands between an operator and
/// each destructive one.
/// <para>
/// The delete confirmation must state the cascade size before the call, from the
/// tenant's owned-tree figure, and must never report an unmeasured figure as
/// zero: "this deletes nothing else" and "we did not measure what this deletes"
/// are opposite things to tell somebody about to delete data.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceLifecycleTests
{
    private static async Task<(TenantsWorkspace Workspace, FakeTenancyDomain Domain)> ReadyAsync()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        await workspace.InitializeAsync();
        return (workspace, domain);
    }

    // ---- create -------------------------------------------------------------

    [Test]
    public async Task Creating_a_tenant_with_no_subjects_asks_the_cluster_to_seed_the_caller()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.OpenCreateForm();
        workspace.CreateTenantId = SampleTenants.Globex;

        await workspace.CreateTenantAsync();

        Assert.Multiple(() =>
        {
            // Null, not empty: a tenant seeded with nothing would be invisible to
            // whoever created it.
            Assert.That(domain.Service.LastSeededSubjects, Is.Null);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(workspace.LastMessage, Does.Contain("caller"));
        });
    }

    [Test]
    public async Task Creating_a_tenant_seeds_the_subjects_the_form_lists()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.OpenCreateForm();
        workspace.CreateTenantId = SampleTenants.Globex;
        workspace.CreateAdminSubjects = "user:ada\nuser:grace, user:alan";

        await workspace.CreateTenantAsync();

        Assert.That(
            domain.Service.LastSeededSubjects,
            Is.EqualTo(new[] { "user:ada", "user:grace", "user:alan" }));
    }

    [Test]
    public async Task Creating_a_tenant_with_a_blank_id_is_refused_before_the_call()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.OpenCreateForm();
        workspace.CreateTenantId = "   ";

        await workspace.CreateTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.CreateNeedsTenantIdMessage));
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.Create));
        });
    }

    [Test]
    public async Task Creating_an_existing_tenant_reports_that_it_already_exists()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.Create, TenantOperationStatus.AlreadyExists);
        workspace.OpenCreateForm();
        workspace.CreateTenantId = SampleTenants.Acme;

        await workspace.CreateTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.AlreadyExists));
            Assert.That(workspace.LastMessage, Does.Contain("already registered"));

            // The form stays open so the operator can correct the id.
            Assert.That(workspace.CreateFormOpen, Is.True);
        });
    }

    [Test]
    public async Task A_successful_create_closes_the_form_and_reloads_the_list()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.OpenCreateForm();
        workspace.CreateTenantId = SampleTenants.Globex;
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));
        domain.Service.Usage[SampleTenants.Globex] = SampleTenants.Usage(SampleTenants.Globex);

        await workspace.CreateTenantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.CreateFormOpen, Is.False);
            Assert.That(workspace.CreateTenantId, Is.Empty);
            Assert.That(workspace.TenantCount, Is.EqualTo(2));

            // The reload must not swallow the message that described the create.
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
        });
    }

    [Test]
    public async Task Opening_the_create_form_clears_whatever_was_typed_before()
    {
        var (workspace, _) = await ReadyAsync();
        using var _guard = workspace;
        workspace.OpenCreateForm();
        workspace.CreateTenantId = "half-typed";
        workspace.CloseCreateForm();

        workspace.OpenCreateForm();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.CreateTenantId, Is.Empty);
            Assert.That(workspace.CreateAdminSubjects, Is.Empty);
        });
    }

    [Test]
    public async Task A_denied_caller_cannot_create_a_tenant()
    {
        var (workspace, domain, store) = SampleTenants.Workspace();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);
        workspace.CreateTenantId = SampleTenants.Globex;

        await workspace.CreateTenantAsync();

        Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.Create));
    }

    // ---- suspend and resume -------------------------------------------------

    [Test]
    public async Task Suspending_a_tenant_asks_for_confirmation_rather_than_acting()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;

        workspace.RequestSuspend(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsAwaitingConfirmation, Is.True);
            Assert.That(workspace.Confirmation!.Kind, Is.EqualTo(TenantConfirmationKind.Suspend));
            Assert.That(workspace.Confirmation.TenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(workspace.Confirmation.Body, Does.Contain("remain intact"));
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.Suspend),
                "the request must not perform the suspend");
        });
    }

    [Test]
    public async Task Confirming_a_suspend_performs_it()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.RequestSuspend(SampleTenants.Acme);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Some.StartsWith(FakeTenantAdminService.Op.Suspend));
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(workspace.LastMessage, Does.Contain("suspended"));
        });
    }

    [Test]
    public async Task Cancelling_a_confirmation_performs_nothing()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.RequestSuspend(SampleTenants.Acme);

        workspace.CancelConfirmation();
        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.Suspend));
        });
    }

    [Test]
    public async Task Confirming_twice_performs_the_operation_once()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        workspace.RequestSuspend(SampleTenants.Acme);

        await workspace.ConfirmAsync();
        await workspace.ConfirmAsync();

        Assert.That(
            domain.Service.Calls.Count(call => call.StartsWith(FakeTenantAdminService.Op.Suspend, StringComparison.Ordinal)),
            Is.EqualTo(1));
    }

    [Test]
    public async Task Suspending_the_reserved_default_tenant_warns_before_the_call()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.DefaultTenant, isDefault: true));
        domain.Service.Usage[SampleTenants.DefaultTenant] = SampleTenants.Usage(SampleTenants.DefaultTenant);
        await workspace.InitializeAsync();

        workspace.RequestSuspend(SampleTenants.DefaultTenant);

        Assert.That(workspace.Confirmation!.Caution, Does.Contain("reserved default tenant"));
    }

    [Test]
    public async Task A_reserved_tenant_refusal_is_reported_with_its_own_meaning()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.Suspend, TenantOperationStatus.ReservedTenant);
        workspace.RequestSuspend(SampleTenants.Acme);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.ReservedTenant));
            Assert.That(workspace.LastMessage, Does.Contain("reserved default tenant"));
        });
    }

    [Test]
    public async Task Resuming_a_tenant_is_not_destructive_and_runs_directly()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;

        await workspace.ResumeTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(domain.Service.Calls, Has.Some.StartsWith(FakeTenantAdminService.Op.Resume));
            Assert.That(workspace.LastMessage, Does.Contain("resumed"));
        });
    }

    [Test]
    public async Task An_idempotent_transition_reports_that_nothing_changed()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        domain.Service.ReportsChanged = false;

        await workspace.ResumeTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(workspace.LastMessage, Does.Contain("already active"));
            Assert.That(workspace.LastMessage, Does.Contain("nothing changed"));
        });
    }

    [Test]
    public void Resuming_a_null_tenant_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(async () => await workspace.ResumeTenantAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Requesting_a_suspend_for_a_null_tenant_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(() => workspace.RequestSuspend(null!), Throws.ArgumentNullException);
    }

    // ---- delete -------------------------------------------------------------

    [Test]
    public async Task The_delete_confirmation_reports_the_cascade_size_before_the_call()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;

        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Confirmation!.Kind, Is.EqualTo(TenantConfirmationKind.Delete));
            Assert.That(workspace.Confirmation.Body, Does.Contain("3 trees"));
            Assert.That(workspace.Confirmation.Body, Does.Contain("irreversible"));
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.Delete),
                "the request must not perform the delete");
        });
    }

    [Test]
    public async Task A_tenant_that_owns_one_tree_reads_in_the_singular()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage(trees: 1);
        await workspace.InitializeAsync();

        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        Assert.That(workspace.Confirmation!.Body, Does.Contain("1 tree, which will be deleted"));
    }

    [Test]
    public async Task A_tenant_that_owns_no_trees_says_so_plainly()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage(trees: 0);
        await workspace.InitializeAsync();

        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        Assert.That(workspace.Confirmation!.Body, Does.Contain("owns no trees"));
    }

    [Test]
    public async Task An_unmeasured_tree_count_is_reported_as_unknown_and_never_as_zero()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage(trees: null);
        await workspace.InitializeAsync();

        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Confirmation!.Body, Does.Contain("was not measured"));
            Assert.That(workspace.Confirmation.Body, Does.Contain("it is not zero"));
            Assert.That(workspace.Confirmation.Body, Does.Not.Contain("owns no trees"));
        });
    }

    [Test]
    public async Task An_unreadable_tree_count_is_reported_as_unknown()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        await workspace.InitializeAsync();
        domain.Service.Fail(FakeTenantAdminService.Op.Usage, TenantOperationStatus.Denied);

        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        Assert.That(workspace.Confirmation!.Body, Does.Contain("could not be read"));
    }

    [Test]
    public async Task Confirming_a_delete_performs_it_and_reports_the_trees_removed()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        domain.Service.CascadedTreeCount = 3;
        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Some.StartsWith(FakeTenantAdminService.Op.Delete));
            Assert.That(workspace.LastMessage, Does.Contain("3 trees"));
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
        });
    }

    [Test]
    public async Task Deleting_the_selected_tenant_clears_the_selection()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        domain.Service.Tenants.Clear();
        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.SelectedTenantId, Is.Null);
            Assert.That(workspace.SelectedDetail, Is.Null);
        });
    }

    [Test]
    public async Task A_refused_delete_is_reported_with_its_own_meaning()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.Delete, TenantOperationStatus.ReservedTenant);
        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        await workspace.ConfirmAsync();

        Assert.That(workspace.LastMessage, Does.Contain("reserved default tenant"));
    }

    [Test]
    public async Task A_refused_lifecycle_change_does_not_re_list_the_cluster()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.Resume, TenantOperationStatus.Denied);
        var before = domain.Service.Calls
            .Count(call => call.StartsWith(FakeTenantAdminService.Op.List, StringComparison.Ordinal));

        await workspace.ResumeTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            // Nothing changed, so there is nothing to re-read.
            Assert.That(
                domain.Service.Calls.Count(call => call.StartsWith(FakeTenantAdminService.Op.List, StringComparison.Ordinal)),
                Is.EqualTo(before));
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
        });
    }

    [Test]
    public async Task A_successful_lifecycle_change_re_lists_and_keeps_its_message()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        var before = domain.Service.Calls
            .Count(call => call.StartsWith(FakeTenantAdminService.Op.List, StringComparison.Ordinal));

        await workspace.ResumeTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(
                domain.Service.Calls.Count(call => call.StartsWith(FakeTenantAdminService.Op.List, StringComparison.Ordinal)),
                Is.GreaterThan(before));
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(workspace.LastMessage, Does.Contain("resumed"));
        });
    }

    [Test]
    public void Requesting_a_delete_for_a_null_tenant_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(async () => await workspace.RequestDeleteAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task A_denied_caller_cannot_request_a_destructive_operation()
    {
        var (workspace, domain, store) = SampleTenants.Workspace();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);

        workspace.RequestSuspend(SampleTenants.Acme);
        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.Delete));
        });
    }

    [Test]
    public async Task A_gate_that_closes_between_the_request_and_the_confirm_stops_the_operation()
    {
        var (workspace, domain, store) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage();
        await workspace.InitializeAsync();
        workspace.RequestSuspend(SampleTenants.Acme);

        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);
        await workspace.ConfirmAsync();

        Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.Suspend));
    }

    [Test]
    public async Task Leaving_a_sub_surface_drops_a_pending_confirmation()
    {
        var (workspace, _) = await ReadyAsync();
        using var _guard = workspace;
        workspace.RequestSuspend(SampleTenants.Acme);

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        Assert.That(workspace.IsAwaitingConfirmation, Is.False);
    }

    [Test]
    public async Task Confirming_with_nothing_pending_does_nothing()
    {
        var (workspace, domain) = await ReadyAsync();
        using var _guard = workspace;
        var before = domain.Service.Calls.Count;

        await workspace.ConfirmAsync();

        Assert.That(domain.Service.Calls, Has.Count.EqualTo(before));
    }
}
