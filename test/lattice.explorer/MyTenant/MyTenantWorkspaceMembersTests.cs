using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The Members surface: listing, adding, and removing the tenant's admin
/// subjects, and the last-admin-subject invariant that is stated at the control
/// before the cluster has to refuse.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceMembersTests
{
    private static async Task<MyTenantWorkspaceHarness> OpenAsync(Action<FakeTenancyDomain>? configure = null)
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(configure);
        await harness.OpenAsync(MyTenantSurfaces.Members);
        return harness;
    }

    [Test]
    public async Task The_admin_subjects_are_listed_for_the_active_tenant()
    {
        var harness = await OpenAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.AdminSubjects, Is.EqualTo(new[] { "user:ada", "user:grace" }));
            Assert.That(harness.Service.TenantIdsTouched, Has.All.EqualTo(MyTenantSample.TenantId));
        });
    }

    [Test]
    public async Task A_refused_list_leaves_no_stale_membership()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.Admins = TenantOperationResult<ExplorerTenantAdmins>.Failure(
                TenantOperationStatus.Denied,
                "refused"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.AdminSubjects, Is.Empty);
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused"));
        });
    }

    [Test]
    public async Task Adding_a_subject_sends_it_and_adopts_the_committed_set()
    {
        var harness = await OpenAsync();
        harness.Workspace.NewAdminSubjectId = " user:hopper ";

        await harness.Workspace.AddAdminSubjectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.AddSubjectCalls, Has.Count.EqualTo(1));
            Assert.That(harness.Service.AddSubjectCalls[0].TenantId, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(
                harness.Service.AddSubjectCalls[0].SubjectId,
                Is.EqualTo("user:hopper"),
                "the id is trimmed before it is sent");
            Assert.That(harness.Workspace.AdminSubjects, Is.EqualTo(new[] { "user:grace" }));
            Assert.That(harness.Workspace.NewAdminSubjectId, Is.Empty, "the form clears on success");
        });
    }

    [Test]
    public async Task An_empty_subject_id_is_refused_before_a_call_is_made()
    {
        var harness = await OpenAsync();
        harness.Workspace.NewAdminSubjectId = "   ";

        await harness.Workspace.AddAdminSubjectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.AddSubjectCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.EmptySubjectRefusal));
            Assert.That(harness.Workspace.LastNotice?.Status, Is.EqualTo(TenantOperationStatus.InvalidRequest));
        });
    }

    [Test]
    public async Task A_refused_add_keeps_the_typed_id_so_the_caller_can_correct_it()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.AdminChange = TenantOperationResult<ExplorerTenantAdminChange>.Failure(
                TenantOperationStatus.Denied,
                "refused"));
        harness.Workspace.NewAdminSubjectId = "user:hopper";

        await harness.Workspace.AddAdminSubjectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.NewAdminSubjectId, Is.EqualTo("user:hopper"));
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused"));
        });
    }

    [Test]
    public async Task Removing_a_subject_sends_it_and_adopts_the_committed_set()
    {
        var harness = await OpenAsync();

        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.RemoveSubjectCalls, Has.Count.EqualTo(1));
            Assert.That(harness.Service.RemoveSubjectCalls[0].SubjectId, Is.EqualTo("user:ada"));
            Assert.That(harness.Service.RemoveSubjectCalls[0].TenantId, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(harness.Workspace.AdminSubjects, Is.EqualTo(new[] { "user:grace" }));
        });
    }

    [Test]
    public async Task Removing_the_last_admin_subject_is_refused_before_a_call_is_made()
    {
        // The gRPC binding collapses this refusal onto a generic precondition
        // status with the reason only in the message, so knowing the invariant
        // here is the only way the surface can name it up front.
        var harness = await OpenAsync(domain =>
            domain.Service.Admins = TenantOperationResult<ExplorerTenantAdmins>.Success(
                new ExplorerTenantAdmins { TenantId = MyTenantSample.TenantId, Subjects = ["user:ada"] },
                "ok"));

        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.RemoveSubjectCalls, Is.Empty, "nothing left the process");
            Assert.That(
                harness.Workspace.LastNotice?.Status,
                Is.EqualTo(TenantOperationStatus.LastAdminSubject),
                "the refusal is named specifically, not folded into a generic failure");
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.LastAdminSubjectRefusal));
            Assert.That(
                harness.Workspace.LastNotice?.Guidance,
                Is.EqualTo(MyTenantNotice.LastAdminSubjectGuidance));
        });
    }

    [Test]
    public async Task The_last_admin_subject_is_reported_so_the_surface_can_explain_the_disabled_control()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.Admins = TenantOperationResult<ExplorerTenantAdmins>.Success(
                new ExplorerTenantAdmins { TenantId = MyTenantSample.TenantId, Subjects = ["user:ada"] },
                "ok"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.IsLastAdminSubject, Is.True);
            Assert.That(harness.Workspace.CanRemoveAdminSubject("user:ada"), Is.False);
        });
    }

    [Test]
    public async Task Removal_is_permitted_once_a_second_subject_exists()
    {
        var harness = await OpenAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.IsLastAdminSubject, Is.False);
            Assert.That(harness.Workspace.CanRemoveAdminSubject("user:ada"), Is.True);
            Assert.That(harness.Workspace.CanRemoveAdminSubject(null), Is.False);
            Assert.That(harness.Workspace.CanRemoveAdminSubject(string.Empty), Is.False);
        });
    }

    [Test]
    public async Task A_servers_precondition_refusal_is_rendered_with_its_own_message()
    {
        // The wire form of the same invariant: the status is generic, so the
        // message is the only place the reason survives and must be shown as-is.
        const string ServerMessage = "Tenant 'acme' must retain at least one admin subject.";

        var harness = await OpenAsync(domain =>
            domain.Service.AdminChange = TenantOperationResult<ExplorerTenantAdminChange>.Failure(
                TenantOperationStatus.PreconditionFailed,
                ServerMessage));

        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.RemoveSubjectCalls, Has.Count.EqualTo(1));
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo(ServerMessage));
            Assert.That(harness.Workspace.LastNotice?.Severity, Is.EqualTo("is-refused"));
        });
    }

    [Test]
    public async Task A_typed_last_admin_refusal_from_the_facade_carries_its_guidance()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.AdminChange = TenantOperationResult<ExplorerTenantAdminChange>.Failure(
                TenantOperationStatus.LastAdminSubject,
                "refused by the facade"));

        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused by the facade"));
            Assert.That(
                harness.Workspace.LastNotice?.Guidance,
                Is.EqualTo(MyTenantNotice.LastAdminSubjectGuidance));
        });
    }

    [Test]
    public async Task An_empty_subject_id_is_ignored_on_removal()
    {
        var harness = await OpenAsync();

        await harness.Workspace.RemoveAdminSubjectAsync(string.Empty);

        Assert.That(harness.Service.RemoveSubjectCalls, Is.Empty);
    }

    [Test]
    public async Task A_denied_gate_makes_every_membership_action_a_no_op()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            access: ExplorerPluginAccess.Denied);
        harness.Workspace.NewAdminSubjectId = "user:hopper";

        await harness.Workspace.AddAdminSubjectAsync();
        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.AddSubjectCalls, Is.Empty);
            Assert.That(harness.Service.RemoveSubjectCalls, Is.Empty);
        });
    }
}
