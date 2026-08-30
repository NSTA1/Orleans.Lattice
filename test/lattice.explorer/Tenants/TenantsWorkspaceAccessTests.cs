using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The tenant-access surface: admin subjects, and cross-tenant grants in both
/// directions and every lifecycle state.
/// <para>
/// Two rules carry the weight here. A tenant must always keep one admin subject,
/// and the surface knows how many there are, so it refuses locally with the rule
/// rather than round-tripping for a refusal the wire strips of its reason. And a
/// grant transition is checked against the state the surface already holds, so
/// an operator is told "that grant is pending, not active" instead of receiving
/// a bare precondition failure.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceAccessTests
{
    private static async Task<(TenantsWorkspace Workspace, FakeTenancyDomain Domain)> OnAccessAsync()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);
        return (workspace, domain);
    }

    // ---- admin subjects -----------------------------------------------------

    [Test]
    public async Task The_surface_lists_the_tenants_admin_subjects()
    {
        var (workspace, _) = await OnAccessAsync();
        using var _guard = workspace;

        Assert.That(workspace.AdminSubjects, Is.EqualTo(new[] { SampleTenants.Subject, "user:grace" }));
    }

    [Test]
    public async Task Adding_an_admin_subject_is_additive_and_runs_directly()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.AdminChangeResult.AddRange([SampleTenants.Subject, "user:grace", "user:alan"]);
        workspace.AddAdminSubjectId = "user:alan";

        await workspace.AddAdminSubjectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(workspace.AdminSubjects, Has.Count.EqualTo(3));
            Assert.That(workspace.AddAdminSubjectId, Is.Empty);
            Assert.That(workspace.LastMessage, Does.Contain("Granted user:alan"));
        });
    }

    [Test]
    public async Task Adding_an_existing_admin_subject_reports_that_nothing_changed()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.ReportsChanged = false;
        domain.Service.AdminChangeResult.AddRange([SampleTenants.Subject, "user:grace"]);
        workspace.AddAdminSubjectId = SampleTenants.Subject;

        await workspace.AddAdminSubjectAsync();

        Assert.That(workspace.LastMessage, Does.Contain("already held admin authority"));
    }

    [Test]
    public async Task Adding_a_blank_subject_is_refused_before_the_call()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        workspace.AddAdminSubjectId = "   ";

        await workspace.AddAdminSubjectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.SubjectRequiredMessage));
            Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.AddAdmin));
        });
    }

    [Test]
    public async Task Removing_an_admin_subject_asks_for_confirmation_rather_than_acting()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;

        workspace.RequestRemoveAdminSubject(SampleTenants.Subject);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Confirmation!.Kind, Is.EqualTo(TenantConfirmationKind.RemoveAdminSubject));
            Assert.That(workspace.Confirmation.Target, Is.EqualTo(SampleTenants.Subject));
            Assert.That(workspace.Confirmation.Body, Does.Contain(SampleTenants.Subject));
            Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.RemoveAdmin));
        });
    }

    [Test]
    public async Task Confirming_a_removal_performs_it()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.AdminChangeResult.Add("user:grace");
        workspace.RequestRemoveAdminSubject(SampleTenants.Subject);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Some.StartsWith(FakeTenantAdminService.Op.RemoveAdmin));
            Assert.That(workspace.AdminSubjects, Is.EqualTo(new[] { "user:grace" }));
            Assert.That(workspace.LastMessage, Does.Contain("Revoked"));
        });
    }

    [Test]
    public async Task Removing_the_last_admin_subject_is_refused_locally_with_the_rule()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        domain.Service.AdminSubjects[SampleTenants.Acme] = [SampleTenants.Subject];

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);

        Assert.That(workspace.CanRemoveAdminSubject, Is.False);

        workspace.RequestRemoveAdminSubject(SampleTenants.Subject);

        Assert.Multiple(() =>
        {
            // The refusal carries the same classification the facade would use,
            // and never becomes a bare precondition failure over the wire.
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.LastAdminSubject));
            Assert.That(workspace.LastMessage, Does.Contain(TenantRefusal.LastAdminSubjectRule));
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.RemoveAdmin));
        });
    }

    [Test]
    public async Task A_server_side_last_admin_refusal_is_still_reported_with_the_rule()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.RemoveAdmin, TenantOperationStatus.LastAdminSubject);
        workspace.RequestRemoveAdminSubject(SampleTenants.Subject);

        await workspace.ConfirmAsync();

        Assert.That(workspace.LastMessage, Is.EqualTo(TenantRefusal.LastAdminSubjectRule));
    }

    [Test]
    public async Task A_wire_collapsed_admin_refusal_renders_the_reason_and_the_rule()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.Fail(
            FakeTenantAdminService.Op.RemoveAdmin,
            TenantOperationStatus.PreconditionFailed,
            "acme would have no admin subjects");

        workspace.RequestRemoveAdminSubject(SampleTenants.Subject);
        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Does.Contain("acme would have no admin subjects"));
            Assert.That(workspace.LastMessage, Does.Contain(TenantRefusal.LastAdminSubjectRule));
        });
    }

    [Test]
    public void Requesting_a_removal_for_a_null_subject_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(() => workspace.RequestRemoveAdminSubject(null!), Throws.ArgumentNullException);
    }

    // ---- grants -------------------------------------------------------------

    [Test]
    public async Task The_surface_lists_grants_in_both_directions()
    {
        var (workspace, _) = await OnAccessAsync();
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IssuedGrants, Has.Count.EqualTo(1));
            Assert.That(workspace.ReceivedGrants, Has.Count.EqualTo(1));
            Assert.That(workspace.IssuedGrants[0].Direction, Is.EqualTo(TenantGrantDirection.Issued));
            Assert.That(workspace.ReceivedGrants[0].Direction, Is.EqualTo(TenantGrantDirection.Received));
        });
    }

    [Test]
    public async Task Every_grant_carries_its_state_explicitly()
    {
        var (workspace, _) = await OnAccessAsync();
        using var _guard = workspace;

        var pending = workspace.IssuedGrants[0];
        var active = workspace.ReceivedGrants[0];

        Assert.Multiple(() =>
        {
            Assert.That(pending.State, Is.EqualTo(ExplorerTenantGrantState.Pending));
            Assert.That(pending.Authorizes, Is.False);
            Assert.That(pending.AuthorityText, Does.StartWith("Authorizes nothing yet"));

            Assert.That(active.State, Is.EqualTo(ExplorerTenantGrantState.Active));
            Assert.That(active.Authorizes, Is.True);
            Assert.That(active.AuthorityText, Does.Contain("now"));
        });
    }

    [Test]
    public async Task The_pending_inbox_is_counted_for_the_operator()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        domain.Service.Grants[SampleTenants.Acme] = new ExplorerTenantGrants
        {
            TenantId = SampleTenants.Acme,
            Issued = [],
            Received =
            [
                SampleTenants.Grant(ExplorerTenantGrantState.Pending, grantee: SampleTenants.Acme, grantId: "a"),
                SampleTenants.Grant(ExplorerTenantGrantState.Active, grantee: SampleTenants.Acme, grantId: "b"),
                SampleTenants.Grant(ExplorerTenantGrantState.Pending, grantee: SampleTenants.Acme, grantId: "c"),
            ],
        };

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);

        Assert.That(workspace.PendingReceivedCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Offering_a_grant_from_the_selected_tenant_names_it_as_the_granter()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        workspace.OfferFromSelectedTenant = true;
        workspace.OfferCounterpartyTenantId = SampleTenants.Globex;
        workspace.OfferScope = SampleTenants.Scope;
        workspace.OfferOperations = ExplorerTenantGrantAccess.ReadWrite;
        domain.Service.TransitionResult = SampleTenants.Grant(ExplorerTenantGrantState.Pending);

        await workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                domain.Service.LastGrantArguments,
                Is.EqualTo(new[] { SampleTenants.Acme, SampleTenants.Globex, SampleTenants.Scope }));
            Assert.That(domain.Service.LastOfferedOperations, Is.EqualTo(ExplorerTenantGrantAccess.ReadWrite));
        });
    }

    [Test]
    public async Task Offering_a_grant_to_the_selected_tenant_names_it_as_the_grantee()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;

        // An operator acts on either tenant's behalf, so both directions are
        // offerable from here.
        workspace.OfferFromSelectedTenant = false;
        workspace.OfferCounterpartyTenantId = SampleTenants.Globex;
        workspace.OfferScope = SampleTenants.Scope;
        domain.Service.TransitionResult = SampleTenants.Grant(ExplorerTenantGrantState.Pending);

        await workspace.OfferGrantAsync();

        Assert.That(
            domain.Service.LastGrantArguments,
            Is.EqualTo(new[] { SampleTenants.Globex, SampleTenants.Acme, SampleTenants.Scope }));
    }

    [Test]
    public async Task An_offer_reports_that_the_grant_authorizes_nothing_yet()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        workspace.OfferCounterpartyTenantId = SampleTenants.Globex;
        workspace.OfferScope = SampleTenants.Scope;
        domain.Service.TransitionResult = SampleTenants.Grant(ExplorerTenantGrantState.Pending);

        await workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(workspace.LastMessage, Does.Contain("pending approval"));
            Assert.That(workspace.LastMessage, Does.Contain("Authorizes nothing yet"));
        });
    }

    [Test]
    public async Task An_incomplete_offer_is_refused_before_the_call()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        workspace.OfferCounterpartyTenantId = string.Empty;
        workspace.OfferScope = SampleTenants.Scope;

        await workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.OfferIncompleteMessage));
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Offer));
        });
    }

    [Test]
    public async Task An_offer_that_authorizes_nothing_is_refused_before_the_call()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        workspace.OfferCounterpartyTenantId = SampleTenants.Globex;
        workspace.OfferScope = SampleTenants.Scope;
        workspace.OfferOperations = ExplorerTenantGrantAccess.None;

        await workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.OfferNeedsOperationsMessage));
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Offer));
        });
    }

    [Test]
    public async Task Approving_a_pending_grant_runs_directly()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        var pending = workspace.IssuedGrants[0];
        domain.Service.TransitionResult = SampleTenants.Grant(ExplorerTenantGrantState.Active);

        await workspace.ApproveGrantAsync(pending);

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Some.EqualTo(FakeTenantAdminService.Op.Approve));
            Assert.That(workspace.LastMessage, Does.Contain("active"));
            Assert.That(workspace.LastMessage, Does.Contain("Authorizes read now"));
        });
    }

    [Test]
    public async Task Approving_a_grant_that_is_not_pending_is_refused_locally()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        var active = workspace.ReceivedGrants[0];

        await workspace.ApproveGrantAsync(active);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.GrantTransitionRejected));
            Assert.That(workspace.LastMessage, Does.Contain("active"));
            Assert.That(workspace.LastMessage, Does.Contain("Only a pending grant"));
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Approve));
        });
    }

    [Test]
    public async Task Rejecting_a_pending_grant_asks_for_confirmation_rather_than_acting()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        var pending = workspace.IssuedGrants[0];

        workspace.RequestRejectGrant(pending);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Confirmation!.Kind, Is.EqualTo(TenantConfirmationKind.RejectGrant));
            Assert.That(workspace.Confirmation.Body, Does.Contain("terminal"));
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Reject));
        });
    }

    [Test]
    public async Task Confirming_a_rejection_performs_it()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.TransitionResult = SampleTenants.Grant(ExplorerTenantGrantState.Rejected);
        workspace.RequestRejectGrant(workspace.IssuedGrants[0]);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Some.EqualTo(FakeTenantAdminService.Op.Reject));
            Assert.That(
                domain.Service.LastGrantArguments,
                Is.EqualTo(new[] { SampleTenants.Acme, SampleTenants.Globex, SampleTenants.Scope }));
            Assert.That(workspace.LastMessage, Does.Contain("rejected"));
        });
    }

    [Test]
    public async Task Rejecting_a_grant_that_is_not_pending_is_refused_locally()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;

        workspace.RequestRejectGrant(workspace.ReceivedGrants[0]);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.GrantTransitionRejected));
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Reject));
        });
    }

    [Test]
    public async Task Revoking_an_active_grant_asks_for_confirmation_rather_than_acting()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;

        workspace.RequestRevokeGrant(workspace.ReceivedGrants[0]);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Confirmation!.Kind, Is.EqualTo(TenantConfirmationKind.RevokeGrant));
            Assert.That(workspace.Confirmation.Body, Does.Contain("stops"));
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Revoke));
        });
    }

    [Test]
    public async Task Confirming_a_revocation_performs_it()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.TransitionResult =
            SampleTenants.Grant(ExplorerTenantGrantState.Revoked, granter: SampleTenants.Globex, grantee: SampleTenants.Acme);
        workspace.RequestRevokeGrant(workspace.ReceivedGrants[0]);

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Some.EqualTo(FakeTenantAdminService.Op.Revoke));
            Assert.That(
                domain.Service.LastGrantArguments,
                Is.EqualTo(new[] { SampleTenants.Globex, SampleTenants.Acme, SampleTenants.Scope }));
            Assert.That(workspace.LastMessage, Does.Contain("withdrawn"));
            Assert.That(workspace.LastMessage, Does.Contain("Authorizes nothing"));
        });
    }

    [Test]
    public async Task Revoking_a_grant_that_is_not_active_is_refused_locally()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;

        // The issued grant is pending: there is no live access to withdraw.
        workspace.RequestRevokeGrant(workspace.IssuedGrants[0]);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.GrantTransitionRejected));
            Assert.That(workspace.LastMessage, Does.Contain("no live access to withdraw"));
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(domain.Service.Calls, Has.None.EqualTo(FakeTenantAdminService.Op.Revoke));
        });
    }

    [Test]
    public async Task A_server_side_grant_transition_refusal_names_the_state_machine()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.Fail(
            FakeTenantAdminService.Op.Approve,
            TenantOperationStatus.GrantTransitionRejected,
            "not pending");

        await workspace.ApproveGrantAsync(workspace.IssuedGrants[0]);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.GrantTransitionRejected));
            Assert.That(workspace.LastMessage, Does.Contain("Only a pending grant"));
        });
    }

    [Test]
    public async Task A_wire_collapsed_grant_refusal_renders_the_reason_and_the_state_machine()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.Fail(
            FakeTenantAdminService.Op.Approve,
            TenantOperationStatus.PreconditionFailed,
            "grant is already active");

        await workspace.ApproveGrantAsync(workspace.IssuedGrants[0]);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Does.Contain("grant is already active"));
            Assert.That(workspace.LastMessage, Does.Contain("Only a pending grant"));
        });
    }

    [Test]
    public async Task A_grant_not_found_refusal_is_reported_with_its_own_meaning()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.Approve, TenantOperationStatus.GrantNotFound);

        await workspace.ApproveGrantAsync(workspace.IssuedGrants[0]);

        Assert.That(workspace.LastMessage, Does.Contain("nothing to approve"));
    }

    [Test]
    public async Task An_idempotent_transition_reports_that_the_grant_had_not_moved()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        domain.Service.ReportsChanged = false;
        domain.Service.TransitionResult = SampleTenants.Grant(ExplorerTenantGrantState.Active);

        await workspace.ApproveGrantAsync(workspace.IssuedGrants[0]);

        Assert.That(workspace.LastMessage, Does.StartWith("The grant was already in that state"));
    }

    [Test]
    public async Task A_refused_grant_listing_is_reported()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        domain.Service.Fail(FakeTenantAdminService.Op.ListGrants, TenantOperationStatus.Denied);

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IssuedGrants, Is.Empty);
            Assert.That(workspace.ReceivedGrants, Is.Empty);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
        });
    }

    [Test]
    public async Task A_refused_admin_listing_is_reported_and_leaves_no_subjects()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        domain.Service.Fail(FakeTenantAdminService.Op.ListAdmins, TenantOperationStatus.ReservedTenant);

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AdminSubjects, Is.Empty);
            Assert.That(workspace.CanRemoveAdminSubject, Is.False);
        });
    }

    [Test]
    public async Task Refreshing_access_re_reads_both_lists()
    {
        var (workspace, domain) = await OnAccessAsync();
        using var _guard = workspace;
        var before = domain.Service.Calls.Count;

        await workspace.RefreshAccessAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Has.Count.GreaterThan(before));
            Assert.That(domain.Service.Calls, Has.Some.StartsWith(FakeTenantAdminService.Op.ListAdmins));
            Assert.That(domain.Service.Calls, Has.Some.StartsWith(FakeTenantAdminService.Op.ListGrants));
        });
    }

    [Test]
    public async Task Adding_a_subject_with_no_tenant_selected_is_refused_before_the_call()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        workspace.AddAdminSubjectId = SampleTenants.Subject;

        await workspace.AddAdminSubjectAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.AccessNeedsTenantMessage));
            Assert.That(domain.Service.Calls, Has.None.StartsWith(FakeTenantAdminService.Op.AddAdmin));
        });
    }
}
