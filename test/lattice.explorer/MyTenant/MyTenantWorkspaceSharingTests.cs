using Orleans.Lattice.Explorer.MyTenant;
using Orleans.Lattice.Explorer.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The Sharing surface: the two-step grant agreement seen from this tenant's
/// side. Outbound offers and withdrawals, and the inbound inbox whose approval
/// is the step that makes a grant live.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceSharingTests
{
    private static async Task<MyTenantWorkspaceHarness> OpenAsync(Action<FakeTenancyDomain>? configure = null)
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(configure);
        await harness.OpenAsync(MyTenantSurfaces.Sharing);
        return harness;
    }

    private static MyTenantWorkspaceHarness WithGrants(ExplorerTenantGrants grants)
    {
        return MyTenantWorkspaceHarness.Create(domain =>
            domain.Service.Grants = TenantOperationResult<ExplorerTenantGrants>.Success(grants, "ok"));
    }

    [Test]
    public async Task Both_directions_are_projected_from_the_clusters_report()
    {
        var harness = await OpenAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Outbound, Has.Count.EqualTo(1));
            Assert.That(harness.Workspace.Inbound, Has.Count.EqualTo(1));
            Assert.That(
                harness.Workspace.Outbound[0].Direction,
                Is.EqualTo(TenantGrantDirection.Outbound));
            Assert.That(harness.Workspace.Inbound[0].Direction, Is.EqualTo(TenantGrantDirection.Inbound));
        });
    }

    [Test]
    public async Task The_inbox_counts_the_offers_awaiting_a_decision()
    {
        var harness = await OpenAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.PendingInboundCount, Is.EqualTo(1));
            Assert.That(harness.Workspace.HasPendingInbound, Is.True);
            Assert.That(harness.Workspace.Inbound[0].NeedsDecision, Is.True);
            Assert.That(
                harness.Workspace.Inbound[0].AuthorizesAccess,
                Is.False,
                "a pending grant is never presented as live access");
        });
    }

    [Test]
    public async Task An_already_live_inbound_grant_is_not_counted_as_awaiting_a_decision()
    {
        var harness = WithGrants(new ExplorerTenantGrants
        {
            TenantId = MyTenantSample.TenantId,
            Issued = [],
            Received =
            [
                MyTenantSample.Grant(
                    granter: MyTenantSample.OtherTenantId,
                    grantee: MyTenantSample.TenantId,
                    state: ExplorerTenantGrantState.Active),
            ],
        });
        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Sharing);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.PendingInboundCount, Is.Zero);
            Assert.That(harness.Workspace.Inbound[0].AuthorizesAccess, Is.True);
            Assert.That(harness.Workspace.Inbound[0].CanRevoke, Is.True);
        });
    }

    [Test]
    public async Task A_refused_grant_read_leaves_both_lists_empty()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.Grants = TenantOperationResult<ExplorerTenantGrants>.Failure(
                TenantOperationStatus.Denied,
                "refused"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Inbound, Is.Empty);
            Assert.That(harness.Workspace.Outbound, Is.Empty);
            Assert.That(harness.Workspace.PendingInboundCount, Is.Zero);
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused"));
        });
    }

    [Test]
    public async Task Approving_an_inbound_offer_names_the_grants_own_parties()
    {
        var harness = await OpenAsync();

        await harness.Workspace.ApproveGrantAsync(harness.Workspace.Inbound[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.ApproveCalls, Has.Count.EqualTo(1));
            Assert.That(
                harness.Service.ApproveCalls[0].Granter,
                Is.EqualTo(MyTenantSample.OtherTenantId));
            Assert.That(harness.Service.ApproveCalls[0].Grantee, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(harness.Service.ApproveCalls[0].Scope, Is.EqualTo(MyTenantSample.Scope));
        });
    }

    [Test]
    public async Task Rejecting_an_inbound_offer_closes_it()
    {
        var harness = await OpenAsync();

        await harness.Workspace.RejectGrantAsync(harness.Workspace.Inbound[0]);

        Assert.That(harness.Service.RejectCalls, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Withdrawing_a_live_grant_is_permitted_from_either_side()
    {
        var harness = WithGrants(new ExplorerTenantGrants
        {
            TenantId = MyTenantSample.TenantId,
            Issued =
            [
                MyTenantSample.Grant(
                    granter: MyTenantSample.TenantId,
                    grantee: MyTenantSample.OtherTenantId,
                    state: ExplorerTenantGrantState.Active),
            ],
            Received = [],
        });
        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Sharing);

        await harness.Workspace.RevokeGrantAsync(harness.Workspace.Outbound[0]);

        Assert.That(harness.Service.RevokeCalls, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task A_pending_outbound_offer_cannot_be_withdrawn_and_nothing_is_sent()
    {
        var harness = await OpenAsync();

        await harness.Workspace.RevokeGrantAsync(harness.Workspace.Outbound[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.RevokeCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(TenantGrantScope.WrongStateMessage));
        });
    }

    [Test]
    public async Task Offering_a_grant_sends_this_tenant_as_the_granter()
    {
        var harness = await OpenAsync();
        harness.Workspace.OfferGranteeTenantId = " globex ";
        harness.Workspace.OfferScope = " t/acme/orders ";
        harness.Workspace.OfferOperations = ExplorerTenantGrantAccess.ReadWrite;

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Has.Count.EqualTo(1));
            Assert.That(harness.Service.OfferCalls[0].Granter, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(harness.Service.OfferCalls[0].Grantee, Is.EqualTo(MyTenantSample.OtherTenantId));
            Assert.That(harness.Service.OfferCalls[0].Scope, Is.EqualTo(MyTenantSample.Scope));
            Assert.That(harness.Workspace.OfferGranteeTenantId, Is.Empty, "the form clears on success");
            Assert.That(harness.Workspace.OfferScope, Is.Empty);
        });
    }

    [Test]
    public async Task An_empty_grantee_is_refused_before_a_call_is_made()
    {
        var harness = await OpenAsync();
        harness.Workspace.OfferScope = "t/acme/orders";

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.EmptyGranteeRefusal));
        });
    }

    [Test]
    public async Task An_empty_scope_is_refused_before_a_call_is_made()
    {
        var harness = await OpenAsync();
        harness.Workspace.OfferGranteeTenantId = MyTenantSample.OtherTenantId;

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.EmptyScopeRefusal));
        });
    }

    [Test]
    public async Task An_offer_authorizing_nothing_is_refused()
    {
        var harness = await OpenAsync();
        harness.Workspace.OfferGranteeTenantId = MyTenantSample.OtherTenantId;
        harness.Workspace.OfferScope = "t/acme/orders";
        harness.Workspace.OfferOperations = ExplorerTenantGrantAccess.None;

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.NoOperationsRefusal));
        });
    }

    [Test]
    public async Task A_tenant_offering_itself_a_grant_is_refused()
    {
        var harness = await OpenAsync();
        harness.Workspace.OfferGranteeTenantId = MyTenantSample.TenantId;
        harness.Workspace.OfferScope = "t/acme/orders";

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.SelfGrantRefusal));
        });
    }

    [Test]
    public async Task A_refused_offer_keeps_the_form_so_the_caller_can_correct_it()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.GrantChange = TenantOperationResult<ExplorerTenantGrantChange>.Failure(
                TenantOperationStatus.GrantTransitionRejected,
                "already live"));
        harness.Workspace.OfferGranteeTenantId = MyTenantSample.OtherTenantId;
        harness.Workspace.OfferScope = "t/acme/orders";

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.OfferGranteeTenantId, Is.EqualTo(MyTenantSample.OtherTenantId));
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("already live"));
            Assert.That(
                harness.Workspace.LastNotice?.Guidance,
                Is.EqualTo(MyTenantNotice.GrantTransitionGuidance));
        });
    }

    [Test]
    public async Task A_grant_not_found_refusal_is_reported_specifically()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.GrantChange = TenantOperationResult<ExplorerTenantGrantChange>.Failure(
                TenantOperationStatus.GrantNotFound,
                "no such grant"));

        await harness.Workspace.ApproveGrantAsync(harness.Workspace.Inbound[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.LastNotice?.Status, Is.EqualTo(TenantOperationStatus.GrantNotFound));
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("no such grant"));
        });
    }

    [Test]
    public async Task A_successful_transition_re_reads_the_grants()
    {
        var harness = await OpenAsync();
        var before = harness.Service.TenantIdsTouched.Count;

        await harness.Workspace.ApproveGrantAsync(harness.Workspace.Inbound[0]);

        Assert.That(harness.Service.TenantIdsTouched.Count, Is.GreaterThan(before));
    }

    [Test]
    public async Task A_denied_gate_makes_every_sharing_action_a_no_op()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            access: ExplorerPluginAccess.Denied);
        harness.Workspace.OfferGranteeTenantId = MyTenantSample.OtherTenantId;
        harness.Workspace.OfferScope = "t/acme/orders";

        await harness.Workspace.OfferGrantAsync();
        await harness.Workspace.ApproveGrantAsync(
            TenantGrantRow.For(
                MyTenantSample.TenantId,
                MyTenantSample.Grant(
                    granter: MyTenantSample.OtherTenantId,
                    grantee: MyTenantSample.TenantId)));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Is.Empty);
            Assert.That(harness.Service.ApproveCalls, Is.Empty);
        });
    }

    [Test]
    public async Task The_row_arrays_are_reused_across_reloads_of_the_same_size()
    {
        var harness = await OpenAsync();
        var before = harness.Workspace.Inbound;

        await harness.Workspace.ReloadGrantsAsync();

        Assert.That(
            harness.Workspace.Inbound,
            Is.SameAs(before),
            "a reload of the same grant count refills the existing array");
    }
}
