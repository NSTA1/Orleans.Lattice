using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The acceptance criterion the issue states outright: <b>a tenant admin of
/// tenant A can never read or mutate tenant B through this plugin</b>.
/// <para>
/// Every test here asserts against the recorded calls on the operations seam
/// rather than against a message. That distinction matters: a test that only
/// checked for an error banner would still pass if the call had gone out and the
/// cluster had refused it. What is being proved is that <em>nothing left the
/// process</em>.
/// </para>
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceIsolationTests
{
    private const string A = MyTenantSample.TenantId;
    private const string B = MyTenantSample.OtherTenantId;
    private const string C = MyTenantSample.ThirdTenantId;

    /// <summary>
    /// A workspace scoped to tenant A whose grant report has been poisoned with
    /// grants belonging to other tenants - the shape a compromised or buggy
    /// server response would take.
    /// </summary>
    private static async Task<MyTenantWorkspaceHarness> ScopedToAAsync(ExplorerTenantGrants grants)
    {
        var harness = MyTenantWorkspaceHarness.Create(domain =>
        {
            domain.ActiveTenant = new ExplorerTenantId(A);
            domain.Service.Grants = TenantOperationResult<ExplorerTenantGrants>.Success(grants, "ok");
        });

        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Sharing);
        return harness;
    }

    [Test]
    public async Task A_cannot_approve_a_grant_offered_to_B()
    {
        // The named case in the issue. B is the grantee, so only B may approve.
        var harness = await ScopedToAAsync(new ExplorerTenantGrants
        {
            TenantId = A,
            Issued = [],
            Received = [MyTenantSample.Grant(granter: C, grantee: B)],
        });

        await harness.Workspace.ApproveGrantAsync(harness.Workspace.Inbound[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.ApproveCalls, Is.Empty, "no approval left the process");
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(TenantGrantScope.UnrelatedGrantMessage));
        });
    }

    [Test]
    public async Task A_cannot_approve_its_own_offer_to_B()
    {
        // The other half of the two-step agreement: A is the granter, so approving
        // would let one party complete both steps.
        var harness = await ScopedToAAsync(new ExplorerTenantGrants
        {
            TenantId = A,
            Issued = [MyTenantSample.Grant(granter: A, grantee: B)],
            Received = [],
        });

        await harness.Workspace.ApproveGrantAsync(harness.Workspace.Outbound[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.ApproveCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(TenantGrantScope.NotGranteeMessage));
        });
    }

    [Test]
    public async Task A_cannot_reject_a_grant_offered_to_B()
    {
        var harness = await ScopedToAAsync(new ExplorerTenantGrants
        {
            TenantId = A,
            Issued = [],
            Received = [MyTenantSample.Grant(granter: C, grantee: B)],
        });

        await harness.Workspace.RejectGrantAsync(harness.Workspace.Inbound[0]);

        Assert.That(harness.Service.RejectCalls, Is.Empty);
    }

    [Test]
    public async Task A_cannot_revoke_a_live_grant_between_B_and_C()
    {
        var harness = await ScopedToAAsync(new ExplorerTenantGrants
        {
            TenantId = A,
            Issued = [MyTenantSample.Grant(granter: B, grantee: C, state: ExplorerTenantGrantState.Active)],
            Received = [],
        });

        await harness.Workspace.RevokeGrantAsync(harness.Workspace.Outbound[0]);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.RevokeCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(TenantGrantScope.UnrelatedGrantMessage));
        });
    }

    [Test]
    public async Task A_cannot_offer_a_grant_from_B()
    {
        // The offer form takes only the grantee: the granter is always the active
        // tenant, so there is no input through which B could be named as granter.
        // This asserts the property the design relies on.
        var harness = await ScopedToAAsync(MyTenantSample.Grants(A));
        harness.Workspace.OfferGranteeTenantId = C;
        harness.Workspace.OfferScope = "t/globex/ledger";

        await harness.Workspace.OfferGrantAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.OfferCalls, Has.Count.EqualTo(1));
            Assert.That(
                harness.Service.OfferCalls[0].Granter,
                Is.EqualTo(A),
                "the granter is the active tenant and can never be another one");
            Assert.That(harness.Service.OfferCalls[0].Granter, Is.Not.EqualTo(B));
        });
    }

    [Test]
    public async Task A_row_belonging_to_another_tenant_offers_no_controls_at_all()
    {
        var harness = await ScopedToAAsync(new ExplorerTenantGrants
        {
            TenantId = A,
            Issued = [MyTenantSample.Grant(granter: B, grantee: C, state: ExplorerTenantGrantState.Pending)],
            Received = [MyTenantSample.Grant(granter: B, grantee: C, state: ExplorerTenantGrantState.Active)],
        });

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Outbound[0].Actions, Is.EqualTo(TenantGrantActions.None));
            Assert.That(harness.Workspace.Inbound[0].Actions, Is.EqualTo(TenantGrantActions.None));
            Assert.That(
                harness.Workspace.PendingInboundCount,
                Is.Zero,
                "another tenant's pending offer is not this tenant's decision to make");
        });
    }

    [Test]
    public async Task Every_read_the_surface_makes_names_only_the_active_tenant()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [MyTenantSample.Summary(A), MyTenantSample.Summary(B)],
                    "ok"));

        await harness.OpenAsync(MyTenantSurfaces.Members);
        await harness.OpenAsync(MyTenantSurfaces.Quota);
        await harness.OpenAsync(MyTenantSurfaces.Regions);
        await harness.OpenAsync(MyTenantSurfaces.Sharing);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.TenantIdsTouched, Is.Not.Empty);
            Assert.That(
                harness.Service.TenantIdsTouched,
                Has.All.EqualTo(A),
                "no surface reads a tenant other than the active one, even when others are accessible");
        });
    }

    [Test]
    public async Task Every_mutation_the_surface_makes_names_only_the_active_tenant()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();
        await harness.OpenAsync(MyTenantSurfaces.Members);
        harness.Workspace.NewAdminSubjectId = "user:hopper";
        await harness.Workspace.AddAdminSubjectAsync();
        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");

        await harness.OpenAsync(MyTenantSurfaces.Regions);
        harness.Workspace.ToggleRegion("eastus");
        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.AddSubjectCalls, Has.All.Matches<FakeTenantAdminService.SubjectCall>(
                call => call.TenantId == A));
            Assert.That(harness.Service.RemoveSubjectCalls, Has.All.Matches<FakeTenantAdminService.SubjectCall>(
                call => call.TenantId == A));
            Assert.That(harness.Service.ResidencyCalls, Has.All.Matches<FakeTenantAdminService.ResidencyCall>(
                call => call.TenantId == A));
        });
    }

    [Test]
    public async Task Switching_tenant_leaves_no_row_of_the_previous_tenant_behind()
    {
        // The isolation invariant at the caching layer: data loaded for A must not
        // survive into B's view of the same surface.
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [MyTenantSample.Summary(A), MyTenantSample.Summary(B)],
                    "ok"));

        await harness.OpenAsync(MyTenantSurfaces.Members);
        Assert.That(harness.Workspace.AdminSubjects, Is.Not.Empty, "A's membership loaded");

        harness.Service.Admins = TenantOperationResult<ExplorerTenantAdmins>.Failure(
            TenantOperationStatus.Denied,
            "B refuses this caller");
        await harness.Workspace.SwitchTenantAsync(B);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TenantId, Is.EqualTo(B));
            Assert.That(
                harness.Workspace.AdminSubjects,
                Is.Empty,
                "A's admin subjects must not still be on screen while viewing B");
        });
    }

    [Test]
    public async Task Switching_tenant_clears_the_quota_reading_and_the_grant_lists()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [MyTenantSample.Summary(A), MyTenantSample.Summary(B)],
                    "ok"));

        await harness.OpenAsync(MyTenantSurfaces.Quota);
        Assert.That(harness.Workspace.Usage, Is.Not.Null, "A's quota loaded");

        harness.Service.QuotaUsage = TenantOperationResult<ExplorerTenantQuotaUsage>.Failure(
            TenantOperationStatus.Denied,
            "B refuses this caller");
        harness.Service.Grants = TenantOperationResult<ExplorerTenantGrants>.Failure(
            TenantOperationStatus.Denied,
            "B refuses this caller");

        await harness.Workspace.SwitchTenantAsync(B);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Usage, Is.Null);
            Assert.That(harness.Workspace.Gauges, Is.Empty);
            Assert.That(harness.Workspace.Inbound, Is.Empty);
            Assert.That(harness.Workspace.Outbound, Is.Empty);
        });
    }

    [Test]
    public async Task A_workspace_with_no_active_tenant_mutates_nothing_at_all()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
        {
            domain.ActiveTenant = null;
            domain.Service.CurrentTenant = TenantOperationResult<ExplorerTenantSummary>.Failure(
                TenantOperationStatus.Denied,
                "no tenant");
        });

        harness.Workspace.NewAdminSubjectId = "user:hopper";
        await harness.Workspace.AddAdminSubjectAsync();
        await harness.Workspace.RemoveAdminSubjectAsync("user:ada");
        await harness.Workspace.ApplyResidencyAsync();
        harness.Workspace.OfferGranteeTenantId = B;
        harness.Workspace.OfferScope = "t/acme/orders";
        await harness.Workspace.OfferGrantAsync();
        await harness.Workspace.ApproveGrantAsync(
            TenantGrantRow.For(null, MyTenantSample.Grant(granter: B, grantee: A)));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TenantId, Is.Null);
            Assert.That(harness.Service.AddSubjectCalls, Is.Empty);
            Assert.That(harness.Service.RemoveSubjectCalls, Is.Empty);
            Assert.That(harness.Service.ResidencyCalls, Is.Empty);
            Assert.That(harness.Service.OfferCalls, Is.Empty);
            Assert.That(harness.Service.ApproveCalls, Is.Empty);
        });
    }
}
