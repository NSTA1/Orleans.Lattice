using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Shell.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Tenancy;

/// <summary>
/// A tenant's cross-tenant grants: both directions in decision order, the
/// validated offer, approve, reject after a confirmation, revoke confirmed by
/// typing the other tenant, refusals, and the default tenant's exclusion.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyGrantsTests : TenancyTestContext
{
    [Test]
    public void Both_directions_are_listed_pending_first_with_their_verbs()
    {
        Cluster.WithTenant("globex").WithTenant("initech")
            .WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Active, TenantGrantAccess.ReadWrite)
            .WithGrant("initech", "acme", "stock", TenantGrantLifecycleState.Pending)
            .WithGrant("acme", "globex", "invoices", TenantGrantLifecycleState.Pending)
            .WithGrant("acme", "initech", "ledger", TenantGrantLifecycleState.Revoked);

        var cut = RenderGrants();

        cut.WaitUntil(() =>
        {
            var tables = cut.FindAll("table");
            Assert.That(tables, Has.Count.EqualTo(2));
            var received = tables[0].QuerySelectorAll("tbody tr").Select(Cells).ToArray();
            var issued = tables[1].QuerySelectorAll("tbody tr").Select(Cells).ToArray();
            Assert.That(received, Is.EqualTo(new[]
            {
                new[] { "initech", "stock", "Read", "Pending" },
                new[] { "globex", "orders", "Read and write", "Active" },
            }));
            Assert.That(issued, Is.EqualTo(new[]
            {
                new[] { "globex", "invoices", "Read", "Pending" },
                new[] { "initech", "ledger", "Read", "Revoked" },
            }));
            Assert.That(tables[0].QuerySelectorAll("tbody tr").Select(Verbs), Is.EqualTo(new[] { "Approve|Reject", "Revoke" }));
            Assert.That(tables[1].QuerySelectorAll("tbody tr").Select(Verbs), Is.EqualTo(new[] { string.Empty, string.Empty }));
            Assert.That(tables[1].QuerySelector("tbody .lt-tenancy-quiet")!.TextContent, Is.EqualTo("Awaiting tenant globex"));
        });
    }

    [Test]
    public void With_no_grant_each_direction_says_so()
    {
        var cut = RenderGrants();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table__empty").Select(cell => cell.TextContent.Trim()),
            Is.EqualTo(new[] { "No tenant has offered tenant acme a grant.", "Tenant acme has not offered a grant." })));
    }

    [Test]
    public void An_offer_is_validated_then_sent_and_the_list_reloads()
    {
        Cluster.WithTenant("globex");
        var cut = OpenOffer();

        TenancyForms.Type(cut, "To tenant", "globex");
        TenancyForms.Type(cut, "Scope", "orders/");
        TenancyForms.Choose(cut, "Access", nameof(TenantGrantAccess.ReadWrite));
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() =>
        {
            var grant = Cluster.GrantList.Single();
            Assert.That((grant.GranterTenantId, grant.GranteeTenantId, grant.Scope, grant.Operations, grant.State),
                Is.EqualTo(("acme", "globex", "orders/", TenantGrantAccess.ReadWrite, TenantGrantLifecycleState.Pending)));
            Assert.That(cut.FindAll("form.lt-tenancy-form"), Is.Empty);
            Assert.That(cut.FindAll("table")[1].QuerySelector("tbody th")!.TextContent.Trim(), Is.EqualTo("globex"));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Offered orders/ to tenant globex. It takes effect once tenant globex approves."));
        });
    }

    [Test]
    [TestCase("", "orders", "To tenant", "Enter the id of the tenant to share with.")]
    [TestCase("Not Valid", "orders", "To tenant", "A tenant id is lower-case letters, digits and hyphens.")]
    [TestCase("default", "orders", "To tenant", "The reserved default tenant takes no part in grants.")]
    [TestCase("acme", "orders", "To tenant", "A tenant cannot grant to itself.")]
    [TestCase("globex", " ", "Scope", "Enter the tree name or prefix to share.")]
    public void An_invalid_offer_is_refused_before_anything_is_sent(string grantee, string scope, string field, string error)
    {
        var cut = OpenOffer();

        TenancyForms.Type(cut, "To tenant", grantee);
        TenancyForms.Type(cut, "Scope", scope);
        cut.Find("form.lt-tenancy-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, field), Is.EqualTo(error));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.OfferGrantAsync)));
        });
    }

    [Test]
    public void The_clusters_refusal_of_an_offer_is_shown_in_the_form()
    {
        Cluster.WithTenant("globex").WithGrant("acme", "globex", "orders", TenantGrantLifecycleState.Active);
        var cut = OpenOffer();

        TenancyForms.Type(cut, "To tenant", "globex");
        TenancyForms.Type(cut, "Scope", "orders");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-form__error").TextContent, Is.EqualTo("The grant is active, so it cannot become pending.")));
    }

    [Test]
    public void A_pending_offer_is_approved()
    {
        Cluster.WithTenant("globex").WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Pending);
        var cut = RenderGrants();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Approve"));

        TenancyForms.Button(cut, "Approve").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.GrantList.Single().State, Is.EqualTo(TenantGrantLifecycleState.Active));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Approved. Tenant acme can now read orders in tenant globex."));
            Assert.That(TenancyForms.HasButton(cut, "Revoke"), Is.True);
        });
    }

    [Test]
    public void A_pending_offer_is_rejected_only_after_a_confirmation()
    {
        Cluster.WithTenant("globex").WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Pending);
        var cut = RenderGrants();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Reject"));

        cut.FindAll("button").First(button => button.TextContent.Trim() == "Reject").Click();
        Assert.That(Cluster.GrantList.Single().State, Is.EqualTo(TenantGrantLifecycleState.Pending));
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Reject the offer?")));

        cut.Find("[role=alertdialog] .lt-dialog__actions .lt-btn--destructive").Click();

        cut.WaitUntil(() => Assert.That(Cluster.GrantList.Single().State, Is.EqualTo(TenantGrantLifecycleState.Rejected)));
    }

    [Test]
    public void An_active_grant_is_revoked_after_typing_the_other_tenant()
    {
        Cluster.WithTenant("globex").WithGrant("acme", "globex", "orders", TenantGrantLifecycleState.Active);
        var cut = RenderGrants();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Revoke"));

        TenancyForms.Button(cut, "Revoke").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo("globex")));
        TenancyForms.Type(cut, "Tenant name", "globex");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.GrantList.Single().State, Is.EqualTo(TenantGrantLifecycleState.Revoked));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Revoked. Tenant globex no longer has access to orders."));
        });
    }

    [Test]
    public void A_received_grant_is_revoked_by_typing_the_granting_tenant()
    {
        Cluster.WithTenant("globex").WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Active);
        var cut = RenderGrants();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Revoke"));

        TenancyForms.Button(cut, "Revoke").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo("globex")));
    }

    [Test]
    public void A_refused_transition_is_a_toast_and_the_list_reloads()
    {
        Cluster.WithTenant("globex").WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Pending);
        Cluster.Fail(nameof(FakeTenancyCluster.ApproveGrantAsync), new TenantGrantNotFoundException("globex", "acme", "orders"));
        var cut = RenderGrants();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Approve"));

        TenancyForms.Button(cut, "Approve").Click();

        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("That grant no longer exists.")));
    }

    [Test]
    public void The_default_tenant_takes_no_part_and_cannot_offer()
    {
        var cut = RenderSection<TenancyGrants>(parameters => parameters.Add(grants => grants.TenantId, TenantId.DefaultId));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-command]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.FindAll(".lt-tenancy-note").Select(note => note.TextContent), Has.Some.Contains("takes no part in cross-tenant grants"));
        });
    }

    [Test]
    public void A_refused_read_is_not_permitted_and_a_failed_one_can_be_retried()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.ListGrantsAsync), FakeTenancyCluster.Denied());
        var denied = RenderGrants();
        denied.WaitUntil(() => Assert.That(denied.Find(".lt-empty h3").TextContent, Is.EqualTo("Not permitted")));

        Cluster.Fail(nameof(FakeTenancyCluster.ListGrantsAsync), new TimeoutException());
        var failed = RenderGrants();
        failed.WaitUntil(() => Assert.That(failed.Find(".lt-empty h3").TextContent, Is.EqualTo("Grants could not be read")));
        Cluster.Heal(nameof(FakeTenancyCluster.ListGrantsAsync));
        TenancyForms.Button(failed, "Try again").Click();
        failed.WaitUntil(() => Assert.That(failed.FindAll("table"), Has.Count.EqualTo(2)));
    }

    [Test]
    public void While_grants_load_a_skeleton_is_shown_and_offering_is_off()
    {
        var hold = Cluster.Hold(nameof(FakeTenancyCluster.ListGrantsAsync));

        var cut = RenderGrants();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("[data-lt-command]").HasAttribute("disabled"), Is.True);
        });
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.FindAll("table"), Has.Count.EqualTo(2)));
    }

    [Test]
    public void Below_the_small_breakpoint_grants_are_rows_whose_sheet_carries_the_verbs()
    {
        Cluster.WithTenant("globex").WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Pending);
        var cut = RenderGrants(LtBreakpoint.Compact);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent.Trim(), Is.EqualTo("globex"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent, Does.Contain("orders, read"));
        });

        cut.FindAll(".lt-table-list__open")[0].Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog .lt-dialog__actions button").Select(button => button.TextContent.Trim()), Is.EqualTo(new[] { "Approve", "Reject" })));
    }

    private IRenderedComponent<TenancyGrants> RenderGrants(LtBreakpoint? band = null) =>
        RenderSection<TenancyGrants>(parameters => parameters.Add(grants => grants.TenantId, "acme"), band);

    private IRenderedComponent<TenancyGrants> OpenOffer()
    {
        var cut = RenderSection<TenancyGrants>(parameters => parameters.Add(grants => grants.TenantId, "acme").Add(grants => grants.OpenOfferOnLoad, true));
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));
        return cut;
    }

    private static string[] Cells(AngleSharp.Dom.IElement row) =>
        [.. row.Children.Take(4).Select(cell => cell.TextContent.Trim())];

    private static string Verbs(AngleSharp.Dom.IElement row) =>
        string.Join('|', row.QuerySelectorAll("button").Select(button => button.TextContent.Trim()));
}
