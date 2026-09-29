using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Shell.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Tenancy;

/// <summary>
/// One tenant's administration pages: the overview with its lifecycle verbs
/// (suspend confirmed, delete confirmed by typing the id), the reserved default
/// tenant's limits, a tenant not listed for the caller, the grants, admin-subject
/// and region pages, and a non-operator sent to their own workspace.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyTenantPageTests : TenancyTestContext
{
    [Test]
    public void The_overview_shows_state_kind_residency_apps_workspace_and_quota()
    {
        UseTenancyAs(isOperator: true);

        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("acme"));
            Assert.That(Facts(cut), Is.EqualTo(new Dictionary<string, string>
            {
                ["State"] = "Active",
                ["Kind"] = "Tenant",
                ["Resident in"] = "eu-west",
                ["Allowed"] = "eu-west",
                ["Apps"] = "2 apps installed",
                ["Workspace"] = "Open the tenant's workspace",
            }));
            Assert.That(cut.FindAll(".lt-tenancy-nav__link").Select(link => link.TextContent), Is.EqualTo(new[] { "Overview", "Grants", "Admin subjects", "Regions" }));
            Assert.That(cut.FindAll(".lt-tenancy-section__title").Select(title => title.TextContent), Is.EqualTo(new[] { "Lifecycle", "Quota" }));
        });
    }

    [Test]
    public void Suspend_is_confirmed_and_resume_is_immediate()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Suspend tenant"));

        TenancyForms.Button(cut, "Suspend tenant").Click();
        Assert.That(Cluster.Tenants["acme"].Status, Is.EqualTo(TenantLifecycleStatus.Active), "nothing happens until confirmed");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Suspend tenant acme?")));
        TenancyForms.Button(cut, "Suspend").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["acme"].Status, Is.EqualTo(TenantLifecycleStatus.Suspended));
            Assert.That(Facts(cut)["State"], Is.EqualTo("Suspended"));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme is suspended."));
        });

        TenancyForms.Button(cut, "Resume tenant").Click();
        cut.WaitUntil(() => Assert.That(Cluster.Tenants["acme"].Status, Is.EqualTo(TenantLifecycleStatus.Active)));
    }

    [Test]
    public void An_unchanged_status_says_so()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Suspend tenant"));
        Cluster.Tenants["acme"].Status = TenantLifecycleStatus.Suspended;

        TenancyForms.Button(cut, "Suspend tenant").Click();
        TenancyForms.Button(cut, "Suspend").Click();

        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant acme was already suspended.")));
    }

    [Test]
    public void Delete_needs_the_tenant_id_typed_and_reports_the_cascade()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex");
        var cut = RenderAt<TenancyTenantPage>("tenancy/globex");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Delete tenant"));

        TenancyForms.Button(cut, "Delete tenant").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__name").TextContent, Is.EqualTo("globex")));
        Assert.That(cut.Find(".lt-confirm button[type=submit]").HasAttribute("disabled"), Is.True);

        TenancyForms.Type(cut, "Tenant name", "globex");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants.ContainsKey("globex"), Is.False);
            Assert.That(Navigation.Uri, Does.EndWith("/tenancy"));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant globex deleted; 3 trees soft-deleted."));
        });
    }

    [Test]
    public void A_refused_lifecycle_change_is_a_toast_and_nothing_moves()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Fail(nameof(FakeTenancyCluster.DeleteTenantAsync), FakeTenancyCluster.Denied());
        Cluster.Fail(nameof(FakeTenancyCluster.ResumeTenantAsync), new TimeoutException());
        Cluster.Tenants["acme"].Status = TenantLifecycleStatus.Suspended;
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Delete tenant"));

        TenancyForms.Button(cut, "Delete tenant").Click();
        TenancyForms.Type(cut, "Tenant name", "acme");
        cut.Find("form.lt-confirm").Submit();
        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Is.EqualTo(TenancyFailure.NotPermittedMessage)));

        TenancyForms.Button(cut, "Resume tenant").Click();
        cut.WaitUntil(() => Assert.That(Services.GetToasts().Last().Message, Is.EqualTo(TenancyFailure.NoAnswerMessage)));
        Assert.That(Navigation.Uri, Does.EndWith("/tenancy/acme"));
    }

    [Test]
    public void The_default_tenant_offers_no_lifecycle_verbs_and_no_quota_editor()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Tenants[TenantId.DefaultId].Listed = true;

        var cut = RenderAt<TenancyTenantPage>("tenancy/default");

        cut.WaitUntil(() =>
        {
            Assert.That(Facts(cut)["Kind"], Is.EqualTo("The reserved default tenant"));
            Assert.That(TenancyForms.HasButton(cut, "Suspend tenant") || TenancyForms.HasButton(cut, "Delete tenant"), Is.False);
            Assert.That(TenancyForms.HasButton(cut, "Edit quotas"), Is.False);
            Assert.That(cut.FindAll(".lt-tenancy-note").Select(note => note.TextContent), Has.Some.Contains("cannot be suspended or deleted"));
        });
    }

    [Test]
    public void A_tenant_not_listed_for_the_operator_is_still_administered_through_its_regions()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("hidden", admins: ["someone-else"], listed: false);

        var cut = RenderAt<TenancyTenantPage>("tenancy/hidden");

        cut.WaitUntil(() =>
        {
            Assert.That(Facts(cut)["State"], Is.EqualTo("Not listed for you: it has no admin subject you hold"));
            Assert.That(Facts(cut)["Workspace"], Is.EqualTo("Scope to this tenant"));
            Assert.That(Facts(cut)["Apps"], Is.EqualTo("Installed apps"));
            Assert.That(TenancyForms.HasButton(cut, "Suspend tenant"), Is.True);
        });
    }

    [Test]
    [TestCase("tenancy/nobody")]
    [TestCase("tenancy/Not-A-Tenant")]
    public void An_unknown_or_malformed_tenant_is_not_found(string address)
    {
        UseTenancyAs(isOperator: true);
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt<TenancyTenantPage>(address);

        Assert.That(notFound, Is.EqualTo(1));
    }

    [Test]
    public void A_failed_read_offers_a_retry()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Fail(nameof(FakeTenancyCluster.GetTenantAsync), new TimeoutException());
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("This tenant could not be read")));

        Cluster.Heal(nameof(FakeTenancyCluster.GetTenantAsync));
        TenancyForms.Button(cut, "Try again").Click();

        cut.WaitUntil(() => Assert.That(Facts(cut)["State"], Is.EqualTo("Active")));
    }

    [Test]
    public void A_tenant_admin_is_sent_from_administration_to_the_same_section_of_their_workspace()
    {
        UseTenancyAs(isOperator: false);

        RenderAt<TenancyTenantPage>("tenancy/acme");
        Assert.That(Navigation.Uri, Does.EndWith("/t/acme/tenancy"));

        RenderAt<TenancyGrantsPage>("tenancy/acme/grants");
        Assert.That(Navigation.Uri, Does.EndWith("/t/acme/tenancy/sharing"));

        RenderAt<TenancyAccessPage>("tenancy/acme/access");
        Assert.That(Navigation.Uri, Does.EndWith("/t/acme/tenancy/members"));

        RenderAt<TenancyRegionsPage>("tenancy/acme/regions");
        Assert.That(Navigation.Uri, Does.EndWith("/t/acme/tenancy/regions"));
    }

    [Test]
    public void The_grants_page_lists_the_tenants_grants_under_its_navigation()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex").WithGrant("globex", "acme", "orders", TenantGrantLifecycleState.Pending);

        var cut = RenderAt<TenancyGrantsPage>("tenancy/acme/grants");

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentTab(cut), Is.EqualTo("Grants"));
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent.Trim()), Does.Contain("globex"));
        });
    }

    [Test]
    public void The_access_page_lists_the_tenants_admin_subjects_under_its_navigation()
    {
        UseTenancyAs(isOperator: true);

        var cut = RenderAt<TenancyAccessPage>("tenancy/acme/access");

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentTab(cut), Is.EqualTo("Admin subjects"));
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent.Trim()), Is.EqualTo(new[] { FakeTenancyCluster.Caller }));
        });
    }

    [Test]
    public void The_regions_page_lets_an_operator_change_the_allowed_set()
    {
        UseTenancyAs(isOperator: true);

        var cut = RenderAt<TenancyRegionsPage>("tenancy/acme/regions");

        cut.WaitUntil(() =>
        {
            Assert.That(CurrentTab(cut), Is.EqualTo("Regions"));
            Assert.That(TenancyForms.HasButton(cut, "Save allowed regions"), Is.True);
        });
    }

    [Test]
    public void A_section_page_whose_standing_cannot_be_proven_says_so()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Fail(nameof(FakeTenancyCluster.GetCurrentTenantAsync), FakeTenancyCluster.Denied());

        var cut = RenderAt<TenancyAccessPage>("tenancy/acme/access");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not permitted")));
    }

    [Test]
    public void A_section_page_for_a_malformed_tenant_is_not_found()
    {
        UseTenancyAs(isOperator: true);
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt<TenancyGrantsPage>("tenancy/Bad_Id/grants");
        RenderAt<TenancyAccessPage>("tenancy/Bad_Id/access");
        RenderAt<TenancyRegionsPage>("tenancy/Bad_Id/regions");

        Assert.That(notFound, Is.EqualTo(3));
    }

    [Test]
    public void Below_the_small_breakpoint_the_lifecycle_confirmation_is_a_sheet()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme", LtBreakpoint.Compact);
        cut.WaitUntil(() => TenancyForms.Button(cut, "Suspend tenant"));

        TenancyForms.Button(cut, "Suspend tenant").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").ClassList, Does.Contain("lt-dialog--end")));
    }

    private static Dictionary<string, string> Facts<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll(".lt-dl__row").Take(6).ToDictionary(
            row => row.QuerySelector("dt")!.TextContent.Trim(),
            row => row.QuerySelector("dd")!.TextContent.Trim());

    private static string CurrentTab<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        cut.FindAll(".lt-tenancy-nav__link").Single(link => link.GetAttribute("aria-current") == "page").TextContent;
}
