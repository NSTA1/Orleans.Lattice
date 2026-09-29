using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The tenant directory: the list with state, quota use, residency and apps,
/// the active tenant marked current, the validated create form and its command,
/// a non-operator sent to their own tenant, and the loading, error, empty and
/// compact states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class TenancyDirectoryPageTests : TenancyTestContext
{
    [Test]
    public void Every_tenant_is_listed_with_its_state_quota_use_residency_and_apps()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex", TenantLifecycleStatus.Suspended);
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxKeys = 100 };
        Cluster.Tenants["acme"].Usage["keys"] = 42;

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(cut.FindAll("tbody th a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "tenancy/acme", "tenancy/globex" }));
            Assert.That(rows[0].QuerySelectorAll("td").Select(cell => cell.TextContent.Trim()).ToArray(), Is.EqualTo(new[] { "Active", "Keys 42%", "eu-west", "2 apps", "Open workspace" }));
            Assert.That(rows[1].QuerySelectorAll("td").Select(cell => cell.TextContent.Trim()).ToArray(), Is.EqualTo(new[] { "Suspended", "Unbounded", "eu-west", "Apps", "Scope to tenant" }));
            Assert.That(cut.Find(".lt-tenancy-count").TextContent, Is.EqualTo("2 tenants, 1 suspended"));
        });
    }

    [Test]
    public void The_active_tenant_is_the_current_row_and_links_go_where_the_spec_says()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex");

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr[aria-current]").Select(row => row.QuerySelector("th")!.TextContent.Trim()), Is.EqualTo(new[] { "acme" }));
            var links = cut.FindAll("tbody tr")[1].QuerySelectorAll("td a").Select(link => link.GetAttribute("href")).ToArray();
            Assert.That(links, Is.EqualTo(new[] { "tenancy/globex/regions", "t/globex/apps", "t/globex/tenancy" }));
            Assert.That(cut.FindAll("tbody tr")[0].QuerySelector("td a[href='t/acme/apps']")!.GetAttribute("aria-label"), Is.EqualTo("2 apps installed for tenant acme"));
            Assert.That(cut.FindAll("tbody tr")[0].QuerySelector("td a[href='tenancy/acme/regions']")!.GetAttribute("aria-label"), Is.EqualTo("Resident in eu-west; open the regions of tenant acme"));
        });
    }

    [Test]
    public void A_row_whose_details_cannot_be_read_says_so_and_the_list_stands()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Fail(nameof(FakeTenancyCluster.GetQuotaUsageAsync), new TimeoutException());

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[0].QuerySelectorAll("td")[1].TextContent.Trim(), Is.EqualTo("Unknown")));
    }

    [Test]
    public void While_a_rows_details_are_read_it_says_so()
    {
        UseTenancyAs(isOperator: true);
        var hold = Cluster.Hold(nameof(FakeTenancyCluster.GetTenantAsync));

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[0].QuerySelectorAll("td")[1].TextContent.Trim(), Is.EqualTo("Reading...")));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[0].QuerySelectorAll("td")[2].TextContent.Trim(), Is.EqualTo("eu-west")));
    }

    [Test]
    public void The_search_narrows_the_list()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex").WithTenant("initech");
        var cut = RenderAt<TenancyDirectoryPage>("tenancy");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3)));

        cut.Find("input[type=search]").Input("glo");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent.Trim()), Is.EqualTo(new[] { "globex" }));
            Assert.That(cut.Find(".lt-tenancy-count").TextContent, Is.EqualTo("1 of 3 tenants"));
        });

        cut.Find("input[type=search]").Input("zzz");
        Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No tenant matches."));
    }

    [Test]
    public void With_no_listed_tenant_the_empty_state_explains_why()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Tenants["acme"].Listed = false;

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Does.StartWith("No tenant is visible to you yet.")));
    }

    [Test]
    public void While_tenants_load_the_page_shows_a_skeleton()
    {
        UseTenancyAs(isOperator: true);
        var hold = Cluster.Hold(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync));

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_failed_read_offers_a_retry_that_recovers()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Fail(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync), new TimeoutException());
        var cut = RenderAt<TenancyDirectoryPage>("tenancy");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Tenants could not be read")));

        Cluster.Heal(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync));
        cut.FindAll("button").Single(button => button.TextContent == "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_refused_read_is_not_permitted_and_offers_no_retry()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Fail(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync), FakeTenancyCluster.Denied());

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not permitted"));
            Assert.That(cut.FindAll("button").Any(button => button.TextContent == "Try again"), Is.False);
        });
    }

    [Test]
    public void A_tenant_admin_is_sent_to_their_own_tenant_replacing_the_history_entry()
    {
        UseTenancyAs(isOperator: false);

        RenderAt<TenancyDirectoryPage>("tenancy");

        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Does.EndWith("/t/acme/tenancy"));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync)));
        });
    }

    [Test]
    public async Task The_create_command_has_a_visible_control_and_its_target_opens_the_form()
    {
        UseTenancyAs(isOperator: true);
        var area = CreateArea();
        await area.GetAvailabilityAsync(CancellationToken.None);
        var command = area.Commands.Single(candidate => candidate.Id == TenancyArea.CreateTenantCommandId);

        var cut = RenderAt<TenancyDirectoryPage>(command.Target!.ToHref());

        cut.WaitUntil(() =>
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("New tenant"));
        });
    }

    [Test]
    [TestCase("", "Enter the tenant id.")]
    [TestCase("Acme Corp", "A tenant id is 1 to 63 lower-case letters, digits and hyphens, and does not start or end with a hyphen.")]
    [TestCase("default", "The default tenant already exists.")]
    [TestCase("acme", "A tenant with the id acme already exists.")]
    public void An_invalid_id_is_refused_before_anything_is_written(string id, string error)
    {
        var cut = OpenCreate();

        TenancyForms.Type(cut, "Tenant id", id);
        cut.Find("form.lt-tenancy-form").Submit();

        Assert.Multiple(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, "Tenant id"), Is.EqualTo(error));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.CreateTenantAsync)));
        });
    }

    [Test]
    public void A_tenant_is_created_with_the_listed_admins_and_the_page_moves_to_it()
    {
        var cut = OpenCreate();

        TenancyForms.Type(cut, "Tenant id", "globex");
        TenancyForms.Type(cut, "Admin subjects (optional)", "alice, bob,alice");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants["globex"].Admins, Is.EqualTo(new[] { "alice", "bob" }));
            Assert.That(Navigation.Uri, Does.EndWith("/tenancy/globex"));
            Assert.That(Services.GetToasts().Single().Message, Is.EqualTo("Tenant globex created, administered by alice, bob."));
        });
    }

    [Test]
    public void Left_blank_the_admin_subjects_default_to_the_caller()
    {
        var cut = OpenCreate();

        TenancyForms.Type(cut, "Tenant id", "globex");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() => Assert.That(Cluster.Tenants["globex"].Admins, Is.EqualTo(new[] { FakeTenancyCluster.Caller })));
    }

    [Test]
    public void The_clusters_refusal_is_shown_beside_the_id_and_other_faults_below_the_form()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.CreateTenantAsync), new TenantAlreadyExistsException("globex"));
        var cut = OpenCreate();
        TenancyForms.Type(cut, "Tenant id", "globex");
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => Assert.That(TenancyForms.ErrorOf(cut, "Tenant id"), Is.EqualTo("A tenant with the id globex already exists.")));

        Cluster.Fail(nameof(FakeTenancyCluster.CreateTenantAsync), FakeTenancyCluster.Denied());
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-form__error").TextContent, Is.EqualTo(TenancyFailure.NotPermittedMessage)));
    }

    [Test]
    public void Cancel_closes_the_create_form()
    {
        var cut = OpenCreate();

        cut.FindAll("button").Single(button => button.TextContent == "Cancel").Click();

        Assert.That(cut.FindAll("form.lt-tenancy-form"), Is.Empty);
    }

    [Test]
    public void Below_the_small_breakpoint_tenants_are_rows_opening_a_sheet_and_create_is_a_sheet()
    {
        UseTenancyAs(isOperator: true);

        var cut = RenderAt<TenancyDirectoryPage>("tenancy", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent.Trim(), Is.EqualTo("acme"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent, Does.Contain("Active"));
        });

        cut.Find(".lt-table-list__open").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog .lt-dialog__actions a").Select(link => link.GetAttribute("href")),
            Is.EqualTo(new[] { "tenancy/acme", "tenancy/acme/regions", "t/acme/tenancy", "t/acme/apps" })));

        cut.FindAll("button").Single(button => button.TextContent == "New tenant").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog").Any(dialog => dialog.ClassList.Contains("lt-dialog--end")), Is.True));
    }

    private IRenderedComponent<TenancyDirectoryPage> OpenCreate()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyDirectoryPage>("tenancy?new=true");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));
        return cut;
    }
}
