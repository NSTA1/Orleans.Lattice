using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// "My tenant": the overview for a tenant admin and an operator, each section,
/// the offer command's control, unknown sections, and the failure states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class MyTenantPageTests : TenancyTestContext
{
    [Test]
    public void A_tenant_admin_sees_the_overview_without_the_directory_link()
    {
        UseTenancyAs(isOperator: false);
        Cluster.Tenants["acme"].Quotas = new TenantQuotasDescriptor { MaxBytes = 1000 };
        Cluster.Tenants["acme"].Usage["bytes"] = 250;

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("acme"));
            Assert.That(Facts(cut), Is.EqualTo(new Dictionary<string, string>
            {
                ["State"] = "Active",
                ["Kind"] = "Tenant",
                ["Your standing"] = "Admin subject of this tenant",
                ["Resident in"] = "eu-west",
                ["Allowed"] = "eu-west",
                ["Quota use"] = "Stored bytes 25%",
                ["Apps"] = "2 apps installed",
            }));
            Assert.That(cut.FindAll(".lt-dl a").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "t/acme/tenancy/regions", "t/acme/tenancy/regions", "t/acme/apps" }), "residency and the allowed set lead to the regions section");
            Assert.That(cut.FindAll(".lt-shell-page-lede a"), Is.Empty);
            Assert.That(cut.FindAll(".lt-tenancy-nav__link").Select(link => link.TextContent), Is.EqualTo(new[] { "Overview", "Members", "Quota", "Regions", "Sharing" }));
            Assert.That(cut.FindAll(".lt-tenancy-nav__link").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "t/acme/tenancy", "t/acme/tenancy/members", "t/acme/tenancy/quota", "t/acme/tenancy/regions", "t/acme/tenancy/sharing" }));
        });
    }

    [Test]
    public void An_operator_is_linked_to_the_directory_and_offered_their_other_tenants()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex", TenantLifecycleStatus.Suspended);

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede a").GetAttribute("href"), Is.EqualTo("tenancy"));
            Assert.That(Facts(cut)["Your standing"], Is.EqualTo("Platform operator"));
            Assert.That(cut.FindAll(".lt-tenancy-links a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "t/globex/tenancy" }));
            Assert.That(cut.Find(".lt-tenancy-links .lt-tenancy-quiet").TextContent, Is.EqualTo("Suspended"));
        });
    }

    [Test]
    public void A_suspended_tenant_says_what_that_means_and_an_unread_quota_is_unknown()
    {
        UseTenancyAs(isOperator: false);
        Cluster.Tenants["acme"].Status = TenantLifecycleStatus.Suspended;
        Cluster.Fail(nameof(FakeTenancyCluster.GetQuotaUsageAsync), new TimeoutException());
        Cluster.InstalledApps = null;

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");

        cut.WaitUntil(() =>
        {
            Assert.That(Facts(cut)["Quota use"], Is.EqualTo("Unknown"));
            Assert.That(Facts(cut)["Apps"], Is.EqualTo("Installed apps"));
            Assert.That(cut.FindAll(".lt-tenancy-note").Select(note => note.TextContent), Has.Some.Contains("This tenant is suspended"));
        });
    }

    [Test]
    [TestCase("members", "Members", "Admin subjects")]
    [TestCase("quota", "Quota", "Quota")]
    [TestCase("regions", "Regions", "Regions")]
    [TestCase("sharing", "Sharing", "Cross-tenant grants")]
    public void Each_section_is_its_own_tab(string section, string tab, string heading)
    {
        UseTenancyAs(isOperator: false);

        var cut = RenderAt<MyTenantPage>($"t/acme/tenancy/{section}");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-tenancy-nav__link").Single(link => link.GetAttribute("aria-current") == "page").TextContent, Is.EqualTo(tab));
            Assert.That(cut.Find(".lt-tenancy-section__title").TextContent, Is.EqualTo(heading));
        });
    }

    [Test]
    public void A_tenant_admin_cannot_change_the_allowed_regions_or_the_quota()
    {
        UseTenancyAs(isOperator: false);

        var regions = RenderAt<MyTenantPage>("t/acme/tenancy/regions");
        regions.WaitUntil(() => Assert.That(TenancyForms.HasButton(regions, "Apply residency"), Is.True));
        Assert.That(TenancyForms.HasButton(regions, "Save allowed regions"), Is.False);

        var quota = RenderAt<MyTenantPage>("t/acme/tenancy/quota");
        quota.WaitUntil(() => Assert.That(quota.FindAll("tbody tr"), Has.Count.EqualTo(5)));
        Assert.That(TenancyForms.HasButton(quota, "Edit quotas"), Is.False);
    }

    [Test]
    public async Task The_offer_command_has_a_visible_control_and_its_target_opens_the_form()
    {
        UseTenancyAs(isOperator: false);
        var area = CreateArea();
        await area.GetAvailabilityAsync(CancellationToken.None);
        var command = area.Commands.Single(candidate => candidate.Id == TenancyArea.OfferGrantCommandId);

        var cut = RenderAt<MyTenantPage>(command.Target!.ToHref());

        cut.WaitUntil(() =>
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("Offer a grant"));
        });
    }

    [Test]
    public async Task The_change_residency_command_has_a_visible_control_on_the_regions_section()
    {
        UseTenancyAs(isOperator: false);
        var area = CreateArea();
        await area.GetAvailabilityAsync(CancellationToken.None);
        var command = area.Commands.Single(candidate => candidate.Id == TenancyArea.ChangeResidencyCommandId);

        var cut = RenderAt<MyTenantPage>(command.Target!.ToHref());

        cut.WaitUntil(() =>
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find($"[data-lt-command=\"{command.Id}\"]").GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(TenancyForms.HasButton(cut, "Apply residency"), Is.True);
        });
    }

    [Test]
    public void A_tenant_with_residency_and_no_online_region_is_said_to_be_served_nowhere()
    {
        UseTenancyAs(isOperator: false);
        Cluster.Tenants["acme"].Regions[0] = Cluster.Tenants["acme"].Regions[0] with { Status = TenantRegionLifecycleStatus.Provisioning };

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-warning").TextContent, Does.StartWith("This tenant is not served anywhere")));
    }

    [Test]
    public void A_tenant_with_no_residency_says_it_is_not_set()
    {
        UseTenancyAs(isOperator: false);
        Cluster.Tenants["acme"].Regions[0] = Cluster.Tenants["acme"].Regions[0] with { Status = TenantRegionLifecycleStatus.None };

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");

        cut.WaitUntil(() =>
        {
            Assert.That(Facts(cut)["Resident in"], Is.EqualTo(TenancyFormat.NoResidency));
            Assert.That(cut.FindAll(".lt-tenancy-warning"), Is.Empty, "a tenant with no residency is served everywhere");
        });
    }

    [Test]
    [TestCase("t/acme/tenancy/unknown")]
    [TestCase("t/acme/tenancy/members/extra")]
    public void An_unknown_section_is_not_found(string address)
    {
        UseTenancyAs(isOperator: false);
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt<MyTenantPage>(address);

        Assert.That(notFound, Is.EqualTo(1));
    }

    [Test]
    public void A_failed_read_offers_a_retry_and_a_refusal_does_not()
    {
        UseTenancyAs(isOperator: false);
        Cluster.Fail(nameof(FakeTenancyCluster.GetTenantAsync), new TimeoutException());
        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("This tenant could not be read")));

        Cluster.Fail(nameof(FakeTenancyCluster.GetTenantAsync), FakeTenancyCluster.Denied());
        TenancyForms.Button(cut, "Try again").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not permitted"));
            Assert.That(TenancyForms.HasButton(cut, "Try again"), Is.False);
        });
    }

    [Test]
    public void While_the_standing_is_proven_the_page_shows_a_skeleton()
    {
        UseTenancyAs(isOperator: false);
        var hold = Cluster.Hold(nameof(FakeTenancyCluster.GetCurrentTenantAsync));

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
        cut.InvokeAsync(hold.SetResult);
        cut.WaitUntil(() => Assert.That(Facts(cut)["State"], Is.EqualTo("Active")));
    }

    [Test]
    public void Below_the_small_breakpoint_a_sections_table_is_a_list_of_rows()
    {
        UseTenancyAs(isOperator: false);

        var cut = RenderAt<MyTenantPage>("t/acme/tenancy/members", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-compact-row__primary").TextContent.Trim(), Is.EqualTo(FakeTenancyCluster.Caller)));
    }

    private static Dictionary<string, string> Facts(IRenderedComponent<MyTenantPage> cut) =>
        cut.FindAll(".lt-dl__row").ToDictionary(
            row => row.QuerySelector("dt")!.TextContent.Trim(),
            row => row.QuerySelector("dd")!.TextContent.Trim());
}
