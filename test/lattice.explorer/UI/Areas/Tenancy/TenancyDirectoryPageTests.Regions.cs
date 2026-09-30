using AngleSharp.Dom;
using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The directory's regions: residency linking to each tenant's regions, the
/// set-regions picker behind the palette command, and a new tenant created with
/// its allowed regions and an initial residency, confirmed because it starts with
/// no Online region, each step reporting its own outcome.
/// </summary>
public sealed partial class TenancyDirectoryPageTests
{
    private const string AllowedLabel = "Allowed regions (optional)";
    private const string ResidencyLabel = "Initial residency (optional)";

    [Test]
    public void A_tenant_with_no_residency_reads_not_set_and_still_links_to_its_regions()
    {
        UseTenancyAs(isOperator: true);
        Cluster.Tenants["acme"].Regions.Clear();

        var cut = RenderAt<TenancyDirectoryPage>("tenancy");

        cut.WaitUntil(() =>
        {
            var link = cut.Find("tbody tr td a[href='tenancy/acme/regions']");
            Assert.That(link.TextContent.Trim(), Is.EqualTo(TenancyFormat.NoResidency));
            Assert.That(link.GetAttribute("aria-label"), Is.EqualTo("Not set: no residency is set; open the regions of tenant acme"),
                "the accessible name starts with the visible text (WCAG 2.5.3, axe label-content-name-mismatch)");
        });
    }

    [Test]
    public async Task The_set_regions_command_has_a_visible_control_and_its_picker_opens_a_tenants_regions()
    {
        UseTenancyAs(isOperator: true);
        var area = CreateArea();
        await area.GetAvailabilityAsync(CancellationToken.None);
        var command = area.Commands.Single(candidate => candidate.Id == TenancyArea.SetRegionsCommandId);

        var cut = RenderAt<TenancyDirectoryPage>(command.Target!.ToHref());

        cut.WaitUntil(() =>
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("Set a tenant's regions"));
            Assert.That(TenancyForms.Field(cut, "Tenant").GetAttribute("value"), Is.EqualTo("acme"), "the scoped tenant is offered first");
        });

        TenancyForms.Field(cut, "Tenant").Closest("form")!.Submit();

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/tenancy/acme/regions")));
    }

    [Test]
    public void The_set_regions_picker_asks_for_a_tenant_before_it_moves()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyDirectoryPage>("tenancy");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Set regions..."));

        TenancyForms.Button(cut, "Set regions...").Click();
        TenancyForms.Type(cut, "Tenant", string.Empty);
        TenancyForms.Field(cut, "Tenant").Closest("form")!.Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(TenancyForms.ErrorOf(cut, "Tenant"), Is.EqualTo("Choose a tenant."));
            Assert.That(Navigation.Uri, Does.EndWith("/tenancy"));
        });
    }

    [Test]
    public void A_tenant_created_with_regions_and_a_residency_is_confirmed_then_each_step_reports_its_outcome()
    {
        Cluster.CreatesTenantsWithoutRegions = true;
        var cut = OpenCreate();

        FillCreate(cut, "globex", allowed: ["eu-west", "us-east"], residency: ["us-east"]);
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=alertdialog] .lt-dialog__title").TextContent, Is.EqualTo("Create tenant globex with no Online region?"));
            Assert.That(cut.Find("[role=alertdialog] .lt-tenancy-consequence").TextContent, Does.Contain("is not served anywhere until then"));
            Assert.That(cut.FindAll("form.lt-tenancy-form"), Is.Empty, "the form gives way to the confirmation");
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.CreateTenantAsync)));
        });

        TenancyForms.Button(cut, "Create tenant").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Calls.Where(call => call is nameof(FakeTenancyCluster.CreateTenantAsync) or nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync) or nameof(FakeTenancyCluster.SetResidencyAsync)),
                Is.EqualTo(new[] { nameof(FakeTenancyCluster.CreateTenantAsync), nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync), nameof(FakeTenancyCluster.SetResidencyAsync) }));
            var regions = Cluster.Tenants["globex"].Regions;
            Assert.That(regions.Where(region => region.IsAllowed).Select(region => region.RegionId), Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(regions.Single(region => region.RegionId == "us-east").Status, Is.EqualTo(TenantRegionLifecycleStatus.Provisioning));
            Assert.That(Services.GetToasts().Select(toast => toast.Message), Is.EqualTo(new[]
            {
                "Tenant globex created, administered by " + FakeTenancyCluster.Caller + ".",
                "Tenant globex is allowed eu-west, us-east.",
                "Tenant globex is adding us-east. " + TenancyRegions.NotServedYet,
            }));
            Assert.That(Navigation.Uri, Does.EndWith("/tenancy/globex/regions"));
        });
    }

    [Test]
    public void Allowed_regions_alone_are_applied_without_a_confirmation()
    {
        Cluster.CreatesTenantsWithoutRegions = true;
        var cut = OpenCreate();

        FillCreate(cut, "globex", allowed: ["us-east"], residency: []);
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(Cluster.Tenants["globex"].Regions.Single().RegionId, Is.EqualTo("us-east"));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetResidencyAsync)));
            Assert.That(Services.GetToasts().Last().Message, Is.EqualTo("Tenant globex is allowed us-east."));
            Assert.That(Navigation.Uri, Does.EndWith("/tenancy/globex/regions"));
        });
    }

    [Test]
    public void Back_from_the_confirmation_returns_to_the_filled_form_and_writes_nothing()
    {
        var cut = OpenCreate();
        FillCreate(cut, "globex", allowed: ["us-east"], residency: ["us-east"]);
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => cut.Find("[role=alertdialog]"));

        TenancyForms.Button(cut, "Back").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(TenancyForms.Field(cut, "Tenant id").GetAttribute("value"), Is.EqualTo("globex"));
            Assert.That(ChipsOf(cut, AllowedLabel), Is.EqualTo(new[] { "us-east" }));
            Assert.That(ChipsOf(cut, ResidencyLabel), Is.EqualTo(new[] { "us-east" }));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.CreateTenantAsync)));
        });
    }

    [Test]
    public void The_residency_waits_for_allowed_regions_and_follows_them()
    {
        var cut = OpenCreate();
        Assert.That(TenancyForms.Field(cut, ResidencyLabel).HasAttribute("disabled"), Is.True, "residency is chosen from the allowed regions");

        FillCreate(cut, "globex", allowed: ["us-east", "eu-west"], residency: ["us-east"]);
        Assert.That(TenancyForms.Field(cut, ResidencyLabel).HasAttribute("disabled"), Is.False);

        TenancyForms.Field(cut, AllowedLabel).Closest(".lt-field")!.QuerySelector("button[aria-label='Remove us-east']")!.Click();

        cut.WaitUntil(() => Assert.That(ChipsOf(cut, ResidencyLabel), Is.Empty, "a region no longer allowed leaves the residency"));
    }

    [Test]
    public void A_failed_region_step_reports_its_own_outcome_and_the_residency_is_not_attempted()
    {
        Cluster.CreatesTenantsWithoutRegions = true;
        Cluster.Fail(nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync), FakeTenancyCluster.Denied());
        var cut = OpenCreate();
        FillCreate(cut, "globex", allowed: ["us-east"], residency: ["us-east"]);
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => cut.Find("[role=alertdialog]"));

        TenancyForms.Button(cut, "Create tenant").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Cluster.Tenants.ContainsKey("globex"), Is.True, "the tenant itself was created");
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.SetResidencyAsync)));
            Assert.That(Services.GetToasts().Skip(1).Select(toast => toast.Message), Is.EqualTo(new[]
            {
                "Tenant globex was created, but its allowed regions were not set: " + TenancyFailure.NotPermittedMessage,
                "The residency of tenant globex was not set, because its allowed regions were not.",
            }));
        });
    }

    [Test]
    public void A_refused_residency_after_creation_is_reported_on_its_own()
    {
        Cluster.CreatesTenantsWithoutRegions = true;
        Cluster.Fail(nameof(FakeTenancyCluster.SetResidencyAsync), new TenantRegionNotAllowedException("globex", "us-east"));
        var cut = OpenCreate();
        FillCreate(cut, "globex", allowed: ["us-east"], residency: ["us-east"]);
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => cut.Find("[role=alertdialog]"));

        TenancyForms.Button(cut, "Create tenant").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Services.GetToasts()[1].Message, Is.EqualTo("Tenant globex is allowed us-east."));
            Assert.That(Services.GetToasts()[2].Message, Does.StartWith("Tenant globex was created, but its residency was not set: "));
        });
    }

    [Test]
    public void A_refused_creation_from_the_confirmation_returns_to_the_form_with_the_reason()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.CreateTenantAsync), new TenantAlreadyExistsException("globex"));
        var cut = OpenCreate();
        FillCreate(cut, "globex", allowed: ["us-east"], residency: ["us-east"]);
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => cut.Find("[role=alertdialog]"));

        TenancyForms.Button(cut, "Create tenant").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
            Assert.That(TenancyForms.ErrorOf(cut, "Tenant id"), Is.EqualTo("A tenant with the id globex already exists."));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.AuthorizeAllowedRegionsAsync)));
        });
    }

    private void FillCreate(IRenderedComponent<TenancyDirectoryPage> cut, string id, string[] allowed, string[] residency)
    {
        TenancyForms.Type(cut, "Tenant id", id);
        if (allowed.Length > 0)
        {
            TenancyForms.Type(cut, AllowedLabel, string.Join(",", allowed) + ",");
            cut.WaitUntil(() => Assert.That(ChipsOf(cut, AllowedLabel), Is.EqualTo(allowed)));
        }

        if (residency.Length > 0)
        {
            TenancyForms.Type(cut, ResidencyLabel, string.Join(",", residency) + ",");
            cut.WaitUntil(() => Assert.That(ChipsOf(cut, ResidencyLabel), Is.EqualTo(residency)));
        }
    }

    private static string[] ChipsOf(IRenderedComponent<TenancyDirectoryPage> cut, string label) =>
        [.. TenancyForms.Field(cut, label).Closest(".lt-field")!.QuerySelectorAll(".lt-combobox__chip-value").Select(chip => chip.TextContent)];
}
