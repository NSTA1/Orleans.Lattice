using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Suggestions;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// Issue #3949: every Tenancy field that names a tenant, a principal or a region
/// is a type-ahead picker. A new tenant id is refused when it is taken; admin
/// subjects and members come from the directory; the allowed regions are chips
/// the cluster knows; a grant's grantee is picked by an operator.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyPickerFieldsTests : TenancyTestContext
{
    [Test]
    public void A_new_tenant_id_that_is_taken_is_flagged_as_it_is_typed()
    {
        Cluster.WithTenant("globex");
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyDirectoryPage>("tenancy?new=true");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));

        Assert.That(SuggestionFields.Offers(cut, "Tenant id", "glob"), Is.EqualTo(new[] { "globex" }));

        SuggestionFields.Box(cut, "Tenant id").Input("globex");
        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Tenant id"), Is.EqualTo("A tenant named globex already exists.")));
    }

    [Test]
    public void Admin_subjects_are_chips_chosen_from_the_directory()
    {
        Directory.WithPrincipal("alice@example.com", "Alice", DirectoryPrincipalKind.User)
            .WithPrincipal("ops", "Operations", DirectoryPrincipalKind.Group);
        UseTenancyAs(isOperator: true);
        var cut = RenderAt<TenancyDirectoryPage>("tenancy?new=true");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));

        Assert.That(SuggestionFields.Offers(cut, "Admin subjects (optional)", "o", atLeast: 1), Does.Contain("ops"));
        cut.FindAll("[role=option]").Single(option => option.QuerySelector(".lt-combobox__value")!.TextContent == "ops").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-combobox__chip-value").Select(chip => chip.TextContent), Is.EqualTo(new[] { "ops" })));

        SuggestionFields.Box(cut, "Admin subjects (optional)").Input("mallory,");
        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Admin subjects (optional)"), Is.EqualTo("No subject is named mallory. Choose one from the list.")));
    }

    [Test]
    public void A_member_the_directory_does_not_list_is_refused()
    {
        Directory.WithPrincipal("bob@example.com", "Bob", DirectoryPrincipalKind.User);
        var cut = RenderSection<TenancyMembers>(parameters => parameters.Add(members => members.TenantId, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        Assert.That(SuggestionFields.Offers(cut, "Subject id", "bob"), Is.EqualTo(new[] { "bob@example.com" }));

        SuggestionFields.Box(cut, "Subject id").Input("eve@example.com");
        cut.Find("form.lt-tenancy-add").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Subject id"), Is.EqualTo("No subject is named eve@example.com. Choose one from the list.")));
        Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.AddAdminSubjectAsync)));
    }

    [Test]
    public void The_allowed_regions_offer_the_clusters_regions_and_refuse_an_unknown_one()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderSection<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, "acme").Add(regions => regions.CanAuthorize, true));
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));

        Assert.That(SuggestionFields.Offers(cut, "Allowed region ids", "a"), Does.Contain("ap-south"));

        SuggestionFields.Box(cut, "Allowed region ids").Input("mars-1,");

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "Allowed region ids"), Is.EqualTo("No region is named mars-1. Choose one from the list.")));
    }

    [Test]
    public void An_allowed_region_is_added_by_the_keyboard_alone()
    {
        UseTenancyAs(isOperator: true);
        var cut = RenderSection<TenancyRegions>(parameters => parameters.Add(regions => regions.TenantId, "acme").Add(regions => regions.CanAuthorize, true));
        cut.WaitUntil(() => TenancyForms.Field(cut, "Allowed region ids"));
        var key = (string name) => SuggestionFields.Box(cut, "Allowed region ids").KeyDown(new Microsoft.AspNetCore.Components.Web.KeyboardEventArgs { Key = name });

        SuggestionFields.Box(cut, "Allowed region ids").Input("sa");
        cut.WaitUntil(() => Assert.That(SuggestionFields.OfferedValues(cut, "Allowed region ids"), Is.EqualTo(new[] { "sa-east" })));
        key("ArrowDown");
        cut.WaitUntil(() => Assert.That(SuggestionFields.Box(cut, "Allowed region ids").GetAttribute("aria-activedescendant"), Is.Not.Null));
        key("Enter");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-combobox__chip-value").Select(chip => chip.TextContent), Does.Contain("sa-east")));
    }
    [Test]
    public void An_operator_picks_the_grantee_from_the_tenants_and_an_unknown_one_is_refused()
    {
        Cluster.WithTenant("globex");
        UseTenancyAs(isOperator: true);
        Services.GetRequiredService<TenancyCatalog>().GetStandingAsync(CancellationToken.None).GetAwaiter().GetResult();
        var cut = RenderSection<TenancyGrants>(parameters => parameters.Add(grants => grants.TenantId, "acme").Add(grants => grants.OpenOfferOnLoad, true));
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));

        Assert.That(SuggestionFields.Offers(cut, "To tenant", "glo"), Is.EqualTo(new[] { "globex" }));

        SuggestionFields.Box(cut, "To tenant").Input("initech");
        TenancyForms.Type(cut, "Scope", "orders/");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() => Assert.That(SuggestionFields.ErrorOf(cut, "To tenant"), Is.EqualTo("No tenant is named initech. Choose one from the list.")));
        Assert.That(Cluster.GrantList, Is.Empty);
    }

    [Test]
    public void The_tenancy_region_source_adds_the_tenants_own_regions_only_when_the_cluster_lists()
    {
        var cluster = new UnavailableRegions();
        var source = new TenancyRegionSuggestionSource(cluster, () => ["eu-west"]);

        var answer = source.SuggestAsync(string.Empty, 5, CancellationToken.None).AsTask().GetAwaiter().GetResult();

        Assert.That(answer.IsAvailable, Is.False, "the tenant's own list alone would refuse real regions");
    }

    /// <summary>A cluster region source that cannot list.</summary>
    private sealed class UnavailableRegions : ILtSuggestionSource
    {
        public ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken) =>
            ValueTask.FromResult(LtSuggestionSet.Unavailable("No replication report."));
    }
}
