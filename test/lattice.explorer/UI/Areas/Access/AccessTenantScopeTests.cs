using Bunit;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// Issue #4025: a tenant-rooted Access address lists only that tenant's items.
/// Two tenants, the default tenant, a cluster-wide <c>Tree:*</c> rule and a
/// platform rule are seeded together, so every assertion that one of them is
/// left out is paired with one that another is kept.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessTenantScopeTests : AccessTestContext
{
    private static LatticeAuthorizationRule Wide(string id) =>
        new(id, LatticeSubjectSelector.Group("ops"), LatticeScope.ClusterWide(), LatticeOperation.Read, LatticeEffect.Allow);

    private void SeedEstate() => Admin
        .WithRule(Wide("everyone-reads"))
        .WithRule(Rule("platform-read", tree: "_lattice_auth_policy"))
        .WithRule(Rule("default-orders", tree: "orders"))
        .WithRule(Rule("acme-orders", tree: "t/acme/orders"))
        .WithRule(Rule("acme-stock", tree: "t/acme/stock"))
        .WithRule(Rule("globex-orders", tree: "t/globex/orders"))
        .WithGroup("ops", "Operators");

    private static string[] Listed(IRenderedComponent<AccessRulesPage> cut) =>
        [.. cut.FindAll("tbody th a").Select(link => link.TextContent)];

    [Test]
    public void At_a_tenant_address_only_that_tenants_rules_are_listed()
    {
        SeedEstate();

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(Listed(cut), Is.EquivalentTo(new[] { "acme-orders", "acme-stock" }));
            Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("2 rules of tenant acme"));
        });
    }

    [Test]
    public void At_a_tenant_address_the_cluster_wide_rules_are_one_quiet_line_that_leads_to_them()
    {
        SeedEstate();

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            var line = cut.Find("[data-lt-cluster-wide-rules]");
            Assert.That(line.TextContent, Does.StartWith("1 cluster-wide rule also applies."));
            Assert.That(line.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("access/rules"));
            Assert.That(Listed(cut), Does.Not.Contain("everyone-reads"));
        });
    }

    [Test]
    public void At_the_default_tenants_address_no_other_tenants_rule_is_listed()
    {
        SeedEstate();

        var cut = RenderAt<AccessRulesPage>("t/default/access/rules");

        cut.WaitUntil(() => Assert.That(Listed(cut), Is.EqualTo(new[] { "default-orders" })));
    }

    [Test]
    public void The_cluster_wide_address_still_lists_every_rule()
    {
        SeedEstate();

        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(Listed(cut), Has.Length.EqualTo(6));
            Assert.That(cut.FindAll("[data-lt-cluster-wide-rules]"), Is.Empty);
            Assert.That(Admin.RuleRequests.All(request => !request.ActiveTenantOnly), Is.True, "the cluster-wide listing is not narrowed");
        });
    }

    [Test]
    public void A_tenant_listing_asks_the_cluster_to_narrow_it()
    {
        SeedEstate();
        Admin.NarrowsTo = "acme";

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(Listed(cut), Is.EquivalentTo(new[] { "acme-orders", "acme-stock" }));
            Assert.That(Admin.RuleRequests, Has.All.Matches<Orleans.Lattice.Api.Auth.AuthPageRequest>(request => request.ActiveTenantOnly));
            Assert.That(Admin.RuleRequests, Has.Count.EqualTo(1), "a narrowed first page is full, so one read fills it");
        });
    }

    [Test]
    public void Against_a_cluster_that_does_not_narrow_the_page_is_read_on_until_it_holds_the_tenants_rules()
    {
        SeedEstate();
        Admin.ForcedPageSize = 1;

        var cut = RenderAt<AccessRulesPage>("t/globex/access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(Listed(cut), Is.EqualTo(new[] { "globex-orders" }));
            Assert.That(Admin.RuleRequests, Has.Count.GreaterThan(1));
        });
    }

    [Test]
    public void A_tenant_address_for_another_tenants_rule_is_not_found_and_never_read()
    {
        SeedEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt<AccessRulePage>("t/acme/access/rules/globex-orders?tree=t/globex/orders");

        Assert.Multiple(() =>
        {
            Assert.That(notFound, Is.EqualTo(1));
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.GetRuleAsync)));
        });
    }

    [Test]
    public void A_tenant_address_for_a_cluster_wide_rule_by_id_alone_is_not_found()
    {
        SeedEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/everyone-reads");

        cut.WaitUntil(() => Assert.That(notFound, Is.EqualTo(1)));
    }

    [Test]
    public void A_tenant_address_for_its_own_rule_renders_it()
    {
        SeedEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/acme-orders?tree=t/acme/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("acme-orders"));
            Assert.That(notFound, Is.Zero);
        });
    }

    [Test]
    public void At_a_tenant_address_groups_are_not_listed_and_the_line_leads_to_the_clusters_groups()
    {
        SeedEstate();

        var cut = RenderAt<AccessGroupsPage>("t/acme/access/groups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-cluster-wide-groups] a").GetAttribute("href"), Is.EqualTo("access/groups"));
            Assert.That(cut.FindAll("tbody tr"), Is.Empty);
            Assert.That(Admin.Calls, Does.Not.Contain(nameof(FakeAuthAdmin.ListGroupsAsync)));
        });
    }

    [Test]
    public void A_tenant_address_for_a_group_is_not_found()
    {
        SeedEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt<AccessGroupPage>("t/acme/access/groups/ops");

        cut.WaitUntil(() => Assert.That(notFound, Is.EqualTo(1)));
    }

    [Test]
    public async Task Completions_from_a_tenant_address_offer_only_its_rules_rooted_at_it_and_no_group()
    {
        SeedEstate();
        var source = new AccessCompletionSource(new AccessCatalog(Admin));
        var current = ExplorerAddress.ForArea("data").WithTenant("acme");

        var completions = await source.CompleteAsync(new AddressQuery("o", AddressQueryMode.Search, current), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Is.EquivalentTo(new[] { "rule:acme-orders", "rule:acme-stock" }));
            Assert.That(completions.Select(completion => completion.Target.Tenant), Has.All.EqualTo("acme"));
        });
    }

    [Test]
    public async Task Completions_from_the_default_tenant_offer_no_other_tenants_rule()
    {
        SeedEstate();
        var source = new AccessCompletionSource(new AccessCatalog(Admin));
        var current = ExplorerAddress.Home.WithTenant("default");

        var completions = await source.CompleteAsync(new AddressQuery("rule:", AddressQueryMode.Search, current), CancellationToken.None);

        Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "rule:default-orders" }));
    }

    [Test]
    public async Task Completions_from_a_cluster_wide_address_are_unchanged()
    {
        SeedEstate();
        var source = new AccessCompletionSource(new AccessCatalog(Admin));

        var completions = await source.CompleteAsync(new AddressQuery("o", AddressQueryMode.Search, ExplorerAddress.Home), CancellationToken.None);

        Assert.That(completions.Select(completion => completion.Label), Does.Contain("group:ops").And.Contain("rule:globex-orders"));
    }

    [Test]
    public void The_area_follows_the_tenant_only_at_a_tenant_rooted_address()
    {
        var area = new AccessArea(Services);

        Assert.Multiple(() =>
        {
            Assert.That(area.IsTenantScopedAt(AccessRoutes.Rules.WithTenant("acme")), Is.True);
            Assert.That(area.IsTenantScopedAt(AccessRoutes.Rules), Is.False);
        });
    }

    [TestCase("t/acme/orders", "acme", true)]
    [TestCase("t/globex/orders", "acme", false)]
    [TestCase("orders", "default", true)]
    [TestCase("t/acme/orders", "default", false)]
    [TestCase("*", "default", false)]
    [TestCase("_lattice_auth_policy", "default", false)]
    [TestCase("t/acme/orders", null, true)]
    public void Lists_decides_by_the_tenancy_ownership_grammar(string treeId, string? scope, bool listed)
    {
        Assert.That(AccessCatalog.Lists(scope, treeId), Is.EqualTo(listed));
    }
}
