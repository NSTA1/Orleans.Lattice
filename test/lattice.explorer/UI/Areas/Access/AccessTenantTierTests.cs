using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// Issues #4163 and #4167: the cluster Access pages read the tenant tier the
/// cluster auth facade now reports - a tenant-tier rule on the Rules list is
/// attributed to its tenant (<c>AuthRulePage.TenantRuleTenants</c>), and Explain
/// says which layer and rule decided and marks that rule
/// (<c>AuthExplanation.DecidingLayer</c> and <c>DecidingRuleId</c>).
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessTenantTierTests : AccessTestContext
{
    private static readonly LatticeAuthorizationRule TenantRule =
        new("tenant:acme:eng-orders", LatticeSubjectSelector.Group("t/acme/eng"), LatticeScope.Tree("t/acme/orders"), LatticeOperation.Read, LatticeEffect.Allow);

    private static readonly LatticeAuthorizationRule ForeignTenantRule =
        new("tenant:globex:ledger", LatticeSubjectSelector.Group("t/globex/fin"), LatticeScope.Tree("t/globex/ledger"), LatticeOperation.Read, LatticeEffect.Allow);

    [Test]
    public void The_cluster_rules_list_attributes_a_tenant_rule_to_its_tenant()
    {
        Admin.Rules.Add(Rule("readers"));
        Admin.Rules.Add(TenantRule);

        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() =>
        {
            var badge = cut.Find("[data-lt-tenant-rule]");
            Assert.That(badge.GetAttribute("data-lt-tenant-rule"), Is.EqualTo("acme"));
            Assert.That(badge.TextContent, Is.EqualTo("tenant acme"));
            Assert.That(badge.HasAttribute("href"), Is.False, "without tenancy there is no tenant address to link to");
            Assert.That(badge.Closest("tr")!.QuerySelector("th")!.TextContent, Is.EqualTo(TenantRule.RuleId));
            Assert.That(cut.FindAll("[data-lt-tenant-rule]"), Has.Count.EqualTo(1), "an authored rule is not badged");
        });
    }

    [Test]
    public void A_tenant_rooted_cluster_listing_keeps_each_badge_on_its_own_rule_after_filtering()
    {
        UseTenancy("acme");
        Admin.Rules.Add(ForeignTenantRule);
        Admin.Rules.Add(Rule("acme-readers", tree: "t/acme/billing"));
        Admin.Rules.Add(TenantRule);

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EquivalentTo(new[] { "acme-readers", TenantRule.RuleId }));
            var badge = cut.Find("[data-lt-tenant-rule]");
            Assert.That(badge.GetAttribute("data-lt-tenant-rule"), Is.EqualTo("acme"));
            Assert.That(badge.GetAttribute("href"), Does.EndWith("t/acme/access/rules"), "it links to the tenant's own rules");
            Assert.That(badge.Closest("tr")!.QuerySelector("th")!.TextContent, Is.EqualTo(TenantRule.RuleId));
        });
    }

    [Test]
    public async Task The_catalogue_reports_the_tenants_of_the_rules_it_keeps_index_aligned()
    {
        Admin.Rules.Add(ForeignTenantRule);
        Admin.Rules.Add(Rule("acme-readers", tree: "t/acme/billing"));
        Admin.Rules.Add(TenantRule);
        var catalog = new AccessCatalog(Admin);

        var scoped = await catalog.ListRulesAsync("acme", new AuthPageRequest());
        Admin.Rules.Remove(TenantRule);
        var none = await catalog.ListRulesAsync("acme", new AuthPageRequest());

        Assert.Multiple(() =>
        {
            Assert.That(scoped.Entries.Select(rule => rule.RuleId), Is.EqualTo(new[] { "acme-readers", TenantRule.RuleId }));
            Assert.That(scoped.TenantRuleTenants, Is.EqualTo(new[] { null, "acme" }));
            Assert.That(none.TenantRuleTenants, Is.Empty, "a page with no tenant-tier rule reports none");
        });
    }

    [Test]
    [TestCase(TenantRuleLayer.Platform, "guard", "platform", "Platform rule guard")]
    [TestCase(TenantRuleLayer.Tenant, "tenant:acme:eng-orders", "tenant", "Tenant rule tenant:acme:eng-orders, because no platform rule matched")]
    public void Explain_says_which_layer_and_rule_decided_and_marks_that_rule(TenantRuleLayer layer, string ruleId, string attribute, string sentence)
    {
        var guard = new LatticeAuthorizationRule("guard", LatticeSubjectSelector.User("alice"), LatticeScope.Tree("t/acme/orders"), LatticeOperation.Read, LatticeEffect.Deny);
        Admin.Explain = (subject, operation, scope, kind) => new AuthExplanation
        {
            SubjectId = subject,
            Operation = operation,
            Scope = scope,
            Allowed = layer == TenantRuleLayer.Tenant,
            MatchedRules = [guard, TenantRule],
            DecidingLayer = layer,
            DecidingRuleId = ruleId,
        };

        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&kind=user&operation=read&tree=t%2Facme%2Forders");

        cut.WaitUntil(() =>
        {
            var decidedBy = cut.Find("[data-lt-decided-by]");
            Assert.That(decidedBy.GetAttribute("data-lt-decided-by"), Is.EqualTo(attribute));
            Assert.That(decidedBy.TextContent, Is.EqualTo(sentence));
            Assert.That(cut.FindAll("tbody tr[aria-current=true] th").Select(cell => cell.TextContent), Is.EqualTo(new[] { ruleId }));
        });
    }

    [Test]
    public void An_explanation_with_no_deciding_layer_marks_nothing()
    {
        var cut = RenderAt<AccessExplainPage>("access/explain?subject=alice&kind=user&operation=read&tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-access-verdict__title").TextContent, Is.EqualTo("Denied"));
            Assert.That(cut.FindAll("[data-lt-decided-by]"), Is.Empty);
            Assert.That(cut.FindAll("tr[aria-current]"), Is.Empty);
        });
    }

    [Test]
    public void Decided_by_names_the_layer_and_rule_or_nothing()
    {
        static AuthExplanation With(TenantRuleLayer? layer, string? rule) => new()
        {
            SubjectId = "alice",
            Scope = LatticeScope.Tree("orders"),
            DecidingLayer = layer,
            DecidingRuleId = rule,
        };

        Assert.Multiple(() =>
        {
            Assert.That(AccessExplainPage.DecidedBy(With(TenantRuleLayer.Platform, "guard")), Is.EqualTo("Platform rule guard"));
            Assert.That(AccessExplainPage.DecidedBy(With(TenantRuleLayer.Tenant, "t1")), Does.StartWith("Tenant rule t1"));
            Assert.That(AccessExplainPage.DecidedBy(With(TenantRuleLayer.Platform, null)), Is.EqualTo("The platform layer"));
            Assert.That(AccessExplainPage.DecidedBy(With(TenantRuleLayer.Tenant, null)), Does.StartWith("The tenant layer"));
            Assert.That(AccessExplainPage.DecidedBy(With(null, null)), Is.Null);
            Assert.That(() => AccessExplainPage.DecidedBy(null!), Throws.ArgumentNullException);
        });
    }
}
