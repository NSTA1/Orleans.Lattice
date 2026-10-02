using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// Issue #4163: the tenant Rules list - the Platform rules on the tenant's trees
/// read-only under a lock and decided first, the tenant's own rules editable,
/// both filtered by tree and by subject, with the tenant's rule cap and usage.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantRulesViewTests : TenantRulesTestContext
{
    [Test]
    public void Platform_rows_are_read_only_under_a_lock_and_decided_first()
    {
        SeedPlatformRule("guard", "orders", "eng");
        SeedTenantRule("eng-orders", "orders", "eng");

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            var platform = cut.Find("[data-lt-rule-layer=platform]");
            Assert.That(platform.QuerySelector("h2 .lt-access-lock"), Is.Not.Null, "the Platform group carries the lock glyph");
            Assert.That(platform.QuerySelector("h2")!.TextContent.Trim(), Is.EqualTo("Platform"));
            Assert.That(platform.TextContent, Does.Contain("decided first; your rules apply only where none of these match"));
            Assert.That(platform.QuerySelectorAll("tbody a"), Is.Empty, "a platform rule links nowhere");
            Assert.That(platform.QuerySelector("tbody")!.TextContent, Does.Contain("guard"));
            Assert.That(platform.QuerySelectorAll("button").Select(button => button.TextContent.Trim()), Has.None.Contains("Edit").And.None.Contains("Delete"));
        });
    }

    [Test]
    public void Tenant_rows_link_to_their_own_pages_and_platform_rules_come_first()
    {
        SeedPlatformRule("guard", "orders", "eng");
        SeedTenantRule("eng-orders", "orders", "eng");

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            var layers = cut.FindAll("[data-lt-rule-layer]").Select(group => group.GetAttribute("data-lt-rule-layer"));
            Assert.That(layers, Is.EqualTo(new[] { "platform", "tenant" }));
            var link = cut.Find("[data-lt-rule-layer=tenant] tbody a");
            Assert.That(link.TextContent, Is.EqualTo("eng-orders"));
            Assert.That(link.GetAttribute("href"), Does.EndWith("t/acme/access/rules/eng-orders"));
            Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("2 rules of tenant acme"));
        });
    }

    [Test]
    public void The_list_filters_by_tree_keeping_every_tree_rules_with_each_tree()
    {
        SeedTenantRule("orders-rule", "orders", "eng");
        SeedTenantRule("billing-rule", "billing", "eng");
        SeedTenantRule("everywhere", null, "eng", scope: TenantRuleScopeKind.TenantWide);
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");
        cut.WaitUntil(() => Assert.That(TenantRows(cut), Has.Length.EqualTo(3)));

        AccessForms.Choose(cut, "Filter by tree", "orders");
        Assert.That(TenantRows(cut), Is.EquivalentTo(new[] { "everywhere", "orders-rule" }));

        AccessForms.Choose(cut, "Filter by tree", "*");
        Assert.That(TenantRows(cut), Is.EqualTo(new[] { "everywhere" }));
        Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("1 of 3 rules of tenant acme"));
    }

    [Test]
    public void The_list_filters_by_subject()
    {
        SeedTenantRule("eng-rule", "orders", "eng");
        SeedTenantRule("ops-rule", "orders", "ops", kind: TenantSubjectKind.ClusterGroup);
        SeedPlatformRule("ops-guard", "orders", "ops", kind: TenantSubjectKind.ClusterGroup);
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");
        cut.WaitUntil(() => Assert.That(TenantRows(cut), Has.Length.EqualTo(2)));

        AccessForms.Type(cut, "Filter by subject", "OPS");

        Assert.Multiple(() =>
        {
            Assert.That(TenantRows(cut), Is.EqualTo(new[] { "ops-rule" }));
            Assert.That(cut.Find("[data-lt-rule-layer=platform] tbody").TextContent, Does.Contain("ops-guard"));
        });

        AccessForms.Type(cut, "Filter by subject", "nobody");
        Assert.That(cut.Find("[data-lt-rule-layer=tenant]").TextContent, Does.Contain("No rule of the tenant's matches."));
    }

    [Test]
    public void The_cap_and_its_usage_are_shown_and_reaching_it_is_said()
    {
        SeedTenantRule("one", "orders", "eng");
        TenantFacades.PolicyFake.MaxTenantRules = 2;
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            var cap = cut.Find("[data-lt-rule-cap]");
            Assert.That(cap.GetAttribute("data-lt-rule-cap"), Is.EqualTo("within"));
            Assert.That(cap.TextContent, Is.EqualTo("1 of 2 tenant rules used."));
        });
    }

    [Test]
    public void A_tenant_at_its_cap_is_told_to_remove_a_rule_first()
    {
        SeedTenantRule("one", "orders", "eng");
        TenantFacades.PolicyFake.MaxTenantRules = 1;

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            var cap = cut.Find("[data-lt-rule-cap]");
            Assert.That(cap.GetAttribute("data-lt-rule-cap"), Is.EqualTo("reached"));
            Assert.That(cap.TextContent, Does.Contain("Tenant acme is at its cap"));
        });
    }

    [Test]
    public void New_rule_opens_the_tenant_editor_and_a_saved_rule_joins_the_tenant_group()
    {
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-tenant-view=rules]").TextContent, Does.Contain("No rules govern tenant acme's trees yet")));

        AccessForms.Button(cut, "New rule").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-tenant-rule-editor=acme]"), Has.Count.EqualTo(1)));
        AccessForms.Type(cut, "Rule id", "eng-orders");
        AccessForms.Type(cut, "Subject", "eng");
        AccessForms.Type(cut, "Tree", "orders");
        cut.Find("form[data-lt-tenant-rule-editor]").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[data-lt-tenant-rule-editor]"), Is.Empty, "the editor closes");
            Assert.That(TenantRows(cut), Is.EqualTo(new[] { "eng-orders" }));
            Assert.That(cut.Find("[data-lt-rule-cap]").TextContent, Is.EqualTo("1 of 1000 tenant rules used."), "the usage is read again");
        });
    }

    [Test]
    public void The_new_query_opens_the_editor_at_once()
    {
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules?new=true");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-tenant-rule-editor=acme]"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void More_rules_are_loaded_on_request()
    {
        for (var i = 0; i < 205; i++)
        {
            SeedTenantRule($"rule-{i:D3}", "orders", "eng");
        }

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("200 rules of tenant acme, more to load")));

        AccessForms.Button(cut, "Load more rules").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("205 rules of tenant acme"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Load more rules"));
        });
    }

    [Test]
    public void A_refused_listing_shows_its_error_and_no_toolbar()
    {
        Script().Faults[nameof(ILatticeTenantPolicyAdmin.ListRulesAsync)] = new LatticeAuthorizationDeniedException("denied");

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-tenant-view=rules]").TextContent, Does.Contain("Not permitted"));
            Assert.That(cut.Find("[data-lt-tenant-view=rules]").TextContent, Does.Contain(TenantAccessFailure.DeniedMessage(Acme)));
            Assert.That(cut.FindAll(".lt-toolbar"), Is.Empty);
        });
    }

    private static string[] TenantRows<TComponent>(IRenderedComponent<TComponent> cut)
        where TComponent : Microsoft.AspNetCore.Components.IComponent =>
        [.. cut.FindAll("[data-lt-rule-layer=tenant] tbody tr th").Select(cell => cell.TextContent.Trim())];
}
