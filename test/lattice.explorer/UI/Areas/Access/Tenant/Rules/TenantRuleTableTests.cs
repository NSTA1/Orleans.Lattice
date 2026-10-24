using Bunit;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>The tenant rule table's compact rows and editable versus platform detail actions.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantRuleTableTests : TenantRulesTestContext
{
    [Test]
    public void Compact_row_summarizes_effect_subject_scope_and_operations()
    {
        SeedTenantRule("eng-orders", "orders", "eng", operations: LatticeOperation.Read | LatticeOperation.Write);

        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            var row = cut.Find("[data-lt-rule-layer=tenant] .lt-table-list__row");
            Assert.That(row.QuerySelector(".lt-compact-row__primary")!.TextContent, Is.EqualTo("eng-orders"));
            Assert.That(row.QuerySelector(".lt-compact-row__secondary")!.TextContent.Trim(),
                Is.EqualTo("Allow tenant-group:eng - orders - Read, Write"));
        });
    }

    [Test]
    public void Compact_tenant_detail_opens_the_selected_rules_tenant_rooted_link()
    {
        SeedTenantRule("first", "orders", "eng");
        SeedTenantRule("second", "billing", "ops");
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-rule-layer=tenant] .lt-table-list__open"), Has.Count.EqualTo(2)));

        cut.FindAll("[data-lt-rule-layer=tenant] .lt-table-list__open")
            .Single(button => button.QuerySelector(".lt-compact-row__primary")!.TextContent == "second").Click();

        var dialog = cut.Find("[data-lt-rule-layer=tenant] [role=dialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector("h2")!.TextContent, Is.EqualTo("second"));
            var link = dialog.QuerySelector(".lt-dialog__actions a");
            Assert.That(link, Is.Not.Null);
            Assert.That(link!.TextContent, Is.EqualTo("Open rule"));
            Assert.That(link.GetAttribute("href"), Is.EqualTo("t/acme/access/rules/second"));
            Assert.That(dialog.QuerySelectorAll(".lt-dl__value").Select(value => value.TextContent),
                Does.Contain("billing").And.Contain("tenant-group:ops"));
        });
    }

    [Test]
    public void Compact_platform_detail_has_no_edit_link_and_preserves_the_denial_summary()
    {
        SeedPlatformRule("guard", "orders", "eng", effect: LatticeEffect.Deny, scope: TenantRuleScopeKind.Prefix, keyOrPrefix: "private/");
        var cut = RenderAt<AccessRulesPage>("t/acme/access/rules", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-rule-layer=platform] .lt-table-list__open"), Has.Count.EqualTo(1)));

        var row = cut.Find("[data-lt-rule-layer=platform] .lt-compact-row__secondary");
        Assert.That(row.TextContent.Trim(), Is.EqualTo("Deny tenant-group:eng - orders prefix private/ - Read"));
        cut.Find("[data-lt-rule-layer=platform] .lt-table-list__open").Click();

        var dialog = cut.Find("[data-lt-rule-layer=platform] [role=dialog]");
        Assert.Multiple(() =>
        {
            Assert.That(dialog.QuerySelector("h2")!.TextContent, Is.EqualTo("guard"));
            Assert.That(dialog.QuerySelectorAll("a"), Is.Empty);
            Assert.That(dialog.TextContent, Does.Contain("Deny"));
        });
    }
}
