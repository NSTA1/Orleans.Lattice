using System.Text;
using Bunit;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// Issue #4163: one of the tenant's own rules - its fields, Edit and Delete, a
/// link to the Explain for its subject, and its history from the policy store
/// with the existing history reader (D19).
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenantRuleDetailViewTests : TenantRulesTestContext
{
    [Test]
    public void The_rule_is_shown_with_its_fields_actions_and_an_explain_for_its_subject()
    {
        SeedTenantRule("eng-orders", "orders", "eng", operations: LatticeOperation.Read | LatticeOperation.Write);

        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/eng-orders");

        cut.WaitUntil(() =>
        {
            var view = cut.Find("[data-lt-tenant-view=rule]");
            Assert.That(view.QuerySelector("[data-lt-rule-layer]")!.GetAttribute("data-lt-rule-layer"), Is.EqualTo("tenant"));
            Assert.That(view.TextContent, Does.Contain("tenant-group:eng"));
            Assert.That(view.TextContent, Does.Contain("Read, Write"));
            Assert.That(view.QuerySelector("a[href$='t/acme/access/groups/eng']"), Is.Not.Null, "a tenant group subject links to its page");
            Assert.That(AccessForms.Button(cut, "Edit rule"), Is.Not.Null);
            Assert.That(AccessForms.Button(cut, "Delete rule"), Is.Not.Null);
            var explain = cut.FindAll("a").Single(link => link.TextContent == "Explain for this subject").GetAttribute("href");
            Assert.That(explain, Does.Contain("t/acme/access/explain?").And.Contain("subject=eng").And.Contain("kind=tenant-group").And.Contain("tree=orders"));
        });
    }

    [Test]
    public void Editing_saves_through_the_tenant_policy_and_shows_the_stored_rule()
    {
        SeedTenantRule("eng-orders", "orders", "eng");
        TenantFacades.WithGroup(Acme, "eng");
        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/eng-orders");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Edit rule"), Is.Not.Null));

        AccessForms.Button(cut, "Edit rule").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form[data-lt-tenant-rule-editor]"), Has.Count.EqualTo(1)));
        cut.Find("[data-lt-operation=\"delete\"]").Change(true);
        cut.Find("form[data-lt-tenant-rule-editor]").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("form[data-lt-tenant-rule-editor]"), Is.Empty);
            Assert.That(cut.Find("[data-lt-tenant-view=rule]").TextContent, Does.Contain("Read, Delete"));
            Assert.That(TenantFacades.Gate.Calls, Does.Contain(nameof(ILatticeTenantPolicyAdmin.PutRuleAsync)));
        });
    }

    [Test]
    public async Task Deleting_removes_the_rule_and_returns_to_the_tenants_rules()
    {
        SeedTenantRule("eng-orders", "orders", "eng");
        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/eng-orders");
        cut.WaitUntil(() => Assert.That(AccessForms.Button(cut, "Delete rule"), Is.Not.Null));

        AccessForms.Button(cut, "Delete rule").Click();
        cut.Find(".lt-confirm input").Input("eng-orders");
        cut.Find(".lt-confirm").Submit();

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("t/acme/access/rules")));
        Assert.That(await TenantFacades.PolicyFake.GetRuleAsync(Acme, "eng-orders"), Is.Null);
    }

    [Test]
    public void The_rules_history_is_read_from_the_policy_store_newest_first()
    {
        var state = UseTrees("t/acme/orders");
        var rule = SeedTenantRule("eng-orders", "orders", "eng");
        var key = TenantRuleHistory.PolicyKey(Acme, rule)!;
        var start = new DateTimeOffset(2026, 9, 28, 14, 0, 0, TimeSpan.Zero);
        state.History[(LatticeAuthReservedTrees.PolicyTreeId, key)] =
        [
            Revision(key, start, HistoryRowKind.Set),
            Revision(key, start.AddMinutes(5), HistoryRowKind.Set),
        ];

        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/eng-orders");

        cut.WaitUntil(() =>
        {
            var history = cut.Find("[data-lt-rule-history]");
            Assert.That(history.GetAttribute("data-lt-rule-history"), Is.EqualTo("loaded"));
            var times = history.QuerySelectorAll(".lt-access-history__time").Select(time => time.TextContent).ToArray();
            Assert.That(times, Has.Length.EqualTo(2));
            Assert.That(times[0], Does.Contain("14:05"), "newest first");
            Assert.That(times[1], Does.Contain("14:00"));
            Assert.That(history.TextContent, Does.Contain("Saved"));
        });
    }

    [Test]
    public void The_policy_key_names_the_governed_tree_or_the_tenant_wide_sentinel_then_the_full_id()
    {
        var onTree = new TenantRuleView { RuleId = "r", ScopeKind = TenantRuleScopeKind.Key, TreeName = "orders", KeyOrPrefix = "k" };
        var wide = new TenantRuleView { RuleId = "r", ScopeKind = TenantRuleScopeKind.TenantWide };

        Assert.Multiple(() =>
        {
            Assert.That(TenantRuleHistory.PolicyKey(Acme, onTree), Is.EqualTo("t/acme/orders\u001ftenant:acme:r"));
            Assert.That(TenantRuleHistory.PolicyKey(Acme, wide), Is.EqualTo("t/acme/*\u001ftenant:acme:r"));
            Assert.That(TenantRuleHistory.PolicyKey("default", onTree), Is.Null);
            Assert.That(TenantRuleHistory.PolicyKey("not a tenant!", onTree), Is.Null);
            Assert.That(TenantRuleHistory.PolicyKey(Acme, onTree with { TreeName = null }), Is.Null);
            Assert.That(() => TenantRuleHistory.PolicyKey(Acme, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Without_a_state_api_or_with_a_refused_read_the_history_says_so()
    {
        SeedTenantRule("eng-orders", "orders", "eng");
        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/eng-orders");

        cut.WaitUntil(() =>
        {
            var history = cut.Find("[data-lt-rule-history]");
            Assert.That(history.GetAttribute("data-lt-rule-history"), Is.EqualTo("unavailable"));
            Assert.That(history.TextContent, Does.Contain(TenantRuleHistory.NotServedText));
        });
    }

    [Test]
    public void A_refused_history_read_names_who_may_read_it()
    {
        UseTrees("t/acme/orders").Fault = call => call.StartsWith(nameof(Orleans.Lattice.Explorer.Core.Connection.ILatticeStateClient.GetEntryHistoryAsync), StringComparison.Ordinal)
            ? new LatticeAuthorizationDeniedException("denied")
            : null;
        SeedTenantRule("eng-orders", "orders", "eng");

        var cut = RenderAt<AccessRulePage>("t/acme/access/rules/eng-orders");

        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-rule-history]").TextContent, Does.Contain(TenantRuleHistory.UnreadableText)));
    }

    private static EntryRevisionRecord Revision(string key, DateTimeOffset at, HistoryRowKind kind) => new()
    {
        SourceKey = key,
        Hlc = new HybridLogicalClock { WallClockTicks = at.UtcTicks },
        Kind = kind,
        ValuePreview = Encoding.UTF8.GetBytes("rule"),
        ValueLength = 4,
    };
}
