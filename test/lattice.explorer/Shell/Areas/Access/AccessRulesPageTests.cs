using Bunit;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// The rule list: loading, the booktabs table and its compact rows, app-owned
/// attribution, filtering, paging, the create lifecycle, and the denied and
/// unreachable states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessRulesPageTests : AccessTestContext
{
    [Test]
    public void While_the_rules_load_the_page_shows_a_skeleton_then_the_table()
    {
        Admin.WithRule(Rule("orders-read"));
        var hold = Admin.Hold(nameof(FakeAuthAdmin.ListRulesAsync));

        var cut = RenderAt<AccessRulesPage>("access/rules");

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));

        cut.InvokeAsync(hold.SetResult);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-skeleton"), Is.Empty);
            Assert.That(cut.Find("table.lt-table tbody th a").TextContent, Is.EqualTo("orders-read"));
        });
    }

    [Test]
    public void Rules_list_every_field_and_link_to_their_page_by_tree()
    {
        Admin.WithRule(Rule("orders-read", operations: LatticeOperation.Read | LatticeOperation.RangeRead));

        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() =>
        {
            var cells = cut.FindAll("tbody tr")[0].Children.Select(cell => cell.TextContent.Trim()).ToArray();
            Assert.That(cells, Is.EqualTo(new[] { "orders-read", "Allow", "group:ops", "orders", "Read, Range read", "Authored" }));
            Assert.That(cut.Find("tbody th a").GetAttribute("href"), Is.EqualTo("access/rules/orders-read?tree=orders"));
        });
    }

    [Test]
    public void An_app_owned_rule_names_its_app_and_links_to_its_roles()
    {
        Admin.WithRule(AppRule("crm"));

        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() =>
        {
            var owner = cut.Find("tbody td:last-child a");
            Assert.That(owner.TextContent, Is.EqualTo("app crm"));
            Assert.That(owner.GetAttribute("href"), Is.EqualTo("apps/crm/roles"));
        });
    }

    [Test]
    public void With_tenancy_on_the_rule_links_stay_cluster_wide_and_the_app_link_is_tenant_rooted()
    {
        UseTenancy("acme");
        Admin.WithRule(AppRule("crm"));

        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("tbody th a").GetAttribute("href"), Does.StartWith("access/rules/"));
            Assert.That(cut.Find("tbody td:last-child a").GetAttribute("href"), Is.EqualTo("t/acme/apps/crm/roles"));
            Assert.That(cut.Find(".lt-access-nav__link").GetAttribute("href"), Is.EqualTo("access/rules"));
        });
    }

    [Test]
    public void The_ownership_filter_and_the_search_narrow_the_loaded_rules()
    {
        Admin.WithRule(Rule("orders-read")).WithRule(Rule("invoices-write", tree: "invoices")).WithRule(AppRule("crm"));
        var cut = RenderAt<AccessRulesPage>("access/rules");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3)));

        cut.FindAll(".lt-access-filter button").Single(button => button.TextContent == "App-owned").Click();
        Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { AppRule("crm").RuleId }));
        Assert.That(cut.FindAll(".lt-access-filter button").Single(button => button.TextContent == "App-owned").GetAttribute("aria-pressed"), Is.EqualTo("true"));

        cut.FindAll(".lt-access-filter button").Single(button => button.TextContent == "Authored").Click();
        cut.Find("input[type=search]").Input("invoice");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "invoices-write" }));
            Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("1 of 3 rules"));
        });
    }

    [Test]
    public void Load_more_appends_the_next_page()
    {
        Admin.ForcedPageSize = 2;
        Admin.WithRule(Rule("a")).WithRule(Rule("b")).WithRule(Rule("c"));
        var cut = RenderAt<AccessRulesPage>("access/rules");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-access-count").TextContent, Is.EqualTo("2 rules, more to load")));

        cut.FindAll("button").Single(button => button.TextContent == "Load more rules").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody th").Select(cell => cell.TextContent), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(cut.FindAll("button").Any(button => button.TextContent == "Load more rules"), Is.False);
        });
    }

    [Test]
    public void An_empty_store_says_so()
    {
        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No rules are defined on this cluster yet.")));
    }

    [Test]
    public void A_restricted_identity_is_told_it_is_not_permitted_and_no_create_is_offered()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListRulesAsync), new LatticeAuthorizationDeniedException("_lattice_policy", LatticeOperation.Admin, "ops@example.com", "not an administrator"));

        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not permitted"));
            Assert.That(cut.Find(".lt-empty__body").TextContent, Is.EqualTo(AccessFailure.NotPermittedMessage));
            Assert.That(cut.Find("[data-lt-command=\"access.create-rule\"]").HasAttribute("disabled"), Is.True);
            Assert.That(cut.FindAll("table"), Is.Empty);
        });
    }

    [Test]
    public void An_unreachable_cluster_offers_a_retry_that_recovers()
    {
        Admin.WithRule(Rule("orders-read"));
        Admin.Fail(nameof(FakeAuthAdmin.ListRulesAsync), new InvalidOperationException("not configured"));
        var cut = RenderAt<AccessRulesPage>("access/rules");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Rules could not be read")));

        Admin.Heal(nameof(FakeAuthAdmin.ListRulesAsync));
        cut.FindAll("button").Single(button => button.TextContent == "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("orders-read")));
    }

    [Test]
    public void The_posture_banner_renders_the_access_model()
    {
        var cut = RenderAt<AccessRulesPage>("access/rules");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-access-banner").TextContent, Does.Contain("Claims (token)")));
    }

    [Test]
    public void Below_the_small_breakpoint_rules_are_two_line_rows_with_a_detail_sheet_and_the_filter_is_a_select()
    {
        Admin.WithRule(Rule("orders-read"));

        var cut = RenderAt<AccessRulesPage>("access/rules", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            Assert.That(cut.Find(".lt-compact-row__primary").TextContent, Is.EqualTo("orders-read"));
            Assert.That(cut.Find(".lt-compact-row__secondary").TextContent.Trim(), Is.EqualTo("Allow group:ops - orders - Read"));
            Assert.That(cut.FindAll(".lt-access-filter"), Is.Empty);
            Assert.That(cut.FindAll("select").Any(select => select.QuerySelectorAll("option").Any(option => option.TextContent == "App-owned")), Is.True);
        });

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end"));
            Assert.That(cut.Find(".lt-dialog .lt-dialog__actions a").GetAttribute("href"), Is.EqualTo("access/rules/orders-read?tree=orders"));
        });
    }

    [Test]
    public void The_create_rule_command_has_a_visible_control_and_its_target_opens_the_editor()
    {
        var command = new AccessArea(Services).Commands.Single(candidate => candidate.Id == AccessArea.CreateRuleCommandId);

        var cut = RenderAt<AccessRulesPage>(command.Target!.ToHref());

        cut.WaitUntil(() =>
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
            Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("New rule"));
            Assert.That(cut.FindAll("form.lt-access-form"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Creating_a_rule_puts_it_through_the_facade_lists_it_and_confirms()
    {
        var cut = RenderAt<AccessRulesPage>("access/rules");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table__empty"), Has.Count.EqualTo(1)));

        cut.Find("[data-lt-command=\"access.create-rule\"]").Click();
        AccessRuleEditorTests.Fill(cut, "orders-read", "ops", "orders");
        cut.Find("[data-lt-operation=\"read\"]").Change(true);
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Rules.Select(rule => rule.RuleId), Is.EqualTo(new[] { "orders-read" }));
            Assert.That(cut.Find("tbody th").TextContent, Is.EqualTo("orders-read"));
            Assert.That(cut.FindAll(".lt-dialog"), Is.Empty);
        });
    }
}
