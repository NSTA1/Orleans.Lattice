using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// One rule's page: an authored rule's details, edit and confirmed delete; an
/// app-owned rule rendered read-only with its app's roles linked; and the
/// resolution of an id with, without and across trees.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessRulePageTests : AccessTestContext
{
    [Test]
    public void An_authored_rule_shows_its_fields_and_offers_edit_and_delete()
    {
        Admin.WithRule(Rule("orders-read", operations: LatticeOperation.Read | LatticeOperation.Write));

        var cut = RenderAt<AccessRulePage>("access/rules/orders-read?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("orders-read"));
            Assert.That(Definitions(cut), Is.EqualTo(new Dictionary<string, string>
            {
                ["Effect"] = "Allow",
                ["Subject"] = "group:ops",
                ["Scope"] = "orders",
                ["Operations"] = "Read, Write",
                ["Owner"] = "Authored",
            }));
            Assert.That(cut.Find(".lt-dl a").GetAttribute("href"), Is.EqualTo("access/groups/ops"));
            Assert.That(AccessForms.Button(cut, "Edit rule").HasAttribute("disabled"), Is.False);
            Assert.That(AccessForms.Button(cut, "Delete rule").ClassList, Does.Contain("lt-btn--destructive"));
            Assert.That(Admin.Calls, Does.Contain(nameof(FakeAuthAdmin.GetRuleAsync)));
        });
    }

    [Test]
    public void An_app_owned_rule_is_read_only_attributed_and_links_to_its_apps_roles()
    {
        var rule = AppRule("crm", "viewer");
        Admin.WithRule(rule);

        var cut = RenderAt<AccessRulePage>(AccessRoutes.Rule(rule.RuleId, rule.Scope.TreeId).ToHref());

        cut.WaitUntil(() =>
        {
            var note = cut.Find("[data-lt-app-owned]");
            Assert.That(note.TextContent, Does.Contain("compiled from the app's manifest"));
            Assert.That(note.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("apps/crm/roles"));
            Assert.That(Definitions(cut)["Owner"], Is.EqualTo("app crm, role viewer"));
            Assert.That(cut.FindAll("a.lt-btn").Single(link => link.TextContent == "Open crm roles").GetAttribute("href"), Is.EqualTo("apps/crm/roles"));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Edit rule").And.None.EqualTo("Delete rule"));
            Assert.That(cut.FindAll(".lt-confirm"), Is.Empty);
        });
    }

    [Test]
    public void Editing_opens_the_editor_and_shows_the_saved_rule()
    {
        Admin.WithRule(Rule("orders-read"));
        var cut = RenderAt<AccessRulePage>("access/rules/orders-read?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("h1"), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Edit rule").Click();
        cut.Find("[data-lt-operation=\"write\"]").Change(true);
        cut.Find("form.lt-access-form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Definitions(cut)["Operations"], Is.EqualTo("Read, Write"));
            Assert.That(Admin.Rules.Single().Operations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
            Assert.That(cut.FindAll("form.lt-access-form"), Is.Empty);
        });
    }

    [Test]
    public void Deleting_asks_for_the_rule_id_then_removes_it_and_returns_to_the_list()
    {
        Admin.WithRule(Rule("orders-read"));
        var cut = RenderAt<AccessRulePage>("access/rules/orders-read?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("h1"), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Delete rule").Click();
        var confirm = cut.Find("form.lt-confirm");
        Assert.That(confirm.QuerySelector(".lt-confirm__name")!.TextContent, Is.EqualTo("orders-read"));
        Assert.That(confirm.QuerySelector("button[type=submit]")!.HasAttribute("disabled"), Is.True, "nothing is removed until the id is typed");

        AccessForms.Type(cut, "Rule name", "orders-read");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Rules, Is.Empty);
            Assert.That(Navigation.Uri, Does.EndWith("/access/rules"));
        });
    }

    [Test]
    public void Without_a_tree_the_id_is_found_across_the_store()
    {
        Admin.WithRule(Rule("other")).WithRule(Rule("orders-read"));

        var cut = RenderAt<AccessRulePage>("access/rules/orders-read");

        cut.WaitUntil(() => Assert.That(Definitions(cut)["Scope"], Is.EqualTo("orders")));
    }

    [Test]
    public void An_id_used_in_several_trees_offers_a_choice()
    {
        Admin.WithRule(Rule("readers", tree: "orders")).WithRule(Rule("readers", tree: "invoices"));

        var cut = RenderAt<AccessRulePage>("access/rules/readers");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("more than one tree"));
            Assert.That(cut.FindAll("tbody th a").Select(link => link.GetAttribute("href")),
                Is.EquivalentTo(new[] { "access/rules/readers?tree=orders", "access/rules/readers?tree=invoices" }));
        });
    }

    [Test]
    public void An_unknown_rule_is_not_found()
    {
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt<AccessRulePage>("access/rules/missing?tree=orders");

        cut.WaitUntil(() => Assert.That(notFound, Is.EqualTo(1)));
    }

    [Test]
    public void A_failed_delete_is_reported_and_the_rule_stays()
    {
        Admin.WithRule(Rule("orders-read"));
        Admin.Fail(nameof(FakeAuthAdmin.RemoveRuleAsync), new LatticeAuthorizationDeniedException("_lattice_policy", LatticeOperation.Admin, "ops", "revoked"));
        var toasts = Services.GetRequiredService<Orleans.Lattice.Explorer.Shell.Design.Components.LtToastService>();
        var cut = RenderAt<AccessRulePage>("access/rules/orders-read?tree=orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("h1"), Has.Count.EqualTo(1)));

        AccessForms.Button(cut, "Delete rule").Click();
        AccessForms.Type(cut, "Rule name", "orders-read");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Admin.Rules, Has.Count.EqualTo(1));
            Assert.That(toasts.Toasts.Select(toast => toast.Message), Does.Contain(AccessFailure.NotPermittedMessage));
        });
    }

    [Test]
    public void The_explain_link_carries_the_subject_and_tree()
    {
        Admin.WithRule(Rule("orders-read"));

        var cut = RenderAt<AccessRulePage>("access/rules/orders-read?tree=orders");

        cut.WaitUntil(() => Assert.That(
            cut.FindAll("a").Single(link => link.TextContent == "Explain for this subject").GetAttribute("href"),
            Is.EqualTo("access/explain?subject=ops&kind=group&tree=orders")));
    }

    private static Dictionary<string, string> Definitions(IRenderedComponent<AccessRulePage> cut) =>
        cut.FindAll(".lt-dl__row").ToDictionary(
            row => row.QuerySelector(".lt-dl__term")!.TextContent.Trim(),
            row => string.Join(' ', row.QuerySelector(".lt-dl__value")!.TextContent.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)));
}
