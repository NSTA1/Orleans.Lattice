using Bunit;
using Orleans.Lattice.Api.Apps;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The manifest-derived sections: trees (linking into Data, never showing a physical or
/// adopted id), roles, MCP tools, subscriptions, replication intent and consent with its
/// drift - each in its expanded table and its compact list form.
/// </summary>
public sealed partial class AppPageTests
{
    private static readonly string[] TableSections = ["trees", "roles", "tools", "subscriptions", "replication", "consent"];

    [Test]
    public void Trees_list_logical_names_shape_retention_and_adoption_and_link_into_data()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/trees");
        var rows = cut.FindAll("tbody tr");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("thead th").Select(header => header.TextContent.Trim()),
                Is.EqualTo(new[] { "Tree", "Adopted", "Shards", "Retention", "Leaf keys", "Internal children", "WAL partitions", "Rebuildable" }));
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("data/a/crm/orders"));
            Assert.That(Cells(rows[0]), Is.EqualTo(new[] { "orders", "Owned", "4", "30 days", "128", "host default", "host default", "No" }));
            Assert.That(Cells(rows[1]), Is.EqualTo(new[] { "legacy", "Adopted", "host default", "host default", "host default", "host default", "host default", "Yes" }));
        });
    }

    [Test]
    public void An_app_install_holder_sees_adopted_trees_by_name_and_never_by_adopted_id()
    {
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/trees");

        Assert.Multiple(() =>
        {
            Assert.That(Cells(cut.FindAll("tbody tr")[1])[..2], Is.EqualTo(new[] { "legacy", "Adopted" }));
            Assert.That(cut.Markup, Does.Not.Contain(AdoptedTreeId));
        });
    }

    [Test]
    public void An_app_with_no_trees_says_so()
    {
        Workspace.Grant(Workspace() with { Trees = [] });

        var cut = RenderAt("apps/crm/trees");

        Assert.That(cut.Find(".lt-table__empty").TextContent, Is.EqualTo("This app declares no trees."));
    }

    [Test]
    public void Roles_show_the_callers_roles_with_their_operations_and_scopes()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/roles");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("h2").Select(heading => heading.TextContent), Is.EqualTo(new[] { "Your roles" }));
            Assert.That(Cells(cut.Find("tbody tr")), Is.EqualTo(new[] { "viewer", "read, range read", "a/crm/orders, prefix eu/" }));
        });
    }

    [Test]
    public void An_app_install_holder_also_sees_every_declared_role_and_its_bound_group()
    {
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/roles");
        var declared = cut.FindAll("table")[1].QuerySelectorAll("tbody tr");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("h2").Select(heading => heading.TextContent), Is.EqualTo(new[] { "Your roles", "Declared roles" }));
            Assert.That(Cells(declared[0]), Is.EqualTo(new[] { "viewer", "grp-crm-viewers", "read, range read", "a/crm/orders, prefix eu/" }));
            Assert.That(Cells(declared[1]), Is.EqualTo(new[] { "editor", "grp-crm-editors", "read, write, delete", "a/crm/orders, whole tree; a/billing/invoices, whole tree" }));
        });
    }

    [Test]
    public void A_caller_without_a_role_is_told_so_on_the_roles_section()
    {
        Control.Administer(Admin() with { RoleBindings = [] }, CoveringConsent());

        var cut = RenderAt("apps/crm/roles");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-table__empty")[0].TextContent, Is.EqualTo("You hold no role in this app."));
            Assert.That(Cells(cut.FindAll("table").Single().QuerySelector("tbody tr")!)[1], Is.EqualTo("Not bound"), "an empty table shows its empty state alone, so the bindings are the one table");
        });
    }

    [Test]
    public void Tools_show_their_slug_qualified_names_and_required_roles()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/tools");

        Assert.That(Cells(cut.Find("tbody tr")), Is.EqualTo(new[] { "crm_search", "viewer", "Finds accounts by name." }));
    }

    [Test]
    public void Subscriptions_show_what_they_observe_including_other_apps()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/subscriptions");

        Assert.That(Cells(cut.Find("tbody tr")), Is.EqualTo(new[] { "invoices-feed", "a/billing/invoices", "2026/", "Another app" }));
    }

    [Test]
    public void Replication_shows_the_intent_and_links_to_the_replication_area_filtered_to_the_app()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/replication");

        Assert.Multiple(() =>
        {
            Assert.That(Cells(cut.Find("tbody tr")), Is.EqualTo(new[] { "orders", "LwwRegister" }));
            Assert.That(cut.Find(".lt-app-actions a").GetAttribute("href"), Is.EqualTo("replication?app=crm"));
        });
    }

    [Test]
    public void Empty_sections_say_what_the_app_does_not_declare()
    {
        Workspace.Grant(Workspace() with { McpTools = [], Subscriptions = [], Replication = [] });

        Assert.Multiple(() =>
        {
            Assert.That(RenderAt("apps/crm/tools").Find(".lt-table__empty").TextContent, Is.EqualTo("This app declares no MCP tools."));
            Assert.That(RenderAt("apps/crm/subscriptions").Find(".lt-table__empty").TextContent, Is.EqualTo("This app declares no change-feed subscriptions."));
            Assert.That(RenderAt("apps/crm/replication").Find(".lt-table__empty").TextContent, Is.EqualTo("This app declares no replication intent."));
        });
    }

    [Test]
    public void Consent_that_covers_the_manifest_shows_the_ceiling_scopes_and_bridge_operations()
    {
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/consent");
        var tables = cut.FindAll("table");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-pill").TextContent, Is.EqualTo("The consent covers the installed manifest"));
            Assert.That(cut.FindAll(".lt-app-findings"), Is.Empty);
            Assert.That(Definition(cut, "Consented version"), Is.EqualTo("2.1.0"));
            Assert.That(Definition(cut, "Allowed operations"), Is.EqualTo("read, write, delete, range read"));
            Assert.That(tables[0].QuerySelector("thead th")!.TextContent.Trim(), Is.EqualTo("Outside a/crm/"));
            Assert.That(tables[0].QuerySelectorAll("tbody tr").Select(Cells),
                Is.EqualTo(new[] { new[] { "a/billing/invoices", "whole tree" }, new[] { "legacy tree legacy-orders-2019", "prefix eu/" } }));
            Assert.That(Cells(tables[1].QuerySelector("tbody tr")!), Is.EqualTo(new[] { "data.read", "read its own trees", "every declared tree" }));
        });
    }

    [Test]
    public void Consent_drift_lists_every_difference_and_links_to_the_catalogues_re_consent()
    {
        Control.Administer(Admin(), DriftedConsent());

        var cut = RenderAt("apps/crm/consent");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-app-head .lt-pill").Select(pill => pill.TextContent), Does.Contain("Consent drift"));
            Assert.That(cut.FindAll(".lt-app-findings li").Select(item => item.TextContent), Is.EqualTo(new[]
            {
                "The consent covers version 2.0.0, but version 2.1.0 is installed.",
                "Its roles need write, delete, which the consented ceiling does not allow.",
                "The role editor reaches a/billing/invoices, outside the app's namespace, without an approved exception scope.",
                "The adopted tree legacy is not covered by an approved exception scope.",
                "Its UI asks to write to its own trees (data.write on tree orders), which was not consented.",
                "Its UI asks to keep the address line in step with its page (nav.sync), which was not consented.",
            }));
            Assert.That(cut.Find(".lt-app-actions a").TextContent, Is.EqualTo("Review and re-consent"));
            Assert.That(cut.Find(".lt-app-actions a").GetAttribute("href"), Is.EqualTo("apps/catalogue/in-image/crm"));
            Assert.That(cut.Find(".lt-table__empty").TextContent, Is.EqualTo("No exception scopes are approved: the app reaches only its own namespace."));
        });
    }

    [Test]
    public void No_recorded_consent_is_drift_and_never_consented_bridge_operations_say_so()
    {
        Control.Administer(Admin() with { SourceKey = null }, consent: null);

        var cut = RenderAt("apps/crm/consent");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-app-findings li").Select(item => item.TextContent), Is.EqualTo(new[] { "No consent is recorded for this install." }));
            Assert.That(cut.Find(".lt-app-actions a").GetAttribute("href"), Is.EqualTo("apps/catalogue?q=crm"));
        });

        Control.Consents[Slug] = CoveringConsent() with { BridgeGrants = null };
        var never = RenderAt("apps/crm/consent");
        Assert.That(never.FindAll(".lt-table__empty").Last().TextContent, Is.EqualTo("No bridge operations were ever consented."));
    }

    [Test]
    public void An_unreadable_consent_is_reported_rather_than_judged()
    {
        Control.Administer(Admin(), CoveringConsent());
        Control.ConsentThrows = new InvalidOperationException("consent down");

        var cut = RenderAt("apps/crm/consent");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-empty h3").TextContent, Is.EqualTo("The consent could not be read"));
            Assert.That(cut.FindAll(".lt-app-head .lt-pill").Select(pill => pill.TextContent), Does.Not.Contain("Consent drift"));
            Assert.That(cut.Markup, Does.Not.Contain("consent down"));
        });
    }

    [Test]
    public void A_failed_activation_is_shown_in_the_state_and_as_drift()
    {
        Control.Administer(Admin(state: AppLifecycleState.Failed), CoveringConsent());

        var cut = RenderAt("apps/crm/consent");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-app-head [data-lt-state=failed]").TextContent, Is.EqualTo("Activation failed"));
            Assert.That(cut.Find(".lt-app-findings li").TextContent, Does.StartWith("Activation failed"));
        });
    }

    [TestCaseSource(nameof(TableSections))]
    public void Below_the_small_breakpoint_every_section_table_is_a_list_of_rows(string section)
    {
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/" + section, compact: true);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty, "a table below 768px becomes a list");
            Assert.That(cut.FindAll(".lt-table-list"), Is.Not.Empty);
            Assert.That(cut.FindAll(".lt-table-list__open").All(row => row.TagName == "BUTTON"), Is.True);
        });
    }

    [Test]
    public void A_compact_tree_row_opens_a_sheet_that_links_into_data()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/trees", compact: true);
        var first = cut.Find(".lt-table-list__open");

        Assert.That(first.TextContent, Does.Contain("orders").And.Contain("Owned, 4 shards, retention 30 days"));

        first.Click();

        cut.WaitForAssertion(() =>
        {
            var sheet = cut.Find("[role=dialog]");
            Assert.That(sheet.QuerySelector("a.lt-btn")!.GetAttribute("href"), Is.EqualTo("data/a/crm/orders"));
            Assert.That(sheet.QuerySelector("a.lt-btn")!.TextContent, Is.EqualTo("Open in Data"));
        });
    }

    private static string[] Cells(AngleSharp.Dom.IElement row) =>
        [.. row.QuerySelectorAll("th, td").Select(cell => cell.TextContent.Trim())];
}
