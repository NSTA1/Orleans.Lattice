using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// <c>/schema/{tree-path}</c>: the heading with the declaring app, the tab row and
/// its addresses, not-found, a restricted identity, load failure and tenancy.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaTreePageTests : SchemaTestContext
{
    [Test]
    public void The_heading_names_the_tree_its_schema_state_and_the_declaring_app()
    {
        UseEstate();
        Schema.Policies["a/crm/orders"] = SchemaTestData.Policy();

        var cut = RenderAt<SchemaTreePage>("schema/a/crm/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("a/crm/orders"));
            Assert.That(cut.Find("h1").ClassList, Does.Contain("lt-shell-mono"));
            Assert.That(cut.FindAll(".lt-schema-heading .lt-pill__text").Select(pill => pill.TextContent), Is.EqualTo(new[] { "policy", "app: crm" }));
            var meta = cut.Find(".lt-schema-meta");
            Assert.That(meta.TextContent, Does.Contain("Declared by app crm 2.1.0"));
            Assert.That(meta.TextContent, Does.Contain("family orders-family, version 2, strict ingest"));
            Assert.That(meta.QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("apps/crm"));
            Assert.That(meta.TextContent, Does.Contain("Policy: 3 rules"));
            Assert.That(meta.TextContent, Does.Contain("Versioning: Unversioned"));
            Assert.That(cut.FindAll("[role=tab]").Select(tab => tab.TextContent), Is.EqualTo(new[] { "Policy", "Versions", "Compliance", "Remediation", "Dead letters" }));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Policy"));
        });
    }

    [TestCase("versions", "Versions")]
    [TestCase("compliance", "Compliance")]
    [TestCase("remediation", "Remediation")]
    [TestCase("dead-letters", "Dead letters")]
    public void The_tab_is_the_tab_query_value(string tab, string title)
    {
        UseEstate();

        var cut = RenderAt<SchemaTreePage>($"schema/orders?tab={tab}");

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo(title)));
    }

    [Test]
    public void Choosing_a_tab_moves_to_its_address()
    {
        UseEstate();
        var cut = RenderAt<SchemaTreePage>("schema/orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tab]"), Has.Count.EqualTo(5)));

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent == "Dead letters").Click();

        Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=dead-letters"));
    }

    [Test]
    public void A_tree_the_catalogue_does_not_list_is_not_found()
    {
        UseEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        var cut = RenderAt<SchemaTreePage>("schema/missing");

        cut.WaitUntil(() =>
        {
            Assert.That(notFound, Is.EqualTo(1));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo("There is no tree with this id."));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
        });
    }

    [Test]
    public void A_restricted_identity_sees_the_tree_but_no_tabs()
    {
        UseEstate();
        Schema.Capabilities["orders"] = FakeSchemaControl.None;

        var cut = RenderAt<SchemaTreePage>("schema/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("You may not manage this tree's schema"));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
        });
    }

    [Test]
    public void A_tree_that_does_not_load_can_be_tried_again()
    {
        UseEstate();
        Services.AddSingleton<Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession>(
            new Orleans.Lattice.Explorer.Tests.UI.Session.FakeExplorerSession(new Orleans.Lattice.Explorer.Tests.UI.Session.FakeStateConnection()));

        var cut = RenderAt<SchemaTreePage>("schema/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("This tree's schema did not load"));
            Assert.That(cut.Find(".lt-empty__body").TextContent, Is.EqualTo(SchemaTreeCatalog.NotConnected));
        });

        cut.Find(".lt-empty__actions button").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("This tree's schema did not load")));
    }

    [Test]
    public void Under_tenancy_the_tabs_and_the_app_link_stay_in_the_tenant()
    {
        UseEstate();
        UseTenancy("acme");
        var cut = RenderAt<SchemaTreePage>("t/acme/schema/a/crm/orders", tenancy: true);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tab]"), Has.Count.EqualTo(5)));

        Assert.That(cut.Find(".lt-schema-meta a").GetAttribute("href"), Is.EqualTo("t/acme/apps/crm"));

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent == "Versions").Click();

        Assert.That(Navigation.Uri, Does.EndWith("t/acme/schema/a/crm/orders?tab=versions"));
    }

    [Test]
    public void The_workspace_follows_the_page()
    {
        UseEstate();

        var cut = RenderAt<SchemaTreePage>("schema/orders?tab=versions");

        cut.WaitUntil(() =>
        {
            var workspace = cut.Instance.Workspace!;
            Assert.That(workspace.TreeId, Is.EqualTo("orders"));
            Assert.That(workspace.Tab, Is.EqualTo(SchemaTabs.Versions));
            Assert.That(workspace.ForTab(SchemaTabs.Compliance).Format(), Is.EqualTo("/schema/orders?tab=compliance"));
            Assert.That(workspace.Href(workspace.ForTab(SchemaTabs.DeadLetters)), Is.EqualTo("schema/orders?tab=dead-letters"));
            Assert.That(workspace.Grants.HasAny, Is.True);
            Assert.That(cut.Instance.TreeId, Is.EqualTo("orders"));
        });
    }
}
