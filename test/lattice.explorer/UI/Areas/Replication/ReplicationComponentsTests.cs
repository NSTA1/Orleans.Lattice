using Bunit;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The area's building blocks on their own: the order diagram, the link table, the
/// filter toolbar and the section links, including their parameter guards.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationComponentsTests : ReplicationTestContext
{
    [Test]
    public void The_diagram_names_an_unnamed_local_region_and_draws_a_peer_with_one_direction()
    {
        var cut = Render<ReplicationMap>(parameters => parameters
            .Add(map => map.LocalRegionId, string.Empty)
            .Add(map => map.Links, [Link("orders", "us-east", ReplicationLinkHealth.Lagging, entries: 3, bytes: 3000)]));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-replication-map__local .lt-replication-map__region").TextContent, Is.EqualTo("Unnamed region"));
            Assert.That(cut.FindAll(".lt-replication-map__edge").Select(edge => edge.GetAttribute("data-direction")), Is.EqualTo(new[] { "outbound", "inbound" }));
            Assert.That(cut.FindAll(".lt-replication-map__edge")[1].TextContent, Does.Contain("From us-east").And.Contain("No links"));
            Assert.That(cut.Find(".lt-replication-map__backlog").TextContent, Is.EqualTo("3 entries, 2.9 KB behind"));
            Assert.That(cut.FindAll(".lt-replication-map__alert"), Is.Empty, "a single link needs no count");
            Assert.That(cut.Find("figure").GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find("figcaption").Id));
            Assert.That(cut.Find(".lt-replication-map__peers").GetAttribute("aria-label"), Is.EqualTo("Peer regions"));
            Assert.That(cut.Find(".lt-replication-map__edges").GetAttribute("aria-label"), Is.EqualTo("Links with us-east"));
            Assert.That(cut.FindAll(".lt-replication-map__line").Select(line => line.GetAttribute("aria-hidden")).Distinct(), Is.EqualTo(new[] { "true" }));
        });
    }

    [Test]
    public void The_diagram_counts_lagging_links_on_an_edge_that_carries_several()
    {
        var cut = Render<ReplicationMap>(parameters => parameters
            .Add(map => map.LocalRegionId, "eu-west")
            .Add(map => map.Links,
            [
                Link("orders", "us-east", ReplicationLinkHealth.Lagging),
                Link("stock", "us-east", ReplicationLinkHealth.Lagging),
                Link("prices", "us-east"),
            ]));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-replication-map__alert").TextContent, Is.EqualTo("2 links lagging"));
            Assert.That(cut.Instance.Peers.Single().Outbound!.Links, Is.EqualTo(3));
        });
    }

    [Test]
    public void The_diagram_rejects_null_links()
    {
        Assert.That(() => Render<ReplicationMap>(parameters => parameters.Add(map => map.LocalRegionId, "x").Add(map => map.Links, null!)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_link_table_requires_a_caption_and_links()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => Render<ReplicationLinkTable>(parameters => parameters.Add(table => table.Links, []).Add(table => table.Caption, " ")),
                Throws.ArgumentException);
            Assert.That(() => Render<ReplicationLinkTable>(parameters => parameters.Add(table => table.Links, null!).Add(table => table.Caption, "Links")),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_link_table_shows_a_tree_that_cannot_be_addressed_as_text_and_its_caption()
    {
        var cut = Render<ReplicationLinkTable>(parameters => parameters
            .Add(table => table.Links, [Link("a//broken", "us-east")])
            .Add(table => table.Caption, "Every link"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("caption").TextContent, Is.EqualTo("Every link"));
            Assert.That(cut.Find("caption").ClassList, Does.Not.Contain("lt-visually-hidden"));
            Assert.That(cut.Find("tbody th").TextContent.Trim(), Is.EqualTo("a//broken"));
            Assert.That(cut.FindAll("tbody th a"), Is.Empty);
        });
    }

    [Test]
    public void The_toolbar_offers_the_given_regions_and_apps_and_labels_every_select()
    {
        var cut = Render<ReplicationToolbar>(parameters => parameters
            .AddCascadingValue(new ExplorerLocation(ExplorerAddress.Parse("/replication?app=crm"), [], true, false))
            .Add(toolbar => toolbar.Regions, ["ap-south", "us-east"])
            .Add(toolbar => toolbar.Apps, ["billing", "crm"]));

        var selects = cut.FindAll("select");
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("label").Select(label => label.TextContent), Is.EqualTo(new[] { "Health", "Peer region", "App" }));
            Assert.That(selects[0].QuerySelectorAll("option").Select(option => option.TextContent), Is.EqualTo(new[] { "Any health", "Stalled", "Lagging", "Unknown", "Healthy" }));
            Assert.That(selects[1].QuerySelectorAll("option").Select(option => option.GetAttribute("value")), Is.EqualTo(new[] { "", "ap-south", "us-east" }));
            Assert.That(selects[2].QuerySelector("option[selected]")!.GetAttribute("value"), Is.EqualTo("crm"));
            Assert.That(cut.Find(".lt-toolbar").GetAttribute("role"), Is.EqualTo("group"));
            Assert.That(cut.Instance.Filter.App, Is.EqualTo("crm"));
        });

        cut.FindAll("select")[2].Change(string.Empty);
        Assert.That(Navigation.Uri, Does.EndWith("/replication"), "choosing Any removes the key");
    }

    [Test]
    public void A_tree_page_shows_the_way_back_to_the_enrolled_trees_instead_of_the_section_row()
    {
        // #3987: one tree is below both sections, so no tab may read as this page.
        var cut = Render<ReplicationSections>(parameters => parameters
            .AddCascadingValue(new ExplorerLocation(ExplorerAddress.Parse("/replication/trees/orders?health=lagging"), [], true, false)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Instance.Section, Is.EqualTo(ReplicationSections.SectionKind.Tree));
            Assert.That(cut.FindAll(".lt-replication-sections__link"), Is.Empty, "no tab strip with a tab marked as if it were this page");
            Assert.That(cut.FindAll("[aria-current]"), Is.Empty);
            Assert.That(cut.Find(".lt-replication-back__link").TextContent, Is.EqualTo(ReplicationSections.BackText));
            Assert.That(cut.Find(".lt-replication-back__link").GetAttribute("href"), Is.EqualTo("replication/trees?health=lagging"), "the filters travel back");
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Replication sections"));
        });
    }

    [Test]
    public void The_enrolled_trees_page_marks_its_own_tab_only()
    {
        var cut = Render<ReplicationSections>(parameters => parameters
            .AddCascadingValue(new ExplorerLocation(ExplorerAddress.Parse("/replication/trees"), [], true, false)));

        Assert.That(cut.FindAll(".lt-replication-sections__link").Select(link => link.GetAttribute("aria-current")), Is.EqualTo(new[] { null, "page" }));
    }
}
