using Bunit;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Design.Components;

/// <summary>
/// The notation's primitives: a node, a spine of stops, a chain of links. The
/// current position is the one ringed marker node, carried by markup and weight
/// as well - never by the marker alone - and there is exactly one of it.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class LtNotationTests : ShellDesignTestContext
{
    [Test]
    [TestCase(LtNodeKind.Filled, LtNodeSize.Small, "lt-node")]
    [TestCase(LtNodeKind.Hollow, LtNodeSize.Small, "lt-node lt-node--hollow")]
    [TestCase(LtNodeKind.Concurrent, LtNodeSize.Small, "lt-node lt-node--concurrent")]
    [TestCase(LtNodeKind.Join, LtNodeSize.Small, "lt-node lt-node--join")]
    [TestCase(LtNodeKind.Filled, LtNodeSize.Large, "lt-node lt-node--large")]
    [TestCase(LtNodeKind.Hollow, LtNodeSize.Large, "lt-node lt-node--hollow lt-node--large")]
    [TestCase(LtNodeKind.Concurrent, LtNodeSize.Large, "lt-node lt-node--concurrent lt-node--large")]
    [TestCase(LtNodeKind.Join, LtNodeSize.Large, "lt-node lt-node--join lt-node--large")]
    public void Each_node_kind_and_size_has_its_class(LtNodeKind kind, LtNodeSize size, string expected)
    {
        var node = Render<LtNode>(p => p.Add(x => x.Kind, kind).Add(x => x.Size, size)).Find("span");

        Assert.That(node.ClassName, Is.EqualTo(expected));
    }

    [Test]
    public void A_node_without_a_label_is_hidden_from_assistive_technology()
    {
        var node = Render<LtNode>().Find("span");

        Assert.Multiple(() =>
        {
            Assert.That(node.GetAttribute("aria-hidden"), Is.EqualTo("true"));
            Assert.That(node.HasAttribute("role"), Is.False);
        });
    }

    [Test]
    public void A_labelled_node_is_an_image_with_that_name()
    {
        var node = Render<LtNode>(p => p.Add(x => x.Kind, LtNodeKind.Join).Add(x => x.Label, "Merged state")).Find("span");

        Assert.Multiple(() =>
        {
            Assert.That(node.GetAttribute("role"), Is.EqualTo("img"));
            Assert.That(node.GetAttribute("aria-label"), Is.EqualTo("Merged state"));
            Assert.That(node.HasAttribute("aria-hidden"), Is.False);
        });
    }

    [Test]
    public void A_labelled_spine_is_a_named_navigation_landmark_of_stops()
    {
        var cut = Render<LtSpine>(p => AddStops(p.Add(x => x.Label, "Areas"), current: "data"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Areas"));
            Assert.That(cut.FindAll("nav > ul.lt-spine > li.lt-spine__stop"), Has.Count.EqualTo(3));
            Assert.That(cut.FindAll("a").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "/data", "/apps", "/access" }));
        });
    }

    [Test]
    public void An_unlabelled_spine_is_a_plain_list()
    {
        var cut = Render<LtSpine>(p => AddStops(p, current: null));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("nav"), Is.Empty);
            Assert.That(cut.FindAll("ul.lt-spine > li"), Has.Count.EqualTo(3));
        });
    }

    [Test]
    public void The_current_stop_is_the_one_marker_node_and_is_marked_in_markup()
    {
        var cut = Render<LtSpine>(p => AddStops(p.Add(x => x.Label, "Areas"), current: "apps"));

        var current = cut.FindAll("a[aria-current]");
        Assert.Multiple(() =>
        {
            Assert.That(current, Has.Count.EqualTo(1));
            Assert.That(current[0].GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(current[0].TextContent.Trim(), Is.EqualTo("Apps"));
            Assert.That(cut.FindAll(".lt-node--join"), Has.Count.EqualTo(1), "one marker per spine");
            Assert.That(current[0].QuerySelector(".lt-node--join"), Is.Not.Null, "the marker sits on the current stop");
            Assert.That(cut.FindAll(".lt-node--hollow"), Has.Count.EqualTo(2), "every other stop is a hollow node");
        });
    }

    [Test]
    public void A_chain_is_a_named_ordered_list_whose_last_link_is_the_current_position()
    {
        var cut = Render<LtChain>(p => p
            .Add(x => x.Label, "Address")
            .Add(x => x.Links, new[]
            {
                new LtChainLink("data", "/data"),
                new LtChainLink("a"),
                new LtChainLink("crm", "/data/a/crm"),
                new LtChainLink("orders", "/data/a/crm/orders"),
            }));

        var items = cut.FindAll("nav > ol > li");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Address"));
            Assert.That(items, Has.Count.EqualTo(4));
            Assert.That(items[0].QuerySelector("a")?.GetAttribute("href"), Is.EqualTo("/data"));
            Assert.That(items[1].QuerySelector("a"), Is.Null, "a segment with no destination is a label");
            Assert.That(items[3].QuerySelector("a"), Is.Null, "the current position is never a link to itself");
            Assert.That(items[3].QuerySelector("[aria-current]")?.GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(items[3].QuerySelector("[aria-current]")?.TextContent, Is.EqualTo("orders"));
            Assert.That(cut.FindAll("[aria-current]"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-node--join"), Has.Count.EqualTo(1));
            Assert.That(items[3].QuerySelector(".lt-node--join"), Is.Not.Null);
        });
    }

    [Test]
    public void A_mono_chain_is_an_address_set_in_cascadia()
    {
        var cut = Render<LtChain>(p => p.Add(x => x.Label, "Address").Add(x => x.Mono, true).Add(x => x.Links, new[] { new LtChainLink("data") }));

        Assert.That(cut.Find("ol").ClassName, Is.EqualTo("lt-chain lt-chain--mono"));
    }

    [Test]
    public void An_empty_chain_renders_an_empty_list()
    {
        var cut = Render<LtChain>(p => p.Add(x => x.Label, "Address"));

        Assert.That(cut.FindAll("li"), Is.Empty);
    }

    private static void AddStops(ComponentParameterCollectionBuilder<LtSpine> parameters, string? current)
    {
        foreach (var (slug, text) in new[] { ("data", "Data"), ("apps", "Apps"), ("access", "Access") })
        {
            parameters.AddChildContent<LtSpineStop>(stop => stop
                .Add(x => x.Href, "/" + slug)
                .Add(x => x.Text, text)
                .Add(x => x.Current, slug == current));
        }
    }
}