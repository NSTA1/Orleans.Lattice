using Bunit;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The address line below the small breakpoint: the last two nodes behind a
/// "..." node that opens the full chain as a spine, a shorter prompt with no key
/// hints, and the command palette as a full-screen sheet.
/// </summary>
public sealed partial class AddressLineTests
{
    [Test]
    public void Compact_shows_the_last_two_nodes_behind_a_more_node()
    {
        var cut = RenderLine(Location("/data/a/crm/orders"), compact: true);

        var more = cut.Find(".lt-shell-address-line__more");
        Assert.Multiple(() =>
        {
            Assert.That(more.TextContent, Is.EqualTo("..."));
            Assert.That(more.GetAttribute("aria-label"), Is.EqualTo("Show the full address"));
            Assert.That(more.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(cut.FindAll(".lt-chain__link .lt-chain__text").Select(node => node.TextContent), Is.EqualTo(new[] { "...", "crm", "orders" }));
            Assert.That(cut.Find("[aria-current='page']").TextContent, Is.EqualTo("orders"));
            Assert.That(cut.FindAll("kbd"), Is.Empty, "a touch screen has no keyboard to hint at");
            Assert.That(cut.Find(".lt-shell-address-line__hint").TextContent, Is.EqualTo("Go to or search"));
        });
    }

    [Test]
    public void Compact_with_a_short_address_shows_the_whole_chain()
    {
        var cut = RenderLine(Location("/data"), compact: true);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-address-line__more"), Is.Empty);
            Assert.That(cut.FindAll(".lt-chain__text").Select(node => node.TextContent), Is.EqualTo(new[] { "Home", "data" }));
        });
    }

    [Test]
    public void The_more_node_opens_the_full_chain_as_a_spine_and_following_it_closes()
    {
        var cut = RenderLine(Location("/data/a/crm/orders"), compact: true);

        cut.Find(".lt-shell-address-line__more").Click();

        var full = cut.Find(".lt-shell-address-line__full");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-shell-address-line__more").GetAttribute("aria-expanded"), Is.EqualTo("true"));
            Assert.That(cut.Find(".lt-shell-address-line__more").GetAttribute("aria-controls"), Is.EqualTo(full.Id));
            Assert.That(full.QuerySelectorAll(".lt-spine__text").Select(node => node.TextContent), Is.EqualTo(new[] { "Home", "data", "a", "crm", "orders" }));
            Assert.That(full.QuerySelector("[aria-current='page']")!.TextContent.Trim(), Is.EqualTo("orders"));
            Assert.That(full.QuerySelectorAll("a")[1].GetAttribute("href"), Is.EqualTo("data"));
        });

        full.QuerySelectorAll("a")[1].Click();

        Assert.That(cut.FindAll(".lt-shell-address-line__full"), Is.Empty);
    }

    [Test]
    public void The_palette_is_a_full_screen_sheet_when_compact()
    {
        var cut = RenderLine(Location("/"), compact: true);
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input(">");

        Assert.That(cut.Find("[role='search']").ClassList, Does.Contain("lt-shell-address-line--sheet"));

        cut.Find("input").Input("/data");

        Assert.That(cut.Find("[role='search']").ClassList, Does.Not.Contain("lt-shell-address-line--sheet"),
            "an address or a search stays a full-width line with its popover");
    }

    [Test]
    public void The_palette_is_not_a_sheet_when_expanded()
    {
        var cut = RenderLine(Location("/"));
        cut.Find(".lt-shell-address-line__edit").Click();

        cut.Find("input").Input(">");

        Assert.That(cut.Find("[role='search']").ClassList, Does.Not.Contain("lt-shell-address-line--sheet"));
    }
}
